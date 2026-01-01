package derecho

import (
	"context"
	"fmt"

	"github.com/atmonostorm/derecho/journal"
)

// ExecutionState feeds events to the scheduler and manages visibility
// for deterministic replay of a single workflow execution.
type ExecutionState interface {
	NewRecorder(afterEventID int) Recorder
	GetByScheduledID(scheduledEventID int) []journal.Event
	GetSignals(signalName string) []journal.SignalReceived
	GetCancelRequest() *journal.WorkflowCancelRequested
}

// ScheduledIDReceiver is implemented by futures that need their scheduled ID
// updated after Commit assigns real event IDs.
type ScheduledIDReceiver interface {
	SetScheduledID(id int)
}

type Recorder interface {
	// Record returns the event and its index in pending. During replay, index
	// is -1 since the event already has the correct ID. During first execution,
	// index identifies this event for RegisterPendingFuture.
	Record(eventType string, generate func() journal.Event) (event journal.Event, pendingIndex int, err error)
	// RegisterPendingFuture associates a future with a pending event so Commit
	// can update the future's scheduled ID with the real ID from the store.
	RegisterPendingFuture(pendingIndex int, f ScheduledIDReceiver)
	Commit(ctx context.Context, scheduledAtTask int) error
	PendingCount() int
}

type executionState struct {
	workflowID string
	runID      string

	store journal.Store

	allEvents          []journal.Event
	schedulerEvents    []journal.Event
	replayIdx          int
	visibilityBoundary int
	newEvents          []journal.Event
	lastEventID        int
}

func (s *executionState) NewRecorder(workflowTaskScheduledID int) Recorder {
	// Visibility must match what existed when this task started, otherwise
	// replay sees completions that arrived after and diverges from original execution.
	for _, ev := range s.allEvents {
		if started, ok := ev.(journal.WorkflowTaskStarted); ok {
			if started.Base().ScheduledByID == workflowTaskScheduledID {
				s.visibilityBoundary = started.Base().ID
				if started.Base().ID > s.lastEventID {
					s.lastEventID = started.Base().ID
				}
				break
			}
		}
	}
	return &recorder{state: s}
}

type recorder struct {
	state          *executionState
	pending        []journal.Event
	pendingFutures []ScheduledIDReceiver
}

// Record checks event type at this position during replay, returning NondeterminismError
// on mismatch. We compare type and position only, not payload - inputs may legitimately
// change between versions as long as the event sequence structure is preserved.
func (r *recorder) Record(eventType string, generate func() journal.Event) (journal.Event, int, error) {
	s := r.state

	if s.replayIdx < len(s.schedulerEvents) {
		recorded := s.schedulerEvents[s.replayIdx]
		if recorded.EventType() != eventType {
			return nil, -1, &NondeterminismError{
				WorkflowID: s.workflowID,
				RunID:      s.runID,
				EventSeq:   recorded.Base().ID,
				Expected:   recorded.EventType(),
				Got:        eventType,
			}
		}
		s.replayIdx++
		return recorded, -1, nil
	}

	event := generate()
	provisionalID := s.lastEventID + len(r.pending) + 1
	event = event.WithID(provisionalID)
	pendingIndex := len(r.pending)
	r.pending = append(r.pending, event)
	s.newEvents = append(s.newEvents, event)
	return event, pendingIndex, nil
}

func (r *recorder) Commit(ctx context.Context, scheduledAtTask int) error {
	if len(r.pending) == 0 {
		return nil
	}

	s := r.state

	// nil store means replay mode - just update bookkeeping, don't persist
	if s.store == nil {
		s.lastEventID = r.pending[len(r.pending)-1].Base().ID
		s.visibilityBoundary = s.lastEventID
		r.pending = nil
		r.pendingFutures = nil
		return nil
	}

	ids, err := s.store.Append(ctx, s.workflowID, s.runID, r.pending, scheduledAtTask)
	if err != nil {
		return err
	}

	// provisional IDs can differ from real IDs when side-effect events
	// (like ChildWorkflowStarted) are inserted between our events during Append
	startIdx := len(s.newEvents) - len(r.pending)
	for i, id := range ids {
		s.newEvents[startIdx+i] = s.newEvents[startIdx+i].WithID(id)
		if i < len(r.pendingFutures) && r.pendingFutures[i] != nil {
			r.pendingFutures[i].SetScheduledID(id)
		}
	}

	s.lastEventID = ids[len(ids)-1]
	s.visibilityBoundary = s.lastEventID
	r.pending = nil
	r.pendingFutures = nil
	return nil
}

func (r *recorder) PendingCount() int {
	return len(r.pending)
}

func (r *recorder) RegisterPendingFuture(pendingIndex int, f ScheduledIDReceiver) {
	if pendingIndex < 0 {
		return
	}
	for len(r.pendingFutures) <= pendingIndex {
		r.pendingFutures = append(r.pendingFutures, nil)
	}
	r.pendingFutures[pendingIndex] = f
}

func isSchedulerEvent(ev journal.Event) bool {
	switch ev.EventType() {
	case journal.TypeActivityScheduled,
		journal.TypeTimerScheduled,
		journal.TypeTimerCancelled,
		journal.TypeChildWorkflowScheduled,
		journal.TypeWorkflowCompleted,
		journal.TypeWorkflowFailed,
		journal.TypeWorkflowCancelled,
		journal.TypeWorkflowContinuedAsNew,
		journal.TypeSideEffectRecorded,
		journal.TypeSignalExternalScheduled:
		return true
	default:
		return false
	}
}

func newExecutionState(workflowID, runID string, events []journal.Event, store journal.Store) *executionState {
	lastEventID := 0
	if len(events) > 0 {
		lastEventID = events[len(events)-1].Base().ID
	}

	var schedulerEvents []journal.Event
	for _, ev := range events {
		if isSchedulerEvent(ev) {
			schedulerEvents = append(schedulerEvents, ev)
		}
	}

	visibilityBoundary := lastEventID

	return &executionState{
		workflowID:         workflowID,
		runID:              runID,
		store:              store,
		allEvents:          events,
		schedulerEvents:    schedulerEvents,
		visibilityBoundary: visibilityBoundary,
		lastEventID:        lastEventID,
	}
}

func (s *executionState) GetByScheduledID(scheduledEventID int) []journal.Event {
	var result []journal.Event
	for _, ev := range s.allEvents {
		base := ev.Base()
		if base.ScheduledByID == scheduledEventID && base.ID <= s.visibilityBoundary {
			result = append(result, ev)
		}
	}
	for _, ev := range s.newEvents {
		if ev.Base().ScheduledByID == scheduledEventID {
			result = append(result, ev)
		}
	}
	return result
}

func (s *executionState) GetSignals(signalName string) []journal.SignalReceived {
	var result []journal.SignalReceived
	for _, ev := range s.allEvents {
		if ev.Base().ID > s.visibilityBoundary {
			continue
		}
		if sig, ok := ev.(journal.SignalReceived); ok && sig.SignalName == signalName {
			result = append(result, sig)
		}
	}
	return result
}

func (s *executionState) GetCancelRequest() *journal.WorkflowCancelRequested {
	for _, ev := range s.allEvents {
		if ev.Base().ID > s.visibilityBoundary {
			continue
		}
		if cancel, ok := ev.(journal.WorkflowCancelRequested); ok {
			return &cancel
		}
	}
	return nil
}

func (s *executionState) AddEvents(events []journal.Event) {
	s.allEvents = append(s.allEvents, events...)
	for _, ev := range events {
		if isSchedulerEvent(ev) {
			s.schedulerEvents = append(s.schedulerEvents, ev)
		}
	}
	if len(events) > 0 {
		lastID := events[len(events)-1].Base().ID
		s.lastEventID = lastID
		s.visibilityBoundary = lastID
	}
}

func (s *executionState) LastEventID() int {
	return s.lastEventID
}

func (s *executionState) PendingEvents() []journal.Event {
	return s.newEvents
}

func (s *executionState) NewEvents() []journal.Event {
	return s.newEvents
}

// HasPendingWork returns true if workflow has in-flight activities or timers.
// ChildWorkflowScheduled intentionally excluded: parents waiting for children
// aren't doing CPU work and can be evicted safely. They'll replay when children complete.
func (s *executionState) HasPendingWork() bool {
	for _, ev := range s.allEvents {
		switch ev.EventType() {
		case journal.TypeActivityScheduled,
			journal.TypeTimerScheduled:
			if !s.hasCompletion(ev.Base().ID) {
				return true
			}
		}
	}
	return false
}

func (s *executionState) hasCompletion(scheduledID int) bool {
	for _, ev := range s.allEvents {
		base := ev.Base()
		if base.ScheduledByID != scheduledID {
			continue
		}
		switch ev.EventType() {
		case journal.TypeActivityCompleted, journal.TypeActivityFailed,
			journal.TypeTimerFired, journal.TypeTimerCancelled,
			journal.TypeChildWorkflowCompleted, journal.TypeChildWorkflowFailed:
			return true
		}
	}
	return false
}

type NondeterminismError struct {
	WorkflowID string
	RunID      string
	EventSeq   int
	Expected   string
	Got        string
}

func (e *NondeterminismError) Error() string {
	return fmt.Sprintf("derecho: nondeterminism detected in workflow %s/%s at event %d: expected %s, got %s",
		e.WorkflowID, e.RunID, e.EventSeq, e.Expected, e.Got)
}
