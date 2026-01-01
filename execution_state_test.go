package derecho

import (
	"context"
	"testing"
	"time"

	"github.com/atmonostorm/derecho/journal"
)

// minimalStore is a minimal in-memory Store for unit testing execution state logic.
// It intentionally lacks task queue management and thread-safety since those aren't
// needed for testing replay/event storage. For integration tests that need full
// worker behavior, use MemoryStore instead.
type minimalStore struct {
	events      map[string][]journal.Event
	lastEventID map[string]int
}

func newMinimalStore() *minimalStore {
	return &minimalStore{
		events:      make(map[string][]journal.Event),
		lastEventID: make(map[string]int),
	}
}

func (s *minimalStore) key(workflowID, runID string) string {
	return journal.WorkflowKey(workflowID, runID)
}

func (s *minimalStore) Load(ctx context.Context, workflowID, runID string) ([]journal.Event, error) {
	return s.events[s.key(workflowID, runID)], nil
}

func (s *minimalStore) LoadFrom(ctx context.Context, workflowID, runID string, afterEventID int) ([]journal.Event, error) {
	all := s.events[s.key(workflowID, runID)]
	var result []journal.Event
	for _, ev := range all {
		if ev.Base().ID > afterEventID {
			result = append(result, ev)
		}
	}
	return result, nil
}

func (s *minimalStore) Append(ctx context.Context, workflowID, runID string, events []journal.Event, scheduledByEventID int) ([]int, error) {
	key := s.key(workflowID, runID)
	eventID := s.lastEventID[key]
	ids := make([]int, len(events))
	for i := range events {
		eventID++
		ids[i] = eventID
		s.events[key] = append(s.events[key], events[i])
	}
	s.lastEventID[key] = eventID
	return ids, nil
}

func (s *minimalStore) WaitForWorkflowTasks(ctx context.Context, workerID string, maxNew int) ([]journal.PendingWorkflowTask, error) {
	return nil, nil
}

func (s *minimalStore) WaitForActivityTasks(ctx context.Context, workerID string, maxActivities int) ([]journal.PendingActivityTask, error) {
	return nil, nil
}

func (s *minimalStore) GetTimersToFire(ctx context.Context, now time.Time) ([]journal.PendingTimerTask, error) {
	return nil, nil
}

func (s *minimalStore) GetTimedOutActivities(ctx context.Context, now time.Time) ([]journal.TimedOutActivity, error) {
	return nil, nil
}

func (s *minimalStore) ReleaseExpiredWorkflowTasks(ctx context.Context, now time.Time, timeout time.Duration) (int, error) {
	return 0, nil
}

func (s *minimalStore) WaitForCompletion(ctx context.Context, workflowID, runID string) (journal.Event, error) {
	return nil, nil
}

func (s *minimalStore) GetStatus(ctx context.Context, workflowID, runID string) (journal.WorkflowStatus, error) {
	return journal.WorkflowStatusUnknown, nil
}

func (s *minimalStore) RequeueForRetry(ctx context.Context, workflowID, runID string, scheduledAt int, info journal.RequeueInfo) error {
	return nil
}

func (s *minimalStore) SignalWorkflow(ctx context.Context, workflowID, signalName string, payload []byte) error {
	return nil
}

func (s *minimalStore) RecordHeartbeat(ctx context.Context, workflowID, runID string, scheduledAt int, details []byte) error {
	return nil
}

func (s *minimalStore) CreateWorkflow(ctx context.Context, workflowID, workflowType string, input []byte, startedAt time.Time) (string, error) {
	return "test-run", nil
}

func (s *minimalStore) CancelWorkflow(ctx context.Context, workflowID, reason string) error {
	return nil
}

func (s *minimalStore) ListWorkflows(ctx context.Context, opts ...journal.ListWorkflowsOption) (*journal.ListWorkflowsResult, error) {
	return &journal.ListWorkflowsResult{}, nil
}

func TestExecutionHistory_FreshExecution(t *testing.T) {
	store := newMinimalStore()
	h := newExecutionState("wf-1", "run-1", nil, store)

	r := h.NewRecorder(0)
	recorded, _, err := r.Record(journal.TypeActivityScheduled, func() journal.Event {
		return journal.ActivityScheduled{}
	})
	if err != nil {
		t.Fatal(err)
	}

	if recorded.Base().ID != 1 {
		t.Errorf("expected provisional id 1, got %d", recorded.Base().ID)
	}

	if r.PendingCount() != 1 {
		t.Fatalf("expected 1 pending event, got %d", r.PendingCount())
	}

	if err := r.Commit(context.Background(), 0); err != nil {
		t.Fatal(err)
	}

	pending := h.PendingEvents()
	if len(pending) != 1 {
		t.Fatalf("expected 1 pending event, got %d", len(pending))
	}
}

func TestExecutionHistory_ReplayThenNew(t *testing.T) {
	preloaded := []journal.Event{
		journal.WorkflowStarted{BaseEvent: journal.BaseEvent{ID: 1}},
		journal.WorkflowTaskScheduled{BaseEvent: journal.BaseEvent{ID: 2}},
		journal.ActivityScheduled{BaseEvent: journal.BaseEvent{ID: 3}},
	}

	store := newMinimalStore()
	store.lastEventID["wf-1/run-1"] = 3
	h := newExecutionState("wf-1", "run-1", preloaded, store)

	r := h.NewRecorder(0)
	recorded, _, err := r.Record(journal.TypeActivityScheduled, func() journal.Event {
		return journal.ActivityScheduled{}
	})
	if err != nil {
		t.Fatal(err)
	}
	if recorded.Base().ID != 3 {
		t.Errorf("replay should return recorded ID 3, got %d", recorded.Base().ID)
	}

	if r.PendingCount() != 0 {
		t.Errorf("expected 0 pending during replay, got %d", r.PendingCount())
	}

	recorded, _, err = r.Record(journal.TypeWorkflowCompleted, func() journal.Event {
		return journal.WorkflowCompleted{}
	})
	if err != nil {
		t.Fatal(err)
	}
	if recorded.Base().ID != 4 {
		t.Errorf("new event should get provisional ID 4, got %d", recorded.Base().ID)
	}

	if r.PendingCount() != 1 {
		t.Errorf("expected 1 pending event, got %d", r.PendingCount())
	}

	if err := r.Commit(context.Background(), 0); err != nil {
		t.Fatal(err)
	}

	if len(h.PendingEvents()) != 1 {
		t.Errorf("expected 1 pending event after commit, got %d", len(h.PendingEvents()))
	}
}

func TestExecutionHistory_NondeterminismDetection(t *testing.T) {
	preloaded := []journal.Event{
		journal.WorkflowStarted{BaseEvent: journal.BaseEvent{ID: 1}},
		journal.ActivityScheduled{BaseEvent: journal.BaseEvent{ID: 2}},
	}

	store := newMinimalStore()
	h := newExecutionState("wf-1", "run-1", preloaded, store)

	r := h.NewRecorder(0)
	_, _, err := r.Record(journal.TypeWorkflowCompleted, func() journal.Event {
		return journal.WorkflowCompleted{}
	})

	if err == nil {
		t.Fatal("expected nondeterminism error")
	}

	nde, ok := err.(*NondeterminismError)
	if !ok {
		t.Fatalf("expected NondeterminismError, got %T", err)
	}

	if nde.Expected != journal.TypeActivityScheduled {
		t.Errorf("expected type %s, got %s", journal.TypeActivityScheduled, nde.Expected)
	}
	if nde.Got != journal.TypeWorkflowCompleted {
		t.Errorf("got type %s, expected %s", journal.TypeWorkflowCompleted, nde.Got)
	}
}

func TestExecutionHistory_GetByScheduledID(t *testing.T) {
	preloaded := []journal.Event{
		journal.ActivityScheduled{BaseEvent: journal.BaseEvent{ID: 1}},
		journal.ActivityCompleted{BaseEvent: journal.BaseEvent{ID: 2, ScheduledByID: 1}},
	}

	store := newMinimalStore()
	h := newExecutionState("wf-1", "run-1", preloaded, store)

	results := h.GetByScheduledID(1)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if results[0].EventType() != journal.TypeActivityCompleted {
		t.Errorf("expected ActivityCompleted, got %s", results[0].EventType())
	}
}

func TestExecutionHistory_VisibilityBoundary(t *testing.T) {
	// Visibility is set by NewRecorder based on WorkflowTaskStarted for the current task.
	// Events with ID <= WorkflowTaskStarted.ID are visible during replay.
	preloaded := []journal.Event{
		journal.WorkflowTaskScheduled{BaseEvent: journal.BaseEvent{ID: 1}},
		journal.WorkflowTaskStarted{BaseEvent: journal.BaseEvent{ID: 2, ScheduledByID: 1}},
		journal.ActivityScheduled{BaseEvent: journal.BaseEvent{ID: 3}},
		journal.ActivityCompleted{BaseEvent: journal.BaseEvent{ID: 4, ScheduledByID: 3}},
		journal.WorkflowTaskCompleted{BaseEvent: journal.BaseEvent{ID: 5, ScheduledByID: 1}},
		// Another completion arrives after task 1 completed, then new task
		journal.ActivityCompleted{BaseEvent: journal.BaseEvent{ID: 6, ScheduledByID: 3}},
		journal.WorkflowTaskScheduled{BaseEvent: journal.BaseEvent{ID: 7}},
		journal.WorkflowTaskStarted{BaseEvent: journal.BaseEvent{ID: 8, ScheduledByID: 7}},
	}

	store := newMinimalStore()
	h := newExecutionState("wf-1", "run-1", preloaded, store)

	// Initially visibility is at lastEventID (8)
	results := h.GetByScheduledID(3)
	if len(results) != 2 {
		t.Fatalf("expected 2 results initially, got %d", len(results))
	}

	// NewRecorder for task scheduled at ID=1 sets visibility to WorkflowTaskStarted ID=2
	// At that point, ActivityCompleted(4) and ActivityCompleted(6) didn't exist yet
	h.NewRecorder(1)
	results = h.GetByScheduledID(3)
	if len(results) != 0 {
		t.Errorf("expected 0 results for task 1 (visibility=2, completions at 4,6), got %d", len(results))
	}

	// NewRecorder for task scheduled at ID=7 sets visibility to WorkflowTaskStarted ID=8
	// At that point, both completions existed
	h2 := newExecutionState("wf-1", "run-1", preloaded, store)
	h2.NewRecorder(7)
	results = h2.GetByScheduledID(3)
	if len(results) != 2 {
		t.Errorf("expected 2 results for task 7 (visibility=8, completions at 4,6), got %d", len(results))
	}
}

func TestExecutionHistory_AddEvents(t *testing.T) {
	store := newMinimalStore()
	h := newExecutionState("wf-1", "run-1", nil, store)

	r := h.NewRecorder(0)
	_, _, _ = r.Record(journal.TypeActivityScheduled, func() journal.Event {
		return journal.ActivityScheduled{}
	})
	r.Commit(context.Background(), 0)

	h.AddEvents([]journal.Event{
		journal.ActivityCompleted{BaseEvent: journal.BaseEvent{ID: 2, ScheduledByID: 1}},
	})

	if h.LastEventID() != 2 {
		t.Errorf("expected lastEventID 2, got %d", h.LastEventID())
	}

	results := h.GetByScheduledID(1)
	if len(results) != 1 {
		t.Fatalf("expected 1 result after AddEvents, got %d", len(results))
	}
}

func TestRecorder_GenerateNotCalledDuringReplay(t *testing.T) {
	preloaded := []journal.Event{
		journal.ActivityScheduled{BaseEvent: journal.BaseEvent{ID: 1}},
	}

	store := newMinimalStore()
	h := newExecutionState("wf-1", "run-1", preloaded, store)

	called := false
	r := h.NewRecorder(0)
	_, _, err := r.Record(journal.TypeActivityScheduled, func() journal.Event {
		called = true
		return journal.ActivityScheduled{}
	})
	if err != nil {
		t.Fatal(err)
	}

	if called {
		t.Error("generate function should not be called during replay")
	}
}
