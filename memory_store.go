package derecho

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/atmonostorm/derecho/journal"
)

type MemoryStore struct {
	mu                sync.Mutex
	cond              *sync.Cond
	events            map[string][]journal.Event
	lastEventID       map[string]int
	processedTasks    map[string]map[int]bool
	pending           []journal.PendingWorkflowTask
	pendingActivities []journal.PendingActivityTask
	pendingTimers     []journal.PendingTimerTask
	complete          map[string]chan journal.Event
	clock             Clock
	idGen             func() string

	// Sticky routing: workflowKey → workerID
	workerAffinity map[string]string

	// Workflow completion timestamps for ListWorkflows
	completedAt map[string]time.Time

	// Child workflow relationship tracking
	childToParent    map[string]childWorkflowLink // childKey → parent info
	parentToChildren map[string][]string          // parentKey → []childKey
}

type childWorkflowLink struct {
	ParentWorkflowID string
	ParentRunID      string
	ScheduledAt      int
	ClosePolicy      journal.ParentClosePolicy
}

type MemoryStoreOption func(*MemoryStore)

func WithStoreClock(clock Clock) MemoryStoreOption {
	return func(ms *MemoryStore) {
		ms.clock = clock
	}
}

// WithIDGenerator sets the function used to generate run IDs.
func WithIDGenerator(gen func() string) MemoryStoreOption {
	return func(ms *MemoryStore) {
		ms.idGen = gen
	}
}

func NewMemoryStore(opts ...MemoryStoreOption) *MemoryStore {
	ms := &MemoryStore{
		events:           make(map[string][]journal.Event),
		lastEventID:      make(map[string]int),
		processedTasks:   make(map[string]map[int]bool),
		complete:         make(map[string]chan journal.Event),
		clock:            RealClock{},
		idGen:            randomID,
		workerAffinity:   make(map[string]string),
		completedAt:      make(map[string]time.Time),
		childToParent:    make(map[string]childWorkflowLink),
		parentToChildren: make(map[string][]string),
	}
	ms.cond = sync.NewCond(&ms.mu)
	for _, opt := range opts {
		opt(ms)
	}
	return ms
}

func randomID() string {
	var b [16]byte
	rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func (ms *MemoryStore) key(workflowID, runID string) string {
	return journal.WorkflowKey(workflowID, runID)
}

func (ms *MemoryStore) Load(ctx context.Context, workflowID, runID string) ([]journal.Event, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return ms.events[ms.key(workflowID, runID)], nil
}

func (ms *MemoryStore) LoadFrom(ctx context.Context, workflowID, runID string, afterEventID int) ([]journal.Event, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	all := ms.events[ms.key(workflowID, runID)]
	var result []journal.Event
	for _, ev := range all {
		if ev.Base().ID > afterEventID {
			result = append(result, ev)
		}
	}
	return result, nil
}

func (ms *MemoryStore) Append(ctx context.Context, workflowID, runID string, events []journal.Event, scheduledByEventID int) ([]int, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	key := ms.key(workflowID, runID)

	if scheduledByEventID > 0 {
		if ms.processedTasks[key] != nil && ms.processedTasks[key][scheduledByEventID] {
			return nil, journal.ErrAlreadyProcessed
		}
	}

	eventID := ms.lastEventID[key]
	eventIDs := make([]int, len(events))

	for i, ev := range events {
		eventID++
		eventIDs[i] = eventID
		ms.events[key] = append(ms.events[key], ev.WithID(eventID))
	}
	ms.lastEventID[key] = eventID

	if scheduledByEventID > 0 {
		if ms.processedTasks[key] == nil {
			ms.processedTasks[key] = make(map[int]bool)
		}
		ms.processedTasks[key][scheduledByEventID] = true
	}

	for i, ev := range events {
		switch ev.EventType() {
		case journal.TypeWorkflowTaskScheduled:
			ms.pending = append(ms.pending, journal.PendingWorkflowTask{
				WorkflowID:  workflowID,
				RunID:       runID,
				ScheduledAt: eventIDs[i],
			})
		case journal.TypeActivityScheduled:
			scheduled := ev.(journal.ActivityScheduled)
			task := journal.PendingActivityTask{
				WorkflowID:    workflowID,
				RunID:         runID,
				ActivityName:  scheduled.Name,
				ScheduledAt:   eventIDs[i],
				Input:         scheduled.Input,
				Attempt:       0,
				RetryPolicy:   scheduled.RetryPolicy,
				TimeoutPolicy: scheduled.TimeoutPolicy,
				ScheduledTime: scheduled.ScheduledAt,
			}
			if tp := scheduled.TimeoutPolicy; tp != nil {
				if tp.ScheduleToStartTimeout > 0 {
					task.ScheduleToStartDeadline = scheduled.ScheduledAt.Add(tp.ScheduleToStartTimeout)
				}
				if tp.ScheduleToCloseTimeout > 0 {
					task.ScheduleToCloseDeadline = scheduled.ScheduledAt.Add(tp.ScheduleToCloseTimeout)
				}
			}
			ms.pendingActivities = append(ms.pendingActivities, task)
		case journal.TypeActivityStarted:
			started := ev.(journal.ActivityStarted)
			for j := range ms.pendingActivities {
				task := &ms.pendingActivities[j]
				if task.WorkflowID == workflowID && task.RunID == runID && task.ScheduledAt == started.Base().ScheduledByID {
					task.StartedTime = started.StartedAt
					if tp := task.TimeoutPolicy; tp != nil {
						if tp.StartToCloseTimeout > 0 {
							task.StartToCloseDeadline = started.StartedAt.Add(tp.StartToCloseTimeout)
						}
						if tp.HeartbeatTimeout > 0 {
							task.HeartbeatTimeout = tp.HeartbeatTimeout
							task.HeartbeatDeadline = started.StartedAt.Add(tp.HeartbeatTimeout)
						}
					}
					break
				}
			}
		case journal.TypeTimerScheduled:
			scheduled := ev.(journal.TimerScheduled)
			ms.pendingTimers = append(ms.pendingTimers, journal.PendingTimerTask{
				WorkflowID:  workflowID,
				RunID:       runID,
				ScheduledAt: eventIDs[i],
				FireAt:      scheduled.FireAt,
			})
		case journal.TypeTimerCancelled:
			scheduledByID := ev.Base().ScheduledByID
			ms.pendingTimers = slices.DeleteFunc(ms.pendingTimers, func(t journal.PendingTimerTask) bool {
				return t.WorkflowID == workflowID && t.RunID == runID && t.ScheduledAt == scheduledByID
			})
		case journal.TypeActivityCompleted, journal.TypeActivityFailed:
			scheduledByID := ev.Base().ScheduledByID
			ms.pendingActivities = slices.DeleteFunc(ms.pendingActivities, func(t journal.PendingActivityTask) bool {
				return t.WorkflowID == workflowID && t.RunID == runID && t.ScheduledAt == scheduledByID
			})
		case journal.TypeChildWorkflowScheduled:
			scheduled := ev.(journal.ChildWorkflowScheduled)
			ms.spawnChildWorkflow(workflowID, runID, scheduled, eventIDs[i])
		case journal.TypeWorkflowCompleted, journal.TypeWorkflowFailed, journal.TypeWorkflowCancelled:
			ms.handleWorkflowCompletion(workflowID, runID, ev)
			delete(ms.workerAffinity, key)
			if ch, ok := ms.complete[key]; ok {
				ch <- ev
			}
		case journal.TypeWorkflowContinuedAsNew:
			continued := ev.(journal.WorkflowContinuedAsNew)
			ms.spawnContinuedRun(workflowID, runID, continued)
			delete(ms.workerAffinity, key)
			if ch, ok := ms.complete[key]; ok {
				ch <- ev
			}
		case journal.TypeSignalExternalScheduled:
			scheduled := ev.(journal.SignalExternalScheduled)
			ms.deliverSignalToWorkflow(scheduled.TargetWorkflowID, scheduled.SignalName, scheduled.Payload)
		}
	}

	ms.cond.Broadcast()
	return eventIDs, nil
}

func (ms *MemoryStore) WaitForWorkflowTasks(ctx context.Context, workerID string, maxNew int) ([]journal.PendingWorkflowTask, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			ms.cond.Broadcast()
		case <-done:
		}
	}()
	defer close(done)

	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		var matched, unassigned, deferred []journal.PendingWorkflowTask
		for _, task := range ms.pending {
			key := ms.key(task.WorkflowID, task.RunID)
			if ms.workerAffinity[key] == workerID {
				matched = append(matched, task)
			} else if ms.workerAffinity[key] == "" {
				unassigned = append(unassigned, task)
			} else {
				deferred = append(deferred, task)
			}
		}

		newCount := min(maxNew, len(unassigned))
		toProcess := append(matched, unassigned[:newCount]...)
		deferred = append(deferred, unassigned[newCount:]...)

		if len(toProcess) > 0 {
			ms.pending = deferred
			now := ms.clock.Now()
			result := make([]journal.PendingWorkflowTask, len(toProcess))

			for i, task := range toProcess {
				key := ms.key(task.WorkflowID, task.RunID)
				ms.workerAffinity[key] = workerID

				startedID := ms.lastEventID[key] + 1
				ms.events[key] = append(ms.events[key], journal.WorkflowTaskStarted{
					BaseEvent: journal.BaseEvent{ID: startedID, ScheduledByID: task.ScheduledAt},
					WorkerID:  workerID,
					StartedAt: now,
				})
				ms.lastEventID[key] = startedID

				result[i] = journal.PendingWorkflowTask{
					WorkflowID:  task.WorkflowID,
					RunID:       task.RunID,
					ScheduledAt: task.ScheduledAt,
					StartedAt:   now,
				}
			}
			return result, nil
		}

		ms.cond.Wait()
	}
}

func (ms *MemoryStore) WaitForActivityTasks(ctx context.Context, workerID string, maxActivities int) ([]journal.PendingActivityTask, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			ms.cond.Broadcast()
		case <-done:
		}
	}()
	defer close(done)

	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		now := ms.clock.Now()
		var ready []journal.PendingActivityTask

		for i := range ms.pendingActivities {
			if maxActivities > 0 && len(ready) >= maxActivities {
				break
			}
			task := &ms.pendingActivities[i]
			if task.Claimed {
				continue
			}
			if task.NotBefore.IsZero() || !task.NotBefore.After(now) {
				task.Claimed = true
				ready = append(ready, *task)
			}
		}

		if len(ready) > 0 {
			return ready, nil
		}

		ms.cond.Wait()
	}
}

func (ms *MemoryStore) GetTimersToFire(ctx context.Context, now time.Time) ([]journal.PendingTimerTask, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	var ready []journal.PendingTimerTask
	var remaining []journal.PendingTimerTask
	for _, t := range ms.pendingTimers {
		if !t.FireAt.After(now) {
			ready = append(ready, t)
		} else {
			remaining = append(remaining, t)
		}
	}
	ms.pendingTimers = remaining
	return ready, nil
}

func (ms *MemoryStore) GetTimedOutActivities(ctx context.Context, now time.Time) ([]journal.TimedOutActivity, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	var timedOut []journal.TimedOutActivity
	var remaining []journal.PendingActivityTask

	for _, task := range ms.pendingActivities {
		if kind := ms.checkActivityTimeout(task, now); kind != "" {
			timedOut = append(timedOut, journal.TimedOutActivity{
				WorkflowID:  task.WorkflowID,
				RunID:       task.RunID,
				ScheduledAt: task.ScheduledAt,
				TimeoutKind: kind,
			})
		} else {
			remaining = append(remaining, task)
		}
	}

	ms.pendingActivities = remaining
	return timedOut, nil
}

func (ms *MemoryStore) checkActivityTimeout(task journal.PendingActivityTask, now time.Time) journal.TimeoutKind {
	// ScheduleToClose takes precedence (overall deadline)
	if !task.ScheduleToCloseDeadline.IsZero() && !now.Before(task.ScheduleToCloseDeadline) {
		return journal.TimeoutKindScheduleToClose
	}

	// ScheduleToStart: activity not yet picked up by worker
	if task.StartedTime.IsZero() && !task.ScheduleToStartDeadline.IsZero() && !now.Before(task.ScheduleToStartDeadline) {
		return journal.TimeoutKindScheduleToStart
	}

	// StartToClose: worker started but hasn't completed
	if !task.StartedTime.IsZero() && !task.StartToCloseDeadline.IsZero() && !now.Before(task.StartToCloseDeadline) {
		return journal.TimeoutKindStartToClose
	}

	if !task.StartedTime.IsZero() && !task.HeartbeatDeadline.IsZero() && !now.Before(task.HeartbeatDeadline) {
		return journal.TimeoutKindHeartbeat
	}

	return ""
}

func (ms *MemoryStore) WaitForCompletion(ctx context.Context, workflowID, runID string) (journal.Event, error) {
	ms.mu.Lock()
	key := ms.key(workflowID, runID)
	ch, ok := ms.complete[key]
	if !ok {
		ch = make(chan journal.Event, 1)
		ms.complete[key] = ch
	}

	for _, ev := range ms.events[key] {
		switch ev.EventType() {
		case journal.TypeWorkflowCompleted, journal.TypeWorkflowFailed, journal.TypeWorkflowCancelled:
			ms.mu.Unlock()
			return ev, nil
		}
	}
	ms.mu.Unlock()

	select {
	case ev := <-ch:
		return ev, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (ms *MemoryStore) GetStatus(ctx context.Context, workflowID, runID string) (journal.WorkflowStatus, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	key := ms.key(workflowID, runID)
	events := ms.events[key]

	if len(events) == 0 {
		return journal.WorkflowStatusUnknown, nil
	}

	var cancelRequested bool
	for _, ev := range events {
		switch ev.EventType() {
		case journal.TypeWorkflowCompleted:
			return journal.WorkflowStatusCompleted, nil
		case journal.TypeWorkflowFailed:
			e := ev.(journal.WorkflowFailed)
			if e.Error != nil && e.Error.Kind == journal.ErrorKindCancelled {
				return journal.WorkflowStatusTerminated, nil
			}
			return journal.WorkflowStatusFailed, nil
		case journal.TypeWorkflowCancelled:
			return journal.WorkflowStatusCancelled, nil
		case journal.TypeWorkflowContinuedAsNew:
			return journal.WorkflowStatusContinuedAsNew, nil
		case journal.TypeWorkflowCancelRequested:
			cancelRequested = true
		}
	}

	if cancelRequested {
		return journal.WorkflowStatusCancelling, nil
	}
	return journal.WorkflowStatusRunning, nil
}

func (ms *MemoryStore) RequeueForRetry(ctx context.Context, workflowID, runID string, scheduledAt int, info journal.RequeueInfo) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	key := ms.key(workflowID, runID)

	var name string
	var input []byte
	var timeoutPolicy *journal.TimeoutPolicyPayload
	var scheduledTime time.Time
	for _, ev := range ms.events[key] {
		if ev.Base().ID == scheduledAt {
			if scheduled, ok := ev.(journal.ActivityScheduled); ok {
				name = scheduled.Name
				input = scheduled.Input
				timeoutPolicy = scheduled.TimeoutPolicy
				scheduledTime = scheduled.ScheduledAt
				break
			}
		}
	}

	var heartbeatDetails []byte
	for i := range ms.pendingActivities {
		prev := &ms.pendingActivities[i]
		if prev.WorkflowID == workflowID && prev.RunID == runID && prev.ScheduledAt == scheduledAt {
			heartbeatDetails = prev.HeartbeatDetails
			break
		}
	}

	task := journal.PendingActivityTask{
		WorkflowID:       workflowID,
		RunID:            runID,
		ActivityName:     name,
		ScheduledAt:      scheduledAt,
		Input:            input,
		Attempt:          info.Attempt,
		RetryPolicy:      info.RetryPolicy,
		NotBefore:        info.NotBefore,
		LastFailure:      info.LastFailure,
		TimeoutPolicy:    timeoutPolicy,
		ScheduledTime:    scheduledTime,
		HeartbeatDetails: heartbeatDetails,
	}

	// ScheduleToClose is absolute from original schedule; ScheduleToStart doesn't apply to retries
	if tp := timeoutPolicy; tp != nil && tp.ScheduleToCloseTimeout > 0 {
		task.ScheduleToCloseDeadline = scheduledTime.Add(tp.ScheduleToCloseTimeout)
	}

	ms.pendingActivities = append(ms.pendingActivities, task)
	return nil
}

// findRunningWorkflow returns the runID of a running workflow with the given ID,
// or empty string if none exists or all runs have terminated. Caller must hold ms.mu.
func (ms *MemoryStore) findRunningWorkflow(workflowID string) string {
	prefix := workflowID + "/"
	for key, events := range ms.events {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		if ms.isTerminated(events) {
			continue
		}
		return strings.TrimPrefix(key, prefix)
	}
	return ""
}

func (ms *MemoryStore) isTerminated(events []journal.Event) bool {
	for _, ev := range events {
		switch ev.EventType() {
		case journal.TypeWorkflowCompleted, journal.TypeWorkflowFailed,
			journal.TypeWorkflowCancelled, journal.TypeWorkflowContinuedAsNew:
			return true
		}
	}
	return false
}

// spawnChildWorkflow creates a new child workflow inline when ChildWorkflowScheduled is appended.
// If a workflow with the same WorkflowID already exists, it links to that instead of creating a duplicate.
func (ms *MemoryStore) spawnChildWorkflow(parentWorkflowID, parentRunID string, scheduled journal.ChildWorkflowScheduled, scheduledAt int) {
	parentKey := ms.key(parentWorkflowID, parentRunID)
	now := ms.clock.Now()

	// Prevents duplicate child workflows from executing same activities twice
	if existingRunID := ms.findRunningWorkflow(scheduled.WorkflowID); existingRunID != "" {
		childKey := ms.key(scheduled.WorkflowID, existingRunID)

		ms.childToParent[childKey] = childWorkflowLink{
			ParentWorkflowID: parentWorkflowID,
			ParentRunID:      parentRunID,
			ScheduledAt:      scheduledAt,
			ClosePolicy:      scheduled.ParentClosePolicy,
		}
		ms.parentToChildren[parentKey] = append(ms.parentToChildren[parentKey], childKey)

		parentEventID := ms.lastEventID[parentKey] + 1
		ms.events[parentKey] = append(ms.events[parentKey], journal.ChildWorkflowStarted{
			BaseEvent:  journal.BaseEvent{ID: parentEventID, ScheduledByID: scheduledAt},
			ChildRunID: existingRunID,
			StartedAt:  now,
		})
		ms.lastEventID[parentKey] = parentEventID
		return
	}

	childRunID := ms.idGen()
	childKey := ms.key(scheduled.WorkflowID, childRunID)

	ms.events[childKey] = append(ms.events[childKey], journal.WorkflowStarted{
		BaseEvent:    journal.BaseEvent{ID: 1},
		WorkflowType: scheduled.WorkflowType,
		Args:         scheduled.Input,
		StartedAt:    now,
	})

	ms.events[childKey] = append(ms.events[childKey], journal.WorkflowTaskScheduled{
		BaseEvent: journal.BaseEvent{ID: 2},
	})
	ms.lastEventID[childKey] = 2

	ms.pending = append(ms.pending, journal.PendingWorkflowTask{
		WorkflowID:  scheduled.WorkflowID,
		RunID:       childRunID,
		ScheduledAt: 2,
	})

	ms.childToParent[childKey] = childWorkflowLink{
		ParentWorkflowID: parentWorkflowID,
		ParentRunID:      parentRunID,
		ScheduledAt:      scheduledAt,
		ClosePolicy:      scheduled.ParentClosePolicy,
	}
	ms.parentToChildren[parentKey] = append(ms.parentToChildren[parentKey], childKey)

	parentEventID := ms.lastEventID[parentKey] + 1
	ms.events[parentKey] = append(ms.events[parentKey], journal.ChildWorkflowStarted{
		BaseEvent:  journal.BaseEvent{ID: parentEventID, ScheduledByID: scheduledAt},
		ChildRunID: childRunID,
		StartedAt:  now,
	})
	ms.lastEventID[parentKey] = parentEventID
}

func (ms *MemoryStore) spawnContinuedRun(workflowID, oldRunID string, continued journal.WorkflowContinuedAsNew) {
	oldKey := ms.key(workflowID, oldRunID)

	var workflowType string
	for _, ev := range ms.events[oldKey] {
		if ws, ok := ev.(journal.WorkflowStarted); ok {
			workflowType = ws.WorkflowType
			break
		}
	}

	newRunID := ms.idGen()
	newKey := ms.key(workflowID, newRunID)
	now := ms.clock.Now()

	ms.events[newKey] = append(ms.events[newKey], journal.WorkflowStarted{
		BaseEvent:    journal.BaseEvent{ID: 1},
		WorkflowType: workflowType,
		Args:         continued.NewInput,
		StartedAt:    now,
	})

	ms.events[newKey] = append(ms.events[newKey], journal.WorkflowTaskScheduled{
		BaseEvent: journal.BaseEvent{ID: 2},
	})
	ms.lastEventID[newKey] = 2

	ms.pending = append(ms.pending, journal.PendingWorkflowTask{
		WorkflowID:  workflowID,
		RunID:       newRunID,
		ScheduledAt: 2,
	})

	// Parent waits on child's runID; must follow continuation
	if link, ok := ms.childToParent[oldKey]; ok {
		delete(ms.childToParent, oldKey)
		ms.childToParent[newKey] = link
	}
}

// handleWorkflowCompletion handles parent notification and child termination when a workflow completes.
func (ms *MemoryStore) handleWorkflowCompletion(workflowID, runID string, ev journal.Event) {
	key := ms.key(workflowID, runID)

	ms.completedAt[key] = ms.clock.Now()

	if link, ok := ms.childToParent[key]; ok {
		ms.notifyParentOfChildCompletion(link, workflowID, runID, ev)
		delete(ms.childToParent, key)
	}

	if children, ok := ms.parentToChildren[key]; ok {
		for _, childKey := range children {
			if childLink, exists := ms.childToParent[childKey]; exists {
				if childLink.ClosePolicy == journal.ParentClosePolicyTerminate {
					ms.terminateChild(childKey)
				}
			}
		}
		delete(ms.parentToChildren, key)
	}
}

func (ms *MemoryStore) notifyParentOfChildCompletion(link childWorkflowLink, childWorkflowID, childRunID string, childCompletionEvent journal.Event) {
	parentKey := ms.key(link.ParentWorkflowID, link.ParentRunID)

	var completionEvent journal.Event
	switch e := childCompletionEvent.(type) {
	case journal.WorkflowCompleted:
		completionEvent = journal.ChildWorkflowCompleted{
			BaseEvent:       journal.BaseEvent{ScheduledByID: link.ScheduledAt},
			ChildWorkflowID: childWorkflowID,
			ChildRunID:      childRunID,
			Result:          e.Result,
		}
	case journal.WorkflowFailed:
		completionEvent = journal.ChildWorkflowFailed{
			BaseEvent:       journal.BaseEvent{ScheduledByID: link.ScheduledAt},
			ChildWorkflowID: childWorkflowID,
			ChildRunID:      childRunID,
			Error:           e.Error,
		}
	case journal.WorkflowCancelled:
		completionEvent = journal.ChildWorkflowFailed{
			BaseEvent:       journal.BaseEvent{ScheduledByID: link.ScheduledAt},
			ChildWorkflowID: childWorkflowID,
			ChildRunID:      childRunID,
			Error:           e.Error,
		}
	default:
		return
	}

	eventID := ms.lastEventID[parentKey] + 1
	ms.events[parentKey] = append(ms.events[parentKey], completionEvent.WithID(eventID))
	ms.lastEventID[parentKey] = eventID

	// Only schedule a wake-up task if there isn't already one pending for this parent.
	// Multiple child completions can share a single wake-up - the parent will process
	// all available completions when it runs.
	for _, task := range ms.pending {
		if task.WorkflowID == link.ParentWorkflowID && task.RunID == link.ParentRunID {
			return
		}
	}

	taskEventID := eventID + 1
	ms.events[parentKey] = append(ms.events[parentKey], journal.WorkflowTaskScheduled{
		BaseEvent: journal.BaseEvent{ID: taskEventID},
	})
	ms.lastEventID[parentKey] = taskEventID

	ms.pending = append(ms.pending, journal.PendingWorkflowTask{
		WorkflowID:  link.ParentWorkflowID,
		RunID:       link.ParentRunID,
		ScheduledAt: taskEventID,
	})
}

func (ms *MemoryStore) terminateChild(childKey string) {
	eventID := ms.lastEventID[childKey] + 1
	ms.events[childKey] = append(ms.events[childKey], journal.WorkflowFailed{
		BaseEvent: journal.BaseEvent{ID: eventID},
		Error:     journal.NewError(journal.ErrorKindCancelled, "parent workflow terminated"),
	})
	ms.lastEventID[childKey] = eventID

	if ch, ok := ms.complete[childKey]; ok {
		ch <- ms.events[childKey][len(ms.events[childKey])-1]
	}

	if grandchildren, ok := ms.parentToChildren[childKey]; ok {
		for _, grandchildKey := range grandchildren {
			if grandchildLink, exists := ms.childToParent[grandchildKey]; exists {
				if grandchildLink.ClosePolicy == journal.ParentClosePolicyTerminate {
					ms.terminateChild(grandchildKey)
				}
			}
		}
		delete(ms.parentToChildren, childKey)
	}
	delete(ms.childToParent, childKey)
}

func (ms *MemoryStore) SignalWorkflow(ctx context.Context, workflowID, signalName string, payload []byte) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	runID := ms.findActiveRun(workflowID)
	if runID == "" {
		return &WorkflowNotFoundError{WorkflowID: workflowID}
	}

	ms.appendSignal(workflowID, runID, signalName, payload)
	return nil
}

func (ms *MemoryStore) RecordHeartbeat(ctx context.Context, workflowID, runID string, scheduledAt int, details []byte) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	for i := range ms.pendingActivities {
		task := &ms.pendingActivities[i]
		if task.WorkflowID == workflowID && task.RunID == runID && task.ScheduledAt == scheduledAt {
			now := ms.clock.Now()
			task.LastHeartbeatTime = now
			task.HeartbeatDeadline = now.Add(task.HeartbeatTimeout)
			task.HeartbeatDetails = details
			return nil
		}
	}
	return nil
}

func (ms *MemoryStore) CreateWorkflow(ctx context.Context, workflowID, workflowType string, input []byte, startedAt time.Time) (string, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if existingRunID := ms.findRunningWorkflow(workflowID); existingRunID != "" {
		return "", journal.ErrWorkflowAlreadyRunning
	}

	runID := ms.idGen()
	key := ms.key(workflowID, runID)

	ms.events[key] = []journal.Event{
		journal.WorkflowStarted{
			BaseEvent:    journal.BaseEvent{ID: 1},
			WorkflowType: workflowType,
			Args:         input,
			StartedAt:    startedAt,
		},
		journal.WorkflowTaskScheduled{
			BaseEvent: journal.BaseEvent{ID: 2},
		},
	}
	ms.lastEventID[key] = 2

	ms.pending = append(ms.pending, journal.PendingWorkflowTask{
		WorkflowID:  workflowID,
		RunID:       runID,
		ScheduledAt: 2,
	})

	ms.cond.Broadcast()
	return runID, nil
}

func (ms *MemoryStore) findActiveRun(workflowID string) string {
	for key, events := range ms.events {
		keyWorkflowID, keyRunID, ok := strings.Cut(key, "/")
		if !ok || keyWorkflowID != workflowID {
			continue
		}

		isRunning := true
		for _, ev := range events {
			switch ev.EventType() {
			case journal.TypeWorkflowCompleted, journal.TypeWorkflowFailed, journal.TypeWorkflowCancelled:
				isRunning = false
			}
		}
		if isRunning {
			return keyRunID
		}
	}
	return ""
}

func (ms *MemoryStore) scheduleTaskIfNeeded(workflowID, runID, key string) {
	for _, task := range ms.pending {
		if task.WorkflowID == workflowID && task.RunID == runID {
			return
		}
	}

	taskEventID := ms.lastEventID[key] + 1
	ms.events[key] = append(ms.events[key], journal.WorkflowTaskScheduled{
		BaseEvent: journal.BaseEvent{ID: taskEventID},
	})
	ms.lastEventID[key] = taskEventID

	ms.pending = append(ms.pending, journal.PendingWorkflowTask{
		WorkflowID:  workflowID,
		RunID:       runID,
		ScheduledAt: taskEventID,
	})
}

func (ms *MemoryStore) deliverSignalToWorkflow(workflowID, signalName string, payload []byte) {
	runID := ms.findActiveRun(workflowID)
	if runID == "" {
		// Fire-and-forget: SignalExternalWorkflow does not track delivery success.
		return
	}

	ms.appendSignal(workflowID, runID, signalName, payload)
}

func (ms *MemoryStore) appendSignal(workflowID, runID, signalName string, payload []byte) {
	key := ms.key(workflowID, runID)
	now := ms.clock.Now()

	eventID := ms.lastEventID[key] + 1
	ms.events[key] = append(ms.events[key], journal.SignalReceived{
		BaseEvent:  journal.BaseEvent{ID: eventID},
		SignalName: signalName,
		Payload:    payload,
		SentAt:     now,
	})
	ms.lastEventID[key] = eventID

	ms.scheduleTaskIfNeeded(workflowID, runID, key)
}

func (ms *MemoryStore) CancelWorkflow(ctx context.Context, workflowID, reason string) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	runID := ms.findActiveRun(workflowID)
	if runID == "" {
		return &WorkflowNotFoundError{WorkflowID: workflowID}
	}

	key := ms.key(workflowID, runID)
	now := ms.clock.Now()

	eventID := ms.lastEventID[key] + 1
	ms.events[key] = append(ms.events[key], journal.WorkflowCancelRequested{
		BaseEvent:   journal.BaseEvent{ID: eventID},
		Reason:      reason,
		RequestedAt: now,
	})
	ms.lastEventID[key] = eventID

	ms.scheduleTaskIfNeeded(workflowID, runID, key)
	return nil
}

func (ms *MemoryStore) ListWorkflows(ctx context.Context, opts ...journal.ListWorkflowsOption) (*journal.ListWorkflowsResult, error) {
	var o journal.ListWorkflowsOptions
	for _, opt := range opts {
		opt(&o)
	}
	o.ApplyDefaults()

	cursor, err := journal.DecodeCursor(o.Cursor)
	if err != nil {
		return nil, err
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()

	var executions []journal.WorkflowExecution
	for key, events := range ms.events {
		workflowID, runID, ok := strings.Cut(key, "/")
		if !ok {
			continue
		}

		ex := ms.buildWorkflowExecution(workflowID, runID, events)

		if !ms.matchesFilters(ex, o) {
			continue
		}

		executions = append(executions, ex)
	}

	// Sort by created_at DESC, workflow_id DESC, run_id DESC
	slices.SortFunc(executions, func(a, b journal.WorkflowExecution) int {
		if !a.CreatedAt.Equal(b.CreatedAt) {
			if a.CreatedAt.After(b.CreatedAt) {
				return -1
			}
			return 1
		}
		if a.WorkflowID != b.WorkflowID {
			if a.WorkflowID > b.WorkflowID {
				return -1
			}
			return 1
		}
		if a.RunID > b.RunID {
			return -1
		}
		if a.RunID < b.RunID {
			return 1
		}
		return 0
	})

	// Apply cursor filter
	if !cursor.CreatedAt.IsZero() {
		var filtered []journal.WorkflowExecution
		for _, ex := range executions {
			if ms.isAfterCursor(ex, cursor) {
				filtered = append(filtered, ex)
			}
		}
		executions = filtered
	}

	// Apply limit
	result := &journal.ListWorkflowsResult{}
	if len(executions) > o.Limit {
		result.Executions = executions[:o.Limit]
		last := result.Executions[len(result.Executions)-1]
		result.NextCursor = journal.EncodeCursor(last.CreatedAt, last.WorkflowID, last.RunID)
	} else {
		result.Executions = executions
	}

	return result, nil
}

func (ms *MemoryStore) buildWorkflowExecution(workflowID, runID string, events []journal.Event) journal.WorkflowExecution {
	ex := journal.WorkflowExecution{
		WorkflowID: workflowID,
		RunID:      runID,
		Status:     journal.WorkflowStatusRunning,
	}

	var cancelRequested bool
	for _, ev := range events {
		switch e := ev.(type) {
		case journal.WorkflowStarted:
			ex.WorkflowType = e.WorkflowType
			ex.CreatedAt = e.StartedAt
			ex.Input = e.Args
		case journal.WorkflowCompleted:
			ex.Status = journal.WorkflowStatusCompleted
		case journal.WorkflowFailed:
			if e.Error != nil && e.Error.Kind == journal.ErrorKindCancelled {
				ex.Status = journal.WorkflowStatusTerminated
			} else {
				ex.Status = journal.WorkflowStatusFailed
			}
			ex.Error = e.Error
		case journal.WorkflowCancelled:
			ex.Status = journal.WorkflowStatusCancelled
		case journal.WorkflowContinuedAsNew:
			ex.Status = journal.WorkflowStatusContinuedAsNew
		case journal.WorkflowCancelRequested:
			cancelRequested = true
		}
	}

	if ex.Status == journal.WorkflowStatusRunning && cancelRequested {
		ex.Status = journal.WorkflowStatusCancelling
	}

	key := ms.key(workflowID, runID)
	if t, ok := ms.completedAt[key]; ok {
		ex.CompletedAt = &t
	}

	return ex
}

func (ms *MemoryStore) matchesFilters(ex journal.WorkflowExecution, o journal.ListWorkflowsOptions) bool {
	if o.Status != journal.WorkflowStatusUnknown && ex.Status != o.Status {
		return false
	}
	if o.WorkflowType != "" && ex.WorkflowType != o.WorkflowType {
		return false
	}
	if o.WorkflowID != "" && ex.WorkflowID != o.WorkflowID {
		return false
	}
	return true
}

func (ms *MemoryStore) isAfterCursor(ex journal.WorkflowExecution, cursor journal.ListCursor) bool {
	if ex.CreatedAt.Before(cursor.CreatedAt) {
		return true
	}
	if ex.CreatedAt.After(cursor.CreatedAt) {
		return false
	}
	if ex.WorkflowID < cursor.WorkflowID {
		return true
	}
	if ex.WorkflowID > cursor.WorkflowID {
		return false
	}
	return ex.RunID < cursor.RunID
}
