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
		childToParent:    make(map[string]childWorkflowLink),
		parentToChildren: make(map[string][]string),
	}
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
			ms.spawnChildWorkflow(scheduled, eventIDs[i])
		case journal.TypeWorkflowCompleted, journal.TypeWorkflowFailed:
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

	return eventIDs, nil
}

func (ms *MemoryStore) WaitForWorkflowTasks(ctx context.Context, workerID string, maxNew int) ([]journal.PendingWorkflowTask, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if len(ms.pending) == 0 {
		ms.mu.Unlock()
		<-ctx.Done()
		ms.mu.Lock()
		return nil, ctx.Err()
	}

	// Partition tasks: affinity matches vs new assignments
	var matched, unassigned, deferred []journal.PendingWorkflowTask
	for _, task := range ms.pending {
		key := ms.key(task.WorkflowID, task.RunID)
		if ms.workerAffinity[key] == workerID {
			matched = append(matched, task)
		} else if ms.workerAffinity[key] == "" {
			unassigned = append(unassigned, task)
		} else {
			// Assigned to different worker - defer
			deferred = append(deferred, task)
		}
	}

	// Take up to maxNew from unassigned
	newCount := min(maxNew, len(unassigned))
	toProcess := append(matched, unassigned[:newCount]...)
	deferred = append(deferred, unassigned[newCount:]...)

	if len(toProcess) == 0 {
		// No tasks for this worker - keep all pending and block
		ms.mu.Unlock()
		<-ctx.Done()
		ms.mu.Lock()
		return nil, ctx.Err()
	}

	ms.pending = deferred
	now := ms.clock.Now()
	result := make([]journal.PendingWorkflowTask, len(toProcess))

	for i, task := range toProcess {
		key := ms.key(task.WorkflowID, task.RunID)

		// Record affinity
		ms.workerAffinity[key] = workerID

		// Append WorkflowTaskStarted with next available ID.
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

func (ms *MemoryStore) WaitForActivityTasks(ctx context.Context, workerID string) ([]journal.PendingActivityTask, error) {
	ms.mu.Lock()

	now := ms.clock.Now()
	var ready []journal.PendingActivityTask

	for i := range ms.pendingActivities {
		task := &ms.pendingActivities[i]
		if task.Claimed {
			continue
		}
		if task.NotBefore.IsZero() || !task.NotBefore.After(now) {
			task.Claimed = true
			ready = append(ready, *task)
		}
	}

	ms.mu.Unlock()

	if len(ready) == 0 {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return ready, nil
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
		if ev.EventType() == journal.TypeWorkflowCompleted || ev.EventType() == journal.TypeWorkflowFailed {
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
		case journal.TypeWorkflowContinuedAsNew:
			return journal.WorkflowStatusContinuedAsNew, nil
		}
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

// spawnChildWorkflow creates a new child workflow inline when ChildWorkflowScheduled is appended.
func (ms *MemoryStore) spawnChildWorkflow(scheduled journal.ChildWorkflowScheduled, scheduledAt int) {
	childRunID := ms.idGen()
	childKey := ms.key(scheduled.WorkflowID, childRunID)
	parentKey := ms.key(scheduled.ParentWorkflowID, scheduled.ParentRunID)
	now := ms.clock.Now()

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
		ParentWorkflowID: scheduled.ParentWorkflowID,
		ParentRunID:      scheduled.ParentRunID,
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
}

// handleWorkflowCompletion handles parent notification and child termination when a workflow completes.
func (ms *MemoryStore) handleWorkflowCompletion(workflowID, runID string, ev journal.Event) {
	key := ms.key(workflowID, runID)

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
			case journal.TypeWorkflowCompleted, journal.TypeWorkflowFailed:
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
