package derecho

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/atmonostorm/derecho/journal"
)

// Any byte sequence produces a valid workflow.
type byteReader struct {
	data []byte
	pos  int
}

func newByteReader(data []byte) *byteReader {
	return &byteReader{data: data}
}

func (r *byteReader) byte() byte {
	if r.pos >= len(r.data) {
		return 0
	}
	b := r.data[r.pos]
	r.pos++
	return b
}

func (r *byteReader) intn(n int) int {
	if n <= 0 {
		return 0
	}
	return int(r.byte()) % n
}

func (r *byteReader) bool() bool {
	return r.byte()&1 == 1
}

func (r *byteReader) remaining() int {
	return len(r.data) - r.pos
}

type op int

const (
	opSchedule      op = iota // Schedule activity, add future to pending list
	opGet                     // Get specific pending future by index
	opGetAll                  // Get all pending futures in order
	opGo                      // Spawn fiber with nested block
	opYield                   // Explicit yield point
	opSleep                   // Sleep for a duration
	opSelect                  // Select from multiple pending futures
	opChildWorkflow           // Schedule child workflow
	opCount                   // sentinel
)

func (o op) String() string {
	//exhaustive:ignore
	switch o {
	case opSchedule:
		return "Schedule"
	case opGet:
		return "Get"
	case opGetAll:
		return "GetAll"
	case opGo:
		return "Go"
	case opYield:
		return "Yield"
	case opSleep:
		return "Sleep"
	case opSelect:
		return "Select"
	case opChildWorkflow:
		return "ChildWorkflow"
	default:
		return fmt.Sprintf("op(%d)", o)
	}
}

type operation struct {
	op       op
	name     string // activity name for opSchedule
	index    int    // future index for opGet
	indices  []int  // future indices for opSelect
	children []ast  // nested block for opGo
	duration int    // sleep duration in ms for opSleep
	childID  int    // child workflow ID for opChildWorkflow
}

type ast struct {
	ops []operation
}

// activityNames for fuzzing - enough variety to catch ordering bugs
var fuzzActivityNames = []string{"a", "b", "c", "d", "e", "f", "g", "h"}

func decode(data []byte) ast {
	r := newByteReader(data)
	return decodeBlock(r, 0)
}

func decodeBlock(r *byteReader, depth int) ast {
	// Limit depth and ops to prevent pathological cases
	maxOps := 12
	if depth > 3 {
		maxOps = 4
	}
	if depth > 5 {
		maxOps = 2
	}

	numOps := r.intn(maxOps) + 1
	result := ast{ops: make([]operation, 0, numOps)}
	pendingCount := 0

	for i := 0; i < numOps; i++ {
		var o op
		if depth > 4 {
			o = op(r.intn(3))
			if o == opGo {
				o = opSchedule
			}
		} else if pendingCount == 0 {
			choices := []op{opSchedule, opGo, opYield, opSleep}
			o = choices[r.intn(len(choices))]
		} else {
			o = op(r.intn(int(opCount)))
		}

		//exhaustive:ignore
		switch o {
		case opSchedule:
			name := fuzzActivityNames[r.intn(len(fuzzActivityNames))]
			result.ops = append(result.ops, operation{op: o, name: name})
			pendingCount++

		case opGet:
			if pendingCount > 0 {
				idx := r.intn(pendingCount)
				result.ops = append(result.ops, operation{op: o, index: idx})
				pendingCount--
			}

		case opGetAll:
			if pendingCount > 0 {
				result.ops = append(result.ops, operation{op: o})
				pendingCount = 0
			}

		case opGo:
			child := decodeBlock(r, depth+1)
			result.ops = append(result.ops, operation{op: o, children: []ast{child}})

		case opYield:
			result.ops = append(result.ops, operation{op: o})

		case opSleep:
			durationMs := r.intn(100) + 1
			result.ops = append(result.ops, operation{op: o, duration: durationMs})

		case opSelect:
			if pendingCount >= 2 {
				// Select 2-4 futures from pending
				count := min(r.intn(3)+2, pendingCount)
				indices := make([]int, count)
				for j := 0; j < count; j++ {
					indices[j] = r.intn(pendingCount)
				}
				result.ops = append(result.ops, operation{op: o, indices: indices})
				pendingCount-- // One future consumed by select
			}

		case opChildWorkflow:
			childID := r.intn(100)
			result.ops = append(result.ops, operation{op: o, childID: childID})
			pendingCount++
		}
	}

	if pendingCount > 0 {
		result.ops = append(result.ops, operation{op: opGetAll})
	}

	return result
}

func (a ast) String() string {
	return formatAST(a, 0)
}

func formatAST(a ast, indent int) string {
	var sb strings.Builder
	prefix := strings.Repeat("  ", indent)

	for _, op := range a.ops {
		sb.WriteString(prefix)
		//exhaustive:ignore
		switch op.op {
		case opSchedule:
			sb.WriteString(fmt.Sprintf("Schedule(%s)\n", op.name))
		case opGet:
			sb.WriteString(fmt.Sprintf("Get(%d)\n", op.index))
		case opGetAll:
			sb.WriteString("GetAll\n")
		case opGo:
			sb.WriteString("Go {\n")
			sb.WriteString(formatAST(op.children[0], indent+1))
			sb.WriteString(prefix + "}\n")
		case opYield:
			sb.WriteString("Yield\n")
		case opSleep:
			sb.WriteString(fmt.Sprintf("Sleep(%dms)\n", op.duration))
		case opSelect:
			sb.WriteString(fmt.Sprintf("Select(%v)\n", op.indices))
		case opChildWorkflow:
			sb.WriteString(fmt.Sprintf("ChildWorkflow(%d)\n", op.childID))
		}
	}

	return sb.String()
}

func compile(a ast) func(Context) {
	return func(ctx Context) {
		executeBlock(ctx, a)
	}
}

func executeBlock(ctx Context, a ast) {
	var futures []Future[string]

	for _, op := range a.ops {
		//exhaustive:ignore
		switch op.op {
		case opSchedule:
			ref := NewActivityRef[struct{}, string](op.name)
			f := ref.Execute(ctx, struct{}{})
			futures = append(futures, f)

		case opGet:
			if op.index < len(futures) {
				futures[op.index].Get(ctx)
				last := len(futures) - 1
				futures[op.index] = futures[last]
				futures = futures[:last]
			}

		case opGetAll:
			for _, f := range futures {
				f.Get(ctx)
			}
			futures = nil

		case opGo:
			child := op.children[0]
			Go(ctx, func(ctx Context) {
				executeBlock(ctx, child)
			})

		case opYield:
			if y, ok := ctx.(yielder); ok {
				y.Yield()
			}

		case opSleep:
			Sleep(ctx, time.Duration(op.duration)*time.Millisecond)

		case opSelect:
			if len(op.indices) >= 2 && len(futures) >= 2 {
				selector := NewSelector()
				var winnerIdx int
				for _, idx := range op.indices {
					if idx < len(futures) {
						capturedIdx := idx
						AddFuture(selector, futures[idx], func(_ string, _ error) {
							winnerIdx = capturedIdx
						})
					}
				}
				selector.Select(ctx)
				// Remove winner from futures
				if winnerIdx < len(futures) {
					last := len(futures) - 1
					futures[winnerIdx] = futures[last]
					futures = futures[:last]
				}
			}

		case opChildWorkflow:
			ref := NewChildWorkflowRef[int, string]("fuzz-child")
			workflowID := fmt.Sprintf("child-%d", op.childID)
			f := ref.Execute(ctx, workflowID, op.childID)
			futures = append(futures, f)
		}
	}
}

type fuzzResult struct {
	completed       bool
	events          []journal.Event
	completionOrder []int // ScheduledByID order of completions
	workflowErr     error
	engineErr       error
	enginePanic     any
}

type fuzzStore struct {
	events            map[string][]journal.Event
	lastEventID       map[string]int
	pendingTasks      []journal.PendingWorkflowTask
	pendingActivities []pendingActivityInfo
	pendingTimers     []pendingTimerInfo
	pendingChildren   []pendingChildInfo
	childToParent     map[string]childLink // childKey -> parent info
	completionOrder   []int
}

type pendingChildInfo struct {
	workflowID string
	runID      string
}

type childLink struct {
	parentWorkflowID string
	parentRunID      string
	scheduledAt      int
}

type pendingActivityInfo struct {
	workflowID  string
	runID       string
	scheduledID int
	name        string
}

type pendingTimerInfo struct {
	workflowID  string
	runID       string
	scheduledID int
}

func newFuzzStore() *fuzzStore {
	return &fuzzStore{
		events:        make(map[string][]journal.Event),
		lastEventID:   make(map[string]int),
		childToParent: make(map[string]childLink),
	}
}

func (fs *fuzzStore) key(workflowID, runID string) string {
	return journal.WorkflowKey(workflowID, runID)
}

func (fs *fuzzStore) Load(ctx context.Context, workflowID, runID string) ([]journal.Event, error) {
	return fs.events[fs.key(workflowID, runID)], nil
}

func (fs *fuzzStore) LoadFrom(ctx context.Context, workflowID, runID string, afterEventID int) ([]journal.Event, error) {
	all := fs.events[fs.key(workflowID, runID)]
	var result []journal.Event
	for _, ev := range all {
		if ev.Base().ID > afterEventID {
			result = append(result, ev)
		}
	}
	return result, nil
}

func (fs *fuzzStore) Append(ctx context.Context, workflowID, runID string, events []journal.Event, scheduledByEventID int) ([]int, error) {
	key := fs.key(workflowID, runID)
	ids := make([]int, len(events))

	// matches real store: assign all IDs before side effects to avoid interleaving
	eventID := fs.lastEventID[key]
	for i, ev := range events {
		eventID++
		ids[i] = eventID
		fs.events[key] = append(fs.events[key], ev.WithID(eventID))
	}
	fs.lastEventID[key] = eventID

	for i, ev := range events {
		switch e := ev.(type) {
		case journal.WorkflowTaskScheduled:
			fs.pendingTasks = append(fs.pendingTasks, journal.PendingWorkflowTask{
				WorkflowID:  workflowID,
				RunID:       runID,
				ScheduledAt: ids[i],
			})
		case journal.ActivityScheduled:
			fs.pendingActivities = append(fs.pendingActivities, pendingActivityInfo{
				workflowID:  workflowID,
				runID:       runID,
				scheduledID: ids[i],
				name:        e.Name,
			})
		case journal.TimerScheduled:
			fs.pendingTimers = append(fs.pendingTimers, pendingTimerInfo{
				workflowID:  workflowID,
				runID:       runID,
				scheduledID: ids[i],
			})
		case journal.ChildWorkflowScheduled:
			fs.spawnChildWorkflow(workflowID, runID, e, ids[i])
		case journal.WorkflowCompleted, journal.WorkflowFailed:
			fs.handleWorkflowCompletion(workflowID, runID, ev)
		}
	}

	return ids, nil
}

func (fs *fuzzStore) spawnChildWorkflow(parentWorkflowID, parentRunID string, scheduled journal.ChildWorkflowScheduled, scheduledAt int) {
	parentKey := fs.key(parentWorkflowID, parentRunID)
	childRunID := fmt.Sprintf("run-%d", time.Now().UnixNano())
	childKey := fs.key(scheduled.WorkflowID, childRunID)

	fs.events[childKey] = []journal.Event{
		journal.WorkflowStarted{
			BaseEvent:    journal.BaseEvent{ID: 1},
			WorkflowType: scheduled.WorkflowType,
			Args:         scheduled.Input,
			StartedAt:    time.Now(),
		},
		journal.WorkflowTaskScheduled{
			BaseEvent: journal.BaseEvent{ID: 2},
		},
	}
	fs.lastEventID[childKey] = 2

	fs.pendingTasks = append(fs.pendingTasks, journal.PendingWorkflowTask{
		WorkflowID:  scheduled.WorkflowID,
		RunID:       childRunID,
		ScheduledAt: 2,
	})

	fs.childToParent[childKey] = childLink{
		parentWorkflowID: parentWorkflowID,
		parentRunID:      parentRunID,
		scheduledAt:      scheduledAt,
	}

	fs.pendingChildren = append(fs.pendingChildren, pendingChildInfo{
		workflowID: scheduled.WorkflowID,
		runID:      childRunID,
	})

	parentEventID := fs.lastEventID[parentKey] + 1
	fs.events[parentKey] = append(fs.events[parentKey], journal.ChildWorkflowStarted{
		BaseEvent:  journal.BaseEvent{ID: parentEventID, ScheduledByID: scheduledAt},
		ChildRunID: childRunID,
		StartedAt:  time.Now(),
	})
	fs.lastEventID[parentKey] = parentEventID
}

func (fs *fuzzStore) handleWorkflowCompletion(workflowID, runID string, ev journal.Event) {
	key := fs.key(workflowID, runID)

	link, ok := fs.childToParent[key]
	if !ok {
		return
	}

	// avoid double-completion when engine completes child
	for i, child := range fs.pendingChildren {
		if child.workflowID == workflowID && child.runID == runID {
			fs.pendingChildren = slices.Delete(fs.pendingChildren, i, i+1)
			break
		}
	}

	fs.notifyParentOfChildCompletion(link, workflowID, runID, ev)
	delete(fs.childToParent, key)
}

func (fs *fuzzStore) notifyParentOfChildCompletion(link childLink, childWorkflowID, childRunID string, childCompletionEvent journal.Event) {
	parentKey := fs.key(link.parentWorkflowID, link.parentRunID)

	var completionEvent journal.Event
	switch e := childCompletionEvent.(type) {
	case journal.WorkflowCompleted:
		completionEvent = journal.ChildWorkflowCompleted{
			BaseEvent:       journal.BaseEvent{ScheduledByID: link.scheduledAt},
			ChildWorkflowID: childWorkflowID,
			ChildRunID:      childRunID,
			Result:          e.Result,
		}
	case journal.WorkflowFailed:
		completionEvent = journal.ChildWorkflowFailed{
			BaseEvent:       journal.BaseEvent{ScheduledByID: link.scheduledAt},
			ChildWorkflowID: childWorkflowID,
			ChildRunID:      childRunID,
			Error:           e.Error,
		}
	default:
		return
	}

	eventID := fs.lastEventID[parentKey] + 1
	fs.events[parentKey] = append(fs.events[parentKey], completionEvent.WithID(eventID))
	fs.lastEventID[parentKey] = eventID

	// COALESCING: Only schedule wake-up if no pending task for parent
	for _, task := range fs.pendingTasks {
		if task.WorkflowID == link.parentWorkflowID && task.RunID == link.parentRunID {
			return
		}
	}

	taskEventID := eventID + 1
	fs.events[parentKey] = append(fs.events[parentKey], journal.WorkflowTaskScheduled{
		BaseEvent: journal.BaseEvent{ID: taskEventID},
	})
	fs.lastEventID[parentKey] = taskEventID

	fs.pendingTasks = append(fs.pendingTasks, journal.PendingWorkflowTask{
		WorkflowID:  link.parentWorkflowID,
		RunID:       link.parentRunID,
		ScheduledAt: taskEventID,
	})
}

func (fs *fuzzStore) WaitForWorkflowTasks(ctx context.Context, workerID string, maxNew int) ([]journal.PendingWorkflowTask, error) {
	if len(fs.pendingTasks) == 0 {
		<-ctx.Done()
		return nil, ctx.Err()
	}

	// Only return ONE task at a time to match production behavior
	// This ensures coalescing works correctly when children complete during processing
	task := fs.pendingTasks[0]
	fs.pendingTasks = fs.pendingTasks[1:]

	now := time.Now()
	key := fs.key(task.WorkflowID, task.RunID)

	startedID := fs.lastEventID[key] + 1
	fs.events[key] = append(fs.events[key], journal.WorkflowTaskStarted{
		BaseEvent: journal.BaseEvent{ID: startedID, ScheduledByID: task.ScheduledAt},
		WorkerID:  workerID,
		StartedAt: now,
	})
	fs.lastEventID[key] = startedID

	return []journal.PendingWorkflowTask{{
		WorkflowID:  task.WorkflowID,
		RunID:       task.RunID,
		ScheduledAt: task.ScheduledAt,
		StartedAt:   now,
	}}, nil
}

func (fs *fuzzStore) WaitForActivityTasks(ctx context.Context, workerID string, maxActivities int) ([]journal.PendingActivityTask, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (fs *fuzzStore) GetTimersToFire(ctx context.Context, now time.Time) ([]journal.PendingTimerTask, error) {
	return nil, nil
}

func (fs *fuzzStore) GetTimedOutActivities(ctx context.Context, now time.Time) ([]journal.TimedOutActivity, error) {
	return nil, nil
}

func (fs *fuzzStore) ReleaseExpiredWorkflowTasks(ctx context.Context, now time.Time, timeout time.Duration) (int, error) {
	return 0, nil
}

func (fs *fuzzStore) WaitForCompletion(ctx context.Context, workflowID, runID string) (journal.Event, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (fs *fuzzStore) GetStatus(ctx context.Context, workflowID, runID string) (journal.WorkflowStatus, error) {
	return journal.WorkflowStatusUnknown, nil
}

func (fs *fuzzStore) RequeueForRetry(ctx context.Context, workflowID, runID string, scheduledAt int, info journal.RequeueInfo) error {
	return nil
}

func (fs *fuzzStore) SignalWorkflow(ctx context.Context, workflowID, signalName string, payload []byte) error {
	return nil
}

func (fs *fuzzStore) RecordHeartbeat(ctx context.Context, workflowID, runID string, scheduledAt int, details []byte) error {
	return nil
}

func (fs *fuzzStore) CreateWorkflow(ctx context.Context, workflowID, workflowType string, input []byte, startedAt time.Time) (string, error) {
	runID := "fuzz-run"
	key := fs.key(workflowID, runID)

	fs.events[key] = []journal.Event{
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
	fs.lastEventID[key] = 2

	fs.pendingTasks = append(fs.pendingTasks, journal.PendingWorkflowTask{
		WorkflowID:  workflowID,
		RunID:       runID,
		ScheduledAt: 2,
	})

	return runID, nil
}

func (fs *fuzzStore) CancelWorkflow(ctx context.Context, workflowID, reason string) error {
	return nil
}

func (fs *fuzzStore) ListWorkflows(ctx context.Context, opts ...journal.ListWorkflowsOption) (*journal.ListWorkflowsResult, error) {
	return &journal.ListWorkflowsResult{}, nil
}

func (fs *fuzzStore) hasPendingTasks() bool {
	return len(fs.pendingTasks) > 0
}

func (fs *fuzzStore) hasPendingActivities() bool {
	return len(fs.pendingActivities) > 0
}

func (fs *fuzzStore) completeActivity(r *byteReader, succeed bool) {
	if len(fs.pendingActivities) == 0 {
		return
	}

	idx := r.intn(len(fs.pendingActivities))
	activity := fs.pendingActivities[idx]
	fs.completionOrder = append(fs.completionOrder, activity.scheduledID)
	fs.pendingActivities = slices.Delete(fs.pendingActivities, idx, idx+1)

	fs.Append(context.Background(), activity.workflowID, activity.runID, []journal.Event{
		journal.ActivityStarted{
			BaseEvent: journal.BaseEvent{ScheduledByID: activity.scheduledID},
			WorkerID:  "fuzz-worker",
			StartedAt: time.Now(),
		},
	}, 0)

	var resultEvent journal.Event
	if succeed {
		resultJSON := []byte(fmt.Sprintf(`"%s-result"`, activity.name))
		resultEvent = journal.ActivityCompleted{
			BaseEvent: journal.BaseEvent{ScheduledByID: activity.scheduledID},
			Result:    resultJSON,
		}
	} else {
		resultEvent = journal.ActivityFailed{
			BaseEvent: journal.BaseEvent{ScheduledByID: activity.scheduledID},
			Error:     journal.NewError(journal.ErrorKindApplication, "fuzz failure"),
		}
	}
	fs.Append(context.Background(), activity.workflowID, activity.runID, []journal.Event{resultEvent}, 0)

	fs.Append(context.Background(), activity.workflowID, activity.runID, []journal.Event{
		journal.WorkflowTaskScheduled{},
	}, 0)
}

func (fs *fuzzStore) hasPendingTimers() bool {
	return len(fs.pendingTimers) > 0
}

func (fs *fuzzStore) fireTimer(r *byteReader) {
	if len(fs.pendingTimers) == 0 {
		return
	}

	idx := r.intn(len(fs.pendingTimers))
	timer := fs.pendingTimers[idx]
	fs.completionOrder = append(fs.completionOrder, timer.scheduledID)
	fs.pendingTimers = slices.Delete(fs.pendingTimers, idx, idx+1)

	fs.Append(context.Background(), timer.workflowID, timer.runID, []journal.Event{
		journal.TimerFired{
			BaseEvent: journal.BaseEvent{ScheduledByID: timer.scheduledID},
			FiredAt:   time.Now(),
		},
	}, 0)

	fs.Append(context.Background(), timer.workflowID, timer.runID, []journal.Event{
		journal.WorkflowTaskScheduled{},
	}, 0)
}

func (fs *fuzzStore) hasPendingChildren() bool {
	return len(fs.pendingChildren) > 0
}

func (fs *fuzzStore) completeChildWorkflow(r *byteReader) {
	if len(fs.pendingChildren) == 0 {
		return
	}

	idx := r.intn(len(fs.pendingChildren))
	child := fs.pendingChildren[idx]
	fs.pendingChildren = slices.Delete(fs.pendingChildren, idx, idx+1)

	key := fs.key(child.workflowID, child.runID)

	var scheduledAt int
	for _, ev := range fs.events[key] {
		if ev.EventType() == journal.TypeWorkflowTaskScheduled {
			scheduledAt = ev.Base().ID
		}
	}

	resultJSON := []byte(`"child-result"`)
	fs.Append(context.Background(), child.workflowID, child.runID, []journal.Event{
		journal.WorkflowTaskStarted{
			BaseEvent: journal.BaseEvent{ScheduledByID: scheduledAt},
			WorkerID:  "fuzz-worker",
			StartedAt: time.Now(),
		},
		journal.WorkflowTaskCompleted{
			BaseEvent: journal.BaseEvent{ScheduledByID: scheduledAt},
		},
		journal.WorkflowCompleted{
			Result: resultJSON,
		},
	}, 0)
}

func (fs *fuzzStore) isWorkflowComplete(workflowID, runID string) bool {
	events, _ := fs.Load(context.Background(), workflowID, runID)
	for _, ev := range events {
		switch ev.EventType() {
		case journal.TypeWorkflowCompleted, journal.TypeWorkflowFailed, journal.TypeWorkflowCancelled:
			return true
		}
	}
	return false
}

func fuzzRun(data []byte, wf func(Context), timeout time.Duration) fuzzResult {
	r := newByteReader(data)
	astReader := newByteReader(data)
	decode(data)
	for i := 0; i < len(data)-astReader.remaining(); i++ {
		r.byte()
	}

	store := newFuzzStore()
	engine, err := NewEngine(store, WithEngineLogger(testLogger()))
	if err != nil {
		panic(err)
	}

	for _, name := range fuzzActivityNames {
		name := name
		RegisterActivity(engine, name, func(_ context.Context, _ struct{}) (string, error) {
			return name + "-result", nil
		})
	}

	RegisterWorkflow(engine, "fuzz", func(ctx Context, _ struct{}) (struct{}, error) {
		wf(ctx)
		return struct{}{}, nil
	})

	RegisterWorkflow(engine, "fuzz-child", func(ctx Context, id int) (string, error) {
		return "child-result", nil
	})

	var result fuzzResult

	func() {
		defer func() {
			if p := recover(); p != nil {
				result.enginePanic = p
			}
		}()

		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		client := engine.Client()
		workflowWorker := engine.WorkflowWorker()

		run, err := client.StartWorkflow(ctx, "fuzz", "wf-1", struct{}{})
		if err != nil {
			result.engineErr = fmt.Errorf("start workflow: %w", err)
			return
		}

		wfID := run.ID()
		runID := run.RunID()

		maxIterations := 1000
		for i := 0; i < maxIterations && ctx.Err() == nil; i++ {
			if err := workflowWorker.Process(ctx); err != nil {
				if ctx.Err() != nil {
					break
				}
				result.engineErr = fmt.Errorf("workflow worker: %w", err)
				return
			}

			if store.isWorkflowComplete(wfID, runID) {
				result.completed = true
				break
			}

			hasActivities := store.hasPendingActivities()
			hasTimers := store.hasPendingTimers()
			hasChildren := store.hasPendingChildren()

			if hasActivities || hasTimers || hasChildren {
				var options []int
				if hasActivities {
					options = append(options, 0)
				}
				if hasTimers {
					options = append(options, 1)
				}
				if hasChildren {
					options = append(options, 2)
				}

				switch options[r.intn(len(options))] {
				case 0:
					store.completeActivity(r, r.bool())
				case 1:
					store.fireTimer(r)
				case 2:
					store.completeChildWorkflow(r)
				}
			}
		}

		result.events, _ = store.Load(context.Background(), wfID, runID)
		result.completionOrder = store.completionOrder
	}()

	return result
}

func fuzzReplay(wf func(Context), events []journal.Event) error {
	if len(events) == 0 {
		return nil
	}

	wfID := "wf-1"
	runID := "run-1"

	store := newFuzzStore()

	for _, ev := range events {
		id := ev.Base().ID
		key := store.key(wfID, runID)
		store.events[key] = append(store.events[key], ev)
		if id > store.lastEventID[key] {
			store.lastEventID[key] = id
		}
	}

	engine, err := NewEngine(store, WithEngineLogger(testLogger()))
	if err != nil {
		panic(err)
	}

	for _, name := range fuzzActivityNames {
		name := name
		RegisterActivity(engine, name, func(_ context.Context, _ struct{}) (string, error) {
			return name + "-result", nil
		})
	}

	RegisterWorkflow(engine, "fuzz", func(ctx Context, _ struct{}) (struct{}, error) {
		wf(ctx)
		return struct{}{}, nil
	})

	RegisterWorkflow(engine, "fuzz-child", func(ctx Context, id int) (string, error) {
		return "child-result", nil
	})

	store.pendingTasks = append(store.pendingTasks, journal.PendingWorkflowTask{
		WorkflowID:  wfID,
		RunID:       runID,
		ScheduledAt: 2,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	workflowWorker := engine.WorkflowWorker()

	err = workflowWorker.Process(ctx)
	if err != nil {
		return err
	}

	return nil
}

func validateEventStream(events []journal.Event) error {
	if len(events) == 0 {
		return nil
	}

	type lifecycle struct {
		scheduledAt int
		startedAt   int
		completedAt int
	}
	activities := make(map[int]*lifecycle)
	timers := make(map[int]*lifecycle)
	workflowTasks := make(map[int]*lifecycle)

	expectedID := 1
	workflowTerminated := false
	workflowTerminalID := 0

	if events[0].EventType() != journal.TypeWorkflowStarted {
		return fmt.Errorf("first event must be WorkflowStarted, got %s", events[0].EventType())
	}

	for _, ev := range events {
		base := ev.Base()

		if base.ID != expectedID {
			return fmt.Errorf("event ID %d, expected consecutive ID %d", base.ID, expectedID)
		}
		expectedID++

		//exhaustive:ignore
		switch ev.EventType() {
		case journal.TypeWorkflowStarted:

		case journal.TypeWorkflowTaskScheduled:
			workflowTasks[base.ID] = &lifecycle{scheduledAt: base.ID}

		case journal.TypeWorkflowTaskStarted:
			lc := workflowTasks[base.ScheduledByID]
			if lc == nil {
				return fmt.Errorf("WorkflowTaskStarted references unknown WorkflowTaskScheduled ID %d", base.ScheduledByID)
			}
			if lc.startedAt != 0 {
				return fmt.Errorf("WorkflowTaskScheduled %d has duplicate Started events", base.ScheduledByID)
			}
			lc.startedAt = base.ID

		case journal.TypeWorkflowTaskCompleted:
			lc := workflowTasks[base.ScheduledByID]
			if lc == nil {
				return fmt.Errorf("WorkflowTaskCompleted references unknown WorkflowTaskScheduled ID %d", base.ScheduledByID)
			}
			if lc.startedAt == 0 {
				return fmt.Errorf("WorkflowTaskCompleted %d before Started", base.ScheduledByID)
			}
			if lc.completedAt != 0 {
				return fmt.Errorf("WorkflowTaskScheduled %d has duplicate Completed events", base.ScheduledByID)
			}
			lc.completedAt = base.ID

		case journal.TypeActivityScheduled:
			activities[base.ID] = &lifecycle{scheduledAt: base.ID}

		case journal.TypeActivityStarted:
			lc := activities[base.ScheduledByID]
			if lc == nil {
				return fmt.Errorf("ActivityStarted references unknown ActivityScheduled ID %d", base.ScheduledByID)
			}
			if lc.startedAt != 0 {
				return fmt.Errorf("ActivityScheduled %d has duplicate Started events", base.ScheduledByID)
			}
			lc.startedAt = base.ID

		case journal.TypeActivityCompleted, journal.TypeActivityFailed:
			lc := activities[base.ScheduledByID]
			if lc == nil {
				return fmt.Errorf("activity completion references unknown ActivityScheduled ID %d", base.ScheduledByID)
			}
			if lc.startedAt == 0 {
				return fmt.Errorf("activity completion for %d before Started", base.ScheduledByID)
			}
			if lc.completedAt != 0 {
				return fmt.Errorf("ActivityScheduled %d has duplicate completion events", base.ScheduledByID)
			}
			lc.completedAt = base.ID

		case journal.TypeTimerScheduled:
			timers[base.ID] = &lifecycle{scheduledAt: base.ID}

		case journal.TypeTimerFired:
			lc := timers[base.ScheduledByID]
			if lc == nil {
				return fmt.Errorf("TimerFired references unknown TimerScheduled ID %d", base.ScheduledByID)
			}
			if lc.completedAt != 0 {
				return fmt.Errorf("TimerScheduled %d has duplicate Fired events", base.ScheduledByID)
			}
			lc.completedAt = base.ID

		case journal.TypeWorkflowCompleted, journal.TypeWorkflowFailed, journal.TypeWorkflowCancelled:
			if workflowTerminated {
				return fmt.Errorf("duplicate workflow terminal event (first at %d, second at %d)", workflowTerminalID, base.ID)
			}
			workflowTerminated = true
			workflowTerminalID = base.ID
		}
	}

	for id, lc := range activities {
		if lc.startedAt == 0 {
			if !workflowTerminated {
				return fmt.Errorf("ActivityScheduled %d missing Started event (workflow not terminated)", id)
			}
			continue
		}
		if lc.completedAt == 0 {
			return fmt.Errorf("ActivityScheduled %d: Started but missing completion event", id)
		}
		if lc.startedAt <= lc.scheduledAt {
			return fmt.Errorf("ActivityScheduled %d: Started (%d) must come after Scheduled (%d)", id, lc.startedAt, lc.scheduledAt)
		}
		if lc.completedAt <= lc.startedAt {
			return fmt.Errorf("ActivityScheduled %d: Completed (%d) must come after Started (%d)", id, lc.completedAt, lc.startedAt)
		}
	}

	for id, lc := range timers {
		if lc.completedAt == 0 {
			if !workflowTerminated {
				return fmt.Errorf("TimerScheduled %d missing Fired event (workflow not terminated)", id)
			}
			continue
		}
		if lc.completedAt <= lc.scheduledAt {
			return fmt.Errorf("TimerScheduled %d: Fired (%d) must come after Scheduled (%d)", id, lc.completedAt, lc.scheduledAt)
		}
	}

	for id, lc := range workflowTasks {
		if lc.startedAt == 0 {
			return fmt.Errorf("WorkflowTaskScheduled %d missing Started event", id)
		}
		if lc.completedAt == 0 {
			return fmt.Errorf("WorkflowTaskScheduled %d missing Completed event", id)
		}
		if lc.startedAt <= lc.scheduledAt {
			return fmt.Errorf("WorkflowTaskScheduled %d: Started (%d) must come after Scheduled (%d)", id, lc.startedAt, lc.scheduledAt)
		}
		if lc.completedAt <= lc.startedAt {
			return fmt.Errorf("WorkflowTaskScheduled %d: Completed (%d) must come after Started (%d)", id, lc.completedAt, lc.startedAt)
		}
	}

	return nil
}

func fuzzCheck(data []byte, timeout time.Duration) error {
	a := decode(data)
	wf := compile(a)

	result := fuzzRun(data, wf, timeout)

	// INVARIANT: Engine must not panic
	if result.enginePanic != nil {
		return fmt.Errorf("INVARIANT VIOLATION: engine panic: %v\nAST:\n%s", result.enginePanic, a)
	}

	// INVARIANT: Engine must not error
	if result.engineErr != nil {
		return fmt.Errorf("INVARIANT VIOLATION: engine error: %v\nAST:\n%s", result.engineErr, a)
	}

	if !result.completed {
		return nil
	}

	// INVARIANT: Events must be valid
	if err := validateEventStream(result.events); err != nil {
		return fmt.Errorf("INVARIANT VIOLATION: invalid events: %v\nAST:\n%s", err, a)
	}

	// INVARIANT: Replay must be deterministic
	if err := fuzzReplay(wf, result.events); err != nil {
		var nde *NondeterminismError
		if errors.As(err, &nde) {
			return fmt.Errorf("INVARIANT VIOLATION: nondeterminism: %v\nAST:\n%s", err, a)
		}
		return fmt.Errorf("INVARIANT VIOLATION: replay error: %v\nAST:\n%s", err, a)
	}

	// INVARIANT: Double replay must be consistent
	if err := fuzzReplay(wf, result.events); err != nil {
		return fmt.Errorf("INVARIANT VIOLATION: double replay inconsistent: %v\nAST:\n%s", err, a)
	}

	return nil
}

func FuzzScheduler(f *testing.F) {
	seeds := [][]byte{
		{0},                      // 1 activity
		{1, 0},                   // 2 activities sequential
		{2, 0, 0},                // 3 activities
		{0, 2, 0},                // activity then Go
		{2, 2, 0, 2, 0},          // nested Go
		{3, 0, 0, 0, 1},          // 4 activities, get middle first
		{5, 0, 0, 0, 0, 0},       // fan-out 6 activities
		{1, 0, 4},                // schedule, schedule, yield
		{0, 3},                   // schedule, go with activity
		{4, 0, 0, 0, 0, 0, 0, 0}, // many activities, various completion orders
		{1, 7, 7},                // 2 child workflows (opChildWorkflow = 7)
		{3, 7, 7, 7},             // 3 child workflows
		{2, 0, 7},                // activity + child workflow
		{3, 7, 7, 6, 0, 1},       // child workflows with select
	}

	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		if err := fuzzCheck(data, 5*time.Second); err != nil {
			t.Fatalf("input %s: %v", hex.EncodeToString(data), err)
		}
	})
}

func TestFuzz_Decode(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		want string // substring expected in output
	}{
		{"single activity", []byte{0}, "Schedule("},
		{"two activities", []byte{1, 0}, "Schedule("},
		{"go block", []byte{0, 1, 0}, "Go {"}, // 1 op, opGo (1%3=1), nested
		{"get operation", []byte{2, 0, 0, 1}, "Get("},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := decode(tt.data)
			s := a.String()
			if !strings.Contains(s, tt.want) {
				t.Errorf("expected %q in AST:\n%s", tt.want, s)
			}
			t.Logf("AST:\n%s", s)
		})
	}
}

func TestFuzz_RunBasic(t *testing.T) {
	a := ast{ops: []operation{{op: opSchedule, name: "a"}}}
	wf := compile(a)

	result := fuzzRun([]byte{1}, wf, 10*time.Second)

	if result.enginePanic != nil {
		t.Fatalf("engine panic: %v", result.enginePanic)
	}
	if result.engineErr != nil {
		t.Fatalf("engine error: %v", result.engineErr)
	}

	t.Logf("completed=%v events=%d", result.completed, len(result.events))
	for i, ev := range result.events {
		t.Logf("  event %d: %s (id=%d)", i, ev.EventType(), ev.Base().ID)
	}
}

func TestFuzz_ParallelActivities(t *testing.T) {
	a := ast{ops: []operation{
		{op: opSchedule, name: "a"},
		{op: opSchedule, name: "b"},
		{op: opSchedule, name: "c"},
		{op: opGetAll},
	}}
	wf := compile(a)

	orders := make(map[string]bool)

	for i := 0; i < 50; i++ {
		data := []byte{byte(i), byte(i * 7), byte(i * 13)}
		result := fuzzRun(data, wf, 5*time.Second)

		if result.completed {
			orderKey := fmt.Sprintf("%v", result.completionOrder)
			orders[orderKey] = true
		}
	}

	t.Logf("observed %d unique completion orders", len(orders))
	for order := range orders {
		t.Logf("  %s", order)
	}

	if len(orders) < 2 {
		t.Errorf("expected multiple completion orders, got %d", len(orders))
	}
}

func TestFuzz_ParallelChildWorkflows(t *testing.T) {
	a := ast{ops: []operation{
		{op: opChildWorkflow, childID: 1},
		{op: opChildWorkflow, childID: 2},
		{op: opChildWorkflow, childID: 3},
		{op: opGetAll},
	}}
	wf := compile(a)

	for i := 0; i < 20; i++ {
		data := []byte{byte(i), byte(i * 7), byte(i * 13)}
		result := fuzzRun(data, wf, 5*time.Second)

		if result.enginePanic != nil {
			t.Fatalf("iteration %d: engine panic: %v", i, result.enginePanic)
		}
		if result.engineErr != nil {
			t.Fatalf("iteration %d: engine error: %v", i, result.engineErr)
		}
		if !result.completed {
			t.Logf("iteration %d: workflow did not complete (may need more iterations)", i)
		}
	}
}

func TestFuzz_ChildWorkflowCoalescing(t *testing.T) {
	// This test specifically exercises the coalescing scenario:
	// Multiple children complete while a parent task is pending
	a := ast{ops: []operation{
		{op: opChildWorkflow, childID: 1},
		{op: opChildWorkflow, childID: 2},
		{op: opChildWorkflow, childID: 3},
		{op: opChildWorkflow, childID: 4},
		{op: opChildWorkflow, childID: 5},
		{op: opGetAll},
	}}
	wf := compile(a)

	for i := 0; i < 50; i++ {
		data := []byte{byte(i), byte(i*3 + 1), byte(i*7 + 2), byte(i*11 + 3)}
		result := fuzzRun(data, wf, 10*time.Second)

		if result.enginePanic != nil {
			t.Fatalf("iteration %d: engine panic: %v\nAST:\n%s", i, result.enginePanic, a)
		}
		if result.engineErr != nil {
			t.Fatalf("iteration %d: engine error: %v\nAST:\n%s", i, result.engineErr, a)
		}

		if result.completed {
			if err := fuzzReplay(wf, result.events); err != nil {
				t.Fatalf("iteration %d: replay failed: %v", i, err)
			}
		}
	}
}

func TestFuzz_DeterminismInvariant(t *testing.T) {
	a := ast{ops: []operation{
		{op: opSchedule, name: "a"},
		{op: opSchedule, name: "b"},
		{op: opGo, children: []ast{{ops: []operation{
			{op: opSchedule, name: "c"},
		}}}},
		{op: opGetAll},
	}}
	wf := compile(a)

	result := fuzzRun([]byte{1, 2, 3, 4, 5}, wf, 10*time.Second)

	if !result.completed {
		t.Skip("workflow did not complete")
	}

	t.Logf("events: %d, completion order: %v", len(result.events), result.completionOrder)

	if err := fuzzReplay(wf, result.events); err != nil {
		t.Fatalf("replay failed: %v", err)
	}

	if err := fuzzReplay(wf, result.events); err != nil {
		t.Fatalf("double replay failed: %v", err)
	}
}

func TestFuzz_Check(t *testing.T) {
	inputs := [][]byte{
		{0},
		{1, 0},
		{2, 0, 0},
		{0, 3, 0},
		{5, 0, 0, 0, 0, 0},
	}

	for _, data := range inputs {
		t.Run(hex.EncodeToString(data), func(t *testing.T) {
			if err := fuzzCheck(data, 5*time.Second); err != nil {
				t.Fatal(err)
			}
		})
	}
}
