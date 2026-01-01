package derecho

import (
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/atmonostorm/derecho/journal"
)

func TestMemoryStore_OrphanedAffinityClaimable(t *testing.T) {
	store := NewMemoryStore()

	runID, err := store.CreateWorkflow(t.Context(), "wf-1", "TestWorkflow", nil, time.Now())
	if err != nil {
		t.Fatal(err)
	}

	// Simulate affinity to a dead worker
	key := store.key("wf-1", runID)
	store.mu.Lock()
	store.workerAffinity[key] = "dead-worker"
	store.mu.Unlock()

	tasks, err := store.WaitForWorkflowTasks(t.Context(), "new-worker", 10)
	if err != nil {
		t.Fatal(err)
	}

	if len(tasks) != 1 {
		t.Fatalf("expected 1 task, got %d", len(tasks))
	}
	if tasks[0].WorkflowID != "wf-1" {
		t.Errorf("expected workflow ID wf-1, got %s", tasks[0].WorkflowID)
	}
}

func TestMemoryStore_MultipleCompletionWaiters(t *testing.T) {
	store := NewMemoryStore()

	runID, err := store.CreateWorkflow(t.Context(), "wf-1", "TestWorkflow", nil, time.Now())
	if err != nil {
		t.Fatal(err)
	}

	// Process the workflow task so we can complete the workflow
	tasks, err := store.WaitForWorkflowTasks(t.Context(), "worker-1", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 {
		t.Fatalf("expected 1 task, got %d", len(tasks))
	}

	var wg sync.WaitGroup
	results := make([]journal.Event, 2)
	errs := make([]error, 2)

	for i := 0; i < 2; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i], errs[i] = store.WaitForCompletion(t.Context(), "wf-1", runID)
		}()
	}

	key := store.key("wf-1", runID)
	for {
		store.mu.Lock()
		n := len(store.complete[key])
		store.mu.Unlock()
		if n >= 2 {
			break
		}
		runtime.Gosched()
	}

	// Complete the workflow
	_, err = store.Append(t.Context(), "wf-1", runID, []journal.Event{
		journal.WorkflowTaskCompleted{},
		journal.WorkflowCompleted{Result: []byte(`"done"`)},
	}, tasks[0].ScheduledAt)
	if err != nil {
		t.Fatal(err)
	}

	wg.Wait()

	for i := 0; i < 2; i++ {
		if errs[i] != nil {
			t.Errorf("waiter %d: unexpected error: %v", i, errs[i])
		}
		if results[i] == nil {
			t.Errorf("waiter %d: got nil event", i)
			continue
		}
		if results[i].EventType() != journal.TypeWorkflowCompleted {
			t.Errorf("waiter %d: expected WorkflowCompleted, got %s", i, results[i].EventType())
		}
	}
}

func TestMemoryStore_ParentCloseAfterChildContinueAsNew(t *testing.T) {
	idSeq := 0
	store := NewMemoryStore(WithIDGenerator(func() string {
		idSeq++
		switch idSeq {
		case 1:
			return "parent-run"
		case 2:
			return "child-run-1"
		case 3:
			return "child-run-2"
		default:
			t.Fatalf("unexpected ID generation call %d", idSeq)
			return ""
		}
	}))

	_, err := store.CreateWorkflow(t.Context(), "parent", "ParentWF", nil, time.Now())
	if err != nil {
		t.Fatal(err)
	}

	tasks, err := store.WaitForWorkflowTasks(t.Context(), "worker-1", 10)
	if err != nil {
		t.Fatal(err)
	}

	// Parent schedules child with Terminate policy (does not complete yet)
	_, err = store.Append(t.Context(), "parent", "parent-run", []journal.Event{
		journal.WorkflowTaskCompleted{},
		journal.ChildWorkflowScheduled{
			WorkflowID:        "child",
			WorkflowType:      "ChildWF",
			ParentClosePolicy: journal.ParentClosePolicyTerminate,
		},
	}, tasks[0].ScheduledAt)
	if err != nil {
		t.Fatal(err)
	}

	// Claim and process child task
	childTasks, err := store.WaitForWorkflowTasks(t.Context(), "worker-1", 10)
	if err != nil {
		t.Fatal(err)
	}
	var childTask journal.PendingWorkflowTask
	for _, ct := range childTasks {
		if ct.WorkflowID == "child" {
			childTask = ct
			break
		}
	}
	if childTask.WorkflowID == "" {
		t.Fatal("child task not found")
	}

	// Child does continue_as_new, creating child-run-2
	_, err = store.Append(t.Context(), "child", "child-run-1", []journal.Event{
		journal.WorkflowTaskCompleted{},
		journal.WorkflowContinuedAsNew{NewInput: []byte(`"new"`)},
	}, childTask.ScheduledAt)
	if err != nil {
		t.Fatal(err)
	}

	// Signal the parent to create a new wake-up task
	err = store.SignalWorkflow(t.Context(), "parent", "done-signal", nil)
	if err != nil {
		t.Fatal(err)
	}

	// Process parent's signal wake-up: complete the parent
	parentTasks, err := store.WaitForWorkflowTasks(t.Context(), "worker-1", 10)
	if err != nil {
		t.Fatal(err)
	}
	var parentTask journal.PendingWorkflowTask
	for _, pt := range parentTasks {
		if pt.WorkflowID == "parent" {
			parentTask = pt
			break
		}
	}
	if parentTask.WorkflowID == "" {
		t.Fatal("parent wake task not found")
	}

	_, err = store.Append(t.Context(), "parent", "parent-run", []journal.Event{
		journal.WorkflowTaskCompleted{},
		journal.WorkflowCompleted{Result: []byte(`"parent done"`)},
	}, parentTask.ScheduledAt)
	if err != nil {
		t.Fatal(err)
	}

	// The continued child run (child-run-2) should be terminated
	newChildKey := store.key("child", "child-run-2")
	store.mu.Lock()
	events := store.events[newChildKey]
	store.mu.Unlock()

	var terminated bool
	for _, ev := range events {
		if failed, ok := ev.(journal.WorkflowFailed); ok {
			if failed.Error.Kind == journal.ErrorKindCancelled {
				terminated = true
			}
		}
	}

	if !terminated {
		t.Error("expected continued child run to be terminated after parent completion")
	}
}
