package storetest

import (
	"context"
	"testing"
	"time"

	"github.com/atmonostorm/derecho/journal"
)

func testWorkflowTasks(t *testing.T, factory Factory) {
	t.Run("InitialTaskAvailable", testInitialTaskAvailable(factory))
	t.Run("TaskConsumedOnce", testTaskConsumedOnce(factory))
	t.Run("MultipleWorkflows", testMultipleWorkflows(factory))
	t.Run("RescheduledTask", testRescheduledTask(factory))
	t.Run("TaskReclaimedAfterTimeout", testTaskReclaimedAfterTimeout(factory))
	t.Run("ConcurrentWorkflowCreation", testConcurrentWorkflowCreation(factory))
}

func testInitialTaskAvailable(factory Factory) func(t *testing.T) {
	return func(t *testing.T) {
		store, _ := newTestStore(t, factory)
		ctx := context.Background()

		runID, err := store.CreateWorkflow(ctx, "wf-1", "TestWorkflow", nil, baseTime)
		if err != nil {
			t.Fatalf("CreateWorkflow failed: %v", err)
		}

		ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()

		tasks, err := store.WaitForWorkflowTasks(ctx, "worker-1", 10)
		if err != nil {
			t.Fatalf("WaitForWorkflowTasks failed: %v", err)
		}

		if len(tasks) != 1 {
			t.Fatalf("expected 1 task, got %d", len(tasks))
		}

		if tasks[0].WorkflowID != "wf-1" {
			t.Errorf("WorkflowID = %s, want wf-1", tasks[0].WorkflowID)
		}
		if tasks[0].RunID != runID {
			t.Errorf("RunID = %s, want %s", tasks[0].RunID, runID)
		}
		if tasks[0].ScheduledAt != 2 {
			t.Errorf("ScheduledAt = %d, want 2", tasks[0].ScheduledAt)
		}
	}
}

func testTaskConsumedOnce(factory Factory) func(t *testing.T) {
	return func(t *testing.T) {
		store, _ := newTestStore(t, factory)
		ctx := context.Background()

		_, err := store.CreateWorkflow(ctx, "wf-1", "TestWorkflow", nil, baseTime)
		if err != nil {
			t.Fatalf("CreateWorkflow failed: %v", err)
		}

		ctx1, cancel1 := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel1()

		tasks, err := store.WaitForWorkflowTasks(ctx1, "worker-1", 10)
		if err != nil {
			t.Fatalf("first WaitForWorkflowTasks failed: %v", err)
		}
		if len(tasks) != 1 {
			t.Fatalf("expected 1 task on first call, got %d", len(tasks))
		}

		ctx2, cancel2 := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel2()

		tasks, err = store.WaitForWorkflowTasks(ctx2, "worker-2", 10)
		if err != context.DeadlineExceeded {
			t.Errorf("expected DeadlineExceeded, got err=%v tasks=%d", err, len(tasks))
		}
	}
}

func testMultipleWorkflows(factory Factory) func(t *testing.T) {
	return func(t *testing.T) {
		store, _ := newTestStore(t, factory)
		ctx := context.Background()

		_, err := store.CreateWorkflow(ctx, "wf-1", "TestWorkflow", nil, baseTime)
		if err != nil {
			t.Fatalf("CreateWorkflow wf-1 failed: %v", err)
		}

		_, err = store.CreateWorkflow(ctx, "wf-2", "TestWorkflow", nil, baseTime)
		if err != nil {
			t.Fatalf("CreateWorkflow wf-2 failed: %v", err)
		}

		ctx1, cancel1 := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel1()

		tasks, err := store.WaitForWorkflowTasks(ctx1, "worker-1", 10)
		if err != nil {
			t.Fatalf("WaitForWorkflowTasks failed: %v", err)
		}

		if len(tasks) != 2 {
			t.Errorf("expected 2 tasks, got %d", len(tasks))
		}

		seen := make(map[string]bool)
		for _, task := range tasks {
			seen[task.WorkflowID] = true
		}
		if !seen["wf-1"] || !seen["wf-2"] {
			t.Errorf("expected tasks for wf-1 and wf-2, got %v", seen)
		}
	}
}

func testRescheduledTask(factory Factory) func(t *testing.T) {
	return func(t *testing.T) {
		store, _ := newTestStore(t, factory)
		ctx := context.Background()

		runID, err := store.CreateWorkflow(ctx, "wf-1", "TestWorkflow", nil, baseTime)
		if err != nil {
			t.Fatalf("CreateWorkflow failed: %v", err)
		}

		ctx1, cancel1 := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel1()

		tasks, err := store.WaitForWorkflowTasks(ctx1, "worker-1", 10)
		if err != nil {
			t.Fatalf("first WaitForWorkflowTasks failed: %v", err)
		}
		if len(tasks) != 1 {
			t.Fatalf("expected 1 task, got %d", len(tasks))
		}

		_, err = store.Append(ctx, "wf-1", runID, []journal.Event{
			journal.WorkflowTaskCompleted{},
			journal.ActivityScheduled{Name: "test"},
		}, tasks[0].ScheduledAt)
		if err != nil {
			t.Fatalf("Append failed: %v", err)
		}

		_, err = store.Append(ctx, "wf-1", runID, []journal.Event{
			journal.ActivityCompleted{Result: []byte(`"ok"`)},
			journal.WorkflowTaskScheduled{},
		}, 3)
		if err != nil {
			t.Fatalf("second Append failed: %v", err)
		}

		ctx2, cancel2 := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel2()

		tasks, err = store.WaitForWorkflowTasks(ctx2, "worker-1", 10)
		if err != nil {
			t.Fatalf("second WaitForWorkflowTasks failed: %v", err)
		}
		if len(tasks) != 1 {
			t.Fatalf("expected 1 rescheduled task, got %d", len(tasks))
		}
		if tasks[0].ScheduledAt != 7 {
			t.Errorf("ScheduledAt = %d, want 7", tasks[0].ScheduledAt)
		}
	}
}

func testTaskReclaimedAfterTimeout(factory Factory) func(t *testing.T) {
	return func(t *testing.T) {
		store, clock := newTestStore(t, factory)
		ctx := context.Background()

		runID, err := store.CreateWorkflow(ctx, "wf-1", "TestWorkflow", nil, baseTime)
		if err != nil {
			t.Fatalf("CreateWorkflow failed: %v", err)
		}

		ctx1, cancel1 := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel1()

		firstTasks, err := store.WaitForWorkflowTasks(ctx1, "worker-1", 10)
		if err != nil {
			t.Fatalf("first WaitForWorkflowTasks failed: %v", err)
		}
		if len(firstTasks) != 1 {
			t.Fatalf("expected 1 task on first claim, got %d", len(firstTasks))
		}
		firstStartedAt := firstTasks[0].StartedAt
		if firstStartedAt.IsZero() {
			t.Fatal("expected StartedAt on first claim")
		}

		timeout := 10 * time.Second
		clock.Advance(timeout + time.Second)

		released, err := store.ReleaseExpiredWorkflowTasks(ctx, clock.Now(), timeout)
		if err != nil {
			t.Fatalf("ReleaseExpiredWorkflowTasks failed: %v", err)
		}
		if released != 1 {
			t.Fatalf("expected 1 released task, got %d", released)
		}

		ctx2, cancel2 := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel2()

		secondTasks, err := store.WaitForWorkflowTasks(ctx2, "worker-2", 10)
		if err != nil {
			t.Fatalf("second WaitForWorkflowTasks failed: %v", err)
		}
		if len(secondTasks) != 1 {
			t.Fatalf("expected 1 task on reclaim, got %d", len(secondTasks))
		}
		if !secondTasks[0].StartedAt.Equal(firstStartedAt) {
			t.Fatalf("StartedAt changed on reclaim: got %v, want %v", secondTasks[0].StartedAt, firstStartedAt)
		}

		events, err := store.Load(ctx, "wf-1", runID)
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}
		startedCount := 0
		for _, ev := range events {
			if ev.EventType() == journal.TypeWorkflowTaskStarted {
				startedCount++
			}
		}
		if startedCount != 1 {
			t.Fatalf("expected 1 WorkflowTaskStarted event, got %d", startedCount)
		}
	}
}

func testConcurrentWorkflowCreation(factory Factory) func(t *testing.T) {
	return func(t *testing.T) {
		store, _ := newTestStore(t, factory)
		ctx := context.Background()

		tasks := assertWakesOnWork(t,
			func(ctx context.Context) ([]journal.PendingWorkflowTask, error) {
				return store.WaitForWorkflowTasks(ctx, "worker-1", 10)
			},
			func() {
				if _, err := store.CreateWorkflow(ctx, "wf-1", "TestWorkflow", nil, baseTime); err != nil {
					t.Fatalf("CreateWorkflow failed: %v", err)
				}
			},
		)

		if len(tasks) != 1 {
			t.Errorf("expected 1 task, got %d", len(tasks))
		}
		if tasks[0].WorkflowID != "wf-1" {
			t.Errorf("WorkflowID = %s, want wf-1", tasks[0].WorkflowID)
		}
	}
}
