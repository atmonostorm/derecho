package storetest

import (
	"context"
	"testing"
	"time"

	"github.com/atmonostorm/derecho/journal"
)

func testActivityTasks(t *testing.T, factory Factory) {
	t.Run("ScheduledActivityAvailable", testScheduledActivityAvailable(factory))
	t.Run("ActivityConsumedOnce", testActivityConsumedOnce(factory))
	t.Run("RequeueForRetry", testRequeueForRetry(factory))
	t.Run("RecordHeartbeat", testRecordHeartbeat(factory))
	t.Run("ConcurrentActivityScheduling", testConcurrentActivityScheduling(factory))
}

func testScheduledActivityAvailable(factory Factory) func(t *testing.T) {
	return func(t *testing.T) {
		store, _ := newTestStore(t, factory)
		ctx := context.Background()

		runID, err := store.CreateWorkflow(ctx, "wf-1", "TestWorkflow", nil, baseTime)
		if err != nil {
			t.Fatalf("CreateWorkflow failed: %v", err)
		}

		_, err = store.Append(ctx, "wf-1", runID, []journal.Event{
			journal.WorkflowTaskCompleted{},
			journal.ActivityScheduled{
				Name:  "TestActivity",
				Input: []byte(`"input"`),
			},
		}, 2)
		if err != nil {
			t.Fatalf("Append failed: %v", err)
		}

		ctx1, cancel1 := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel1()

		tasks, err := store.WaitForActivityTasks(ctx1, "worker-1", 100)
		if err != nil {
			t.Fatalf("WaitForActivityTasks failed: %v", err)
		}

		if len(tasks) != 1 {
			t.Fatalf("expected 1 activity task, got %d", len(tasks))
		}

		if tasks[0].WorkflowID != "wf-1" {
			t.Errorf("WorkflowID = %s, want wf-1", tasks[0].WorkflowID)
		}
		if tasks[0].ActivityName != "TestActivity" {
			t.Errorf("ActivityName = %s, want TestActivity", tasks[0].ActivityName)
		}
		if string(tasks[0].Input) != `"input"` {
			t.Errorf("Input = %s, want %q", tasks[0].Input, `"input"`)
		}
	}
}

func testActivityConsumedOnce(factory Factory) func(t *testing.T) {
	return func(t *testing.T) {
		store, _ := newTestStore(t, factory)
		ctx := context.Background()

		runID, err := store.CreateWorkflow(ctx, "wf-1", "TestWorkflow", nil, baseTime)
		if err != nil {
			t.Fatalf("CreateWorkflow failed: %v", err)
		}

		_, err = store.Append(ctx, "wf-1", runID, []journal.Event{
			journal.WorkflowTaskCompleted{},
			journal.ActivityScheduled{Name: "TestActivity"},
		}, 2)
		if err != nil {
			t.Fatalf("Append failed: %v", err)
		}

		ctx1, cancel1 := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel1()

		tasks, err := store.WaitForActivityTasks(ctx1, "worker-1", 100)
		if err != nil {
			t.Fatalf("first WaitForActivityTasks failed: %v", err)
		}
		if len(tasks) != 1 {
			t.Fatalf("expected 1 task on first call, got %d", len(tasks))
		}

		ctx2, cancel2 := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel2()

		tasks, err = store.WaitForActivityTasks(ctx2, "worker-2", 100)
		if err != context.DeadlineExceeded {
			t.Errorf("expected DeadlineExceeded, got err=%v tasks=%d", err, len(tasks))
		}
	}
}

func testRequeueForRetry(factory Factory) func(t *testing.T) {
	return func(t *testing.T) {
		store, clock := newTestStore(t, factory)
		ctx := context.Background()

		runID, err := store.CreateWorkflow(ctx, "wf-1", "TestWorkflow", nil, baseTime)
		if err != nil {
			t.Fatalf("CreateWorkflow failed: %v", err)
		}

		_, err = store.Append(ctx, "wf-1", runID, []journal.Event{
			journal.WorkflowTaskCompleted{},
			journal.ActivityScheduled{Name: "TestActivity"},
		}, 2)
		if err != nil {
			t.Fatalf("Append failed: %v", err)
		}

		ctx1, cancel1 := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel1()

		tasks, err := store.WaitForActivityTasks(ctx1, "worker-1", 100)
		if err != nil {
			t.Fatalf("WaitForActivityTasks failed: %v", err)
		}
		if len(tasks) != 1 {
			t.Fatalf("expected 1 task, got %d", len(tasks))
		}

		scheduledAt := tasks[0].ScheduledAt
		notBefore := clock.Now().Add(5 * time.Second)

		err = store.RequeueForRetry(ctx, "wf-1", runID, scheduledAt, journal.RequeueInfo{
			Attempt:     1,
			NotBefore:   notBefore,
			LastFailure: "test failure",
		})
		if err != nil {
			t.Fatalf("RequeueForRetry failed: %v", err)
		}

		ctx2, cancel2 := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel2()

		tasks, err = store.WaitForActivityTasks(ctx2, "worker-1", 100)
		if err != context.DeadlineExceeded {
			t.Errorf("expected DeadlineExceeded (task delayed), got err=%v tasks=%d", err, len(tasks))
		}

		clock.Advance(10 * time.Second)

		ctx3, cancel3 := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel3()

		tasks, err = store.WaitForActivityTasks(ctx3, "worker-1", 100)
		if err != nil {
			t.Fatalf("WaitForActivityTasks after advance failed: %v", err)
		}
		if len(tasks) != 1 {
			t.Fatalf("expected 1 requeued task, got %d", len(tasks))
		}
		if tasks[0].Attempt != 1 {
			t.Errorf("Attempt = %d, want 1", tasks[0].Attempt)
		}
		if tasks[0].LastFailure != "test failure" {
			t.Errorf("LastFailure = %s, want 'test failure'", tasks[0].LastFailure)
		}
	}
}

func testRecordHeartbeat(factory Factory) func(t *testing.T) {
	return func(t *testing.T) {
		store, _ := newTestStore(t, factory)
		ctx := context.Background()

		runID, err := store.CreateWorkflow(ctx, "wf-1", "TestWorkflow", nil, baseTime)
		if err != nil {
			t.Fatalf("CreateWorkflow failed: %v", err)
		}

		_, err = store.Append(ctx, "wf-1", runID, []journal.Event{
			journal.WorkflowTaskCompleted{},
			journal.ActivityScheduled{
				Name: "TestActivity",
				TimeoutPolicy: &journal.TimeoutPolicyPayload{
					HeartbeatTimeout: 30 * time.Second,
				},
			},
		}, 2)
		if err != nil {
			t.Fatalf("Append failed: %v", err)
		}

		ctx1, cancel1 := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel1()

		tasks, err := store.WaitForActivityTasks(ctx1, "worker-1", 100)
		if err != nil {
			t.Fatalf("WaitForActivityTasks failed: %v", err)
		}
		if len(tasks) != 1 {
			t.Fatalf("expected 1 task, got %d", len(tasks))
		}

		err = store.RecordHeartbeat(ctx, "wf-1", runID, tasks[0].ScheduledAt, []byte(`"progress"`))
		if err != nil {
			t.Fatalf("RecordHeartbeat failed: %v", err)
		}
	}
}

func testConcurrentActivityScheduling(factory Factory) func(t *testing.T) {
	return func(t *testing.T) {
		store, _ := newTestStore(t, factory)
		ctx := context.Background()

		runID, err := store.CreateWorkflow(ctx, "wf-1", "TestWorkflow", nil, baseTime)
		if err != nil {
			t.Fatalf("CreateWorkflow failed: %v", err)
		}

		ctx0, cancel0 := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel0()
		_, err = store.WaitForWorkflowTasks(ctx0, "worker-1", 10)
		if err != nil {
			t.Fatalf("WaitForWorkflowTasks failed: %v", err)
		}

		tasks := assertWakesOnWork(t,
			func(ctx context.Context) ([]journal.PendingActivityTask, error) {
				return store.WaitForActivityTasks(ctx, "worker-1", 100)
			},
			func() {
				if _, err := store.Append(ctx, "wf-1", runID, []journal.Event{
					journal.WorkflowTaskCompleted{},
					journal.ActivityScheduled{Name: "TestActivity"},
				}, 2); err != nil {
					t.Fatalf("Append failed: %v", err)
				}
			},
		)

		if len(tasks) != 1 {
			t.Errorf("expected 1 task, got %d", len(tasks))
		}
		if tasks[0].ActivityName != "TestActivity" {
			t.Errorf("ActivityName = %s, want TestActivity", tasks[0].ActivityName)
		}
	}
}
