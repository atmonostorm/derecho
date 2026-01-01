package storetest

import (
	"context"
	"testing"
	"time"

	"github.com/atmonostorm/derecho/journal"
)

func testTimeouts(t *testing.T, factory Factory) {
	t.Run("ScheduleToStartTimeout", testScheduleToStartTimeout(factory))
	t.Run("ScheduleToCloseTimeout", testScheduleToCloseTimeout(factory))
	t.Run("NoTimeoutBeforeDeadline", testNoTimeoutBeforeDeadline(factory))
}

func testScheduleToStartTimeout(factory Factory) func(t *testing.T) {
	return func(t *testing.T) {
		store, clock := newTestStore(t, factory)
		ctx := context.Background()

		runID, err := store.CreateWorkflow(ctx, "wf-1", "TestWorkflow", nil, baseTime)
		if err != nil {
			t.Fatalf("CreateWorkflow failed: %v", err)
		}

		_, err = store.Append(ctx, "wf-1", runID, []journal.Event{
			journal.WorkflowTaskCompleted{},
			journal.ActivityScheduled{
				Name:        "TestActivity",
				ScheduledAt: clock.Now(),
				TimeoutPolicy: &journal.TimeoutPolicyPayload{
					ScheduleToStartTimeout: 30 * time.Second,
				},
			},
		}, 2)
		if err != nil {
			t.Fatalf("Append failed: %v", err)
		}

		timedOut, err := store.GetTimedOutActivities(ctx, clock.Now())
		if err != nil {
			t.Fatalf("GetTimedOutActivities failed: %v", err)
		}
		if len(timedOut) != 0 {
			t.Errorf("expected 0 timeouts before deadline, got %d", len(timedOut))
		}

		clock.Advance(1 * time.Minute)

		timedOut, err = store.GetTimedOutActivities(ctx, clock.Now())
		if err != nil {
			t.Fatalf("GetTimedOutActivities failed: %v", err)
		}
		if len(timedOut) != 1 {
			t.Fatalf("expected 1 timeout, got %d", len(timedOut))
		}

		if timedOut[0].TimeoutKind != journal.TimeoutKindScheduleToStart {
			t.Errorf("TimeoutKind = %s, want schedule_to_start", timedOut[0].TimeoutKind)
		}
	}
}

func testScheduleToCloseTimeout(factory Factory) func(t *testing.T) {
	return func(t *testing.T) {
		store, clock := newTestStore(t, factory)
		ctx := context.Background()

		runID, err := store.CreateWorkflow(ctx, "wf-1", "TestWorkflow", nil, baseTime)
		if err != nil {
			t.Fatalf("CreateWorkflow failed: %v", err)
		}

		_, err = store.Append(ctx, "wf-1", runID, []journal.Event{
			journal.WorkflowTaskCompleted{},
			journal.ActivityScheduled{
				Name:        "TestActivity",
				ScheduledAt: clock.Now(),
				TimeoutPolicy: &journal.TimeoutPolicyPayload{
					ScheduleToCloseTimeout: 1 * time.Minute,
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

		clock.Advance(2 * time.Minute)

		timedOut, err := store.GetTimedOutActivities(ctx, clock.Now())
		if err != nil {
			t.Fatalf("GetTimedOutActivities failed: %v", err)
		}
		if len(timedOut) != 1 {
			t.Fatalf("expected 1 timeout, got %d", len(timedOut))
		}

		if timedOut[0].TimeoutKind != journal.TimeoutKindScheduleToClose {
			t.Errorf("TimeoutKind = %s, want schedule_to_close", timedOut[0].TimeoutKind)
		}
	}
}

func testNoTimeoutBeforeDeadline(factory Factory) func(t *testing.T) {
	return func(t *testing.T) {
		store, clock := newTestStore(t, factory)
		ctx := context.Background()

		runID, err := store.CreateWorkflow(ctx, "wf-1", "TestWorkflow", nil, baseTime)
		if err != nil {
			t.Fatalf("CreateWorkflow failed: %v", err)
		}

		_, err = store.Append(ctx, "wf-1", runID, []journal.Event{
			journal.WorkflowTaskCompleted{},
			journal.ActivityScheduled{
				Name:        "TestActivity",
				ScheduledAt: clock.Now(),
				TimeoutPolicy: &journal.TimeoutPolicyPayload{
					ScheduleToStartTimeout: 1 * time.Hour,
					ScheduleToCloseTimeout: 2 * time.Hour,
				},
			},
		}, 2)
		if err != nil {
			t.Fatalf("Append failed: %v", err)
		}

		clock.Advance(30 * time.Minute)

		timedOut, err := store.GetTimedOutActivities(ctx, clock.Now())
		if err != nil {
			t.Fatalf("GetTimedOutActivities failed: %v", err)
		}
		if len(timedOut) != 0 {
			t.Errorf("expected 0 timeouts, got %d", len(timedOut))
		}
	}
}
