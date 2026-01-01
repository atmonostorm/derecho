package storetest

import (
	"context"
	"testing"
	"time"

	"github.com/atmonostorm/derecho/journal"
)

func testTimers(t *testing.T, factory Factory) {
	t.Run("TimerFires", testTimerFires(factory))
	t.Run("TimerNotReady", testTimerNotReady(factory))
	t.Run("MultipleTimers", testMultipleTimers(factory))
}

func testTimerFires(factory Factory) func(t *testing.T) {
	return func(t *testing.T) {
		store, clock := newTestStore(t, factory)
		ctx := context.Background()

		runID, err := store.CreateWorkflow(ctx, "wf-1", "TestWorkflow", nil, baseTime)
		if err != nil {
			t.Fatalf("CreateWorkflow failed: %v", err)
		}

		fireAt := clock.Now().Add(5 * time.Second)

		_, err = store.Append(ctx, "wf-1", runID, []journal.Event{
			journal.WorkflowTaskCompleted{},
			journal.TimerScheduled{FireAt: fireAt},
		}, 2)
		if err != nil {
			t.Fatalf("Append failed: %v", err)
		}

		timers, err := store.GetTimersToFire(ctx, clock.Now())
		if err != nil {
			t.Fatalf("GetTimersToFire failed: %v", err)
		}
		if len(timers) != 0 {
			t.Errorf("expected 0 timers before fire time, got %d", len(timers))
		}

		clock.Advance(10 * time.Second)

		timers, err = store.GetTimersToFire(ctx, clock.Now())
		if err != nil {
			t.Fatalf("GetTimersToFire failed: %v", err)
		}
		if len(timers) != 1 {
			t.Fatalf("expected 1 timer after fire time, got %d", len(timers))
		}

		if timers[0].WorkflowID != "wf-1" {
			t.Errorf("WorkflowID = %s, want wf-1", timers[0].WorkflowID)
		}
		if timers[0].ScheduledAt != 4 {
			t.Errorf("ScheduledAt = %d, want 4", timers[0].ScheduledAt)
		}
	}
}

func testTimerNotReady(factory Factory) func(t *testing.T) {
	return func(t *testing.T) {
		store, clock := newTestStore(t, factory)
		ctx := context.Background()

		runID, err := store.CreateWorkflow(ctx, "wf-1", "TestWorkflow", nil, baseTime)
		if err != nil {
			t.Fatalf("CreateWorkflow failed: %v", err)
		}

		fireAt := clock.Now().Add(1 * time.Hour)

		_, err = store.Append(ctx, "wf-1", runID, []journal.Event{
			journal.WorkflowTaskCompleted{},
			journal.TimerScheduled{FireAt: fireAt},
		}, 2)
		if err != nil {
			t.Fatalf("Append failed: %v", err)
		}

		timers, err := store.GetTimersToFire(ctx, clock.Now())
		if err != nil {
			t.Fatalf("GetTimersToFire failed: %v", err)
		}
		if len(timers) != 0 {
			t.Errorf("expected 0 timers, got %d", len(timers))
		}
	}
}

func testMultipleTimers(factory Factory) func(t *testing.T) {
	return func(t *testing.T) {
		store, clock := newTestStore(t, factory)
		ctx := context.Background()

		runID, err := store.CreateWorkflow(ctx, "wf-1", "TestWorkflow", nil, baseTime)
		if err != nil {
			t.Fatalf("CreateWorkflow failed: %v", err)
		}

		_, err = store.Append(ctx, "wf-1", runID, []journal.Event{
			journal.WorkflowTaskCompleted{},
			journal.TimerScheduled{FireAt: clock.Now().Add(5 * time.Second)},
			journal.TimerScheduled{FireAt: clock.Now().Add(10 * time.Second)},
			journal.TimerScheduled{FireAt: clock.Now().Add(1 * time.Hour)},
		}, 2)
		if err != nil {
			t.Fatalf("Append failed: %v", err)
		}

		clock.Advance(15 * time.Second)

		timers, err := store.GetTimersToFire(ctx, clock.Now())
		if err != nil {
			t.Fatalf("GetTimersToFire failed: %v", err)
		}
		if len(timers) != 2 {
			t.Errorf("expected 2 timers after 15s, got %d", len(timers))
		}

		timers, err = store.GetTimersToFire(ctx, clock.Now())
		if err != nil {
			t.Fatalf("second GetTimersToFire failed: %v", err)
		}
		if len(timers) != 0 {
			t.Errorf("expected 0 timers after consumption, got %d", len(timers))
		}
	}
}
