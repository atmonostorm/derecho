package storetest

import (
	"context"
	"testing"

	"github.com/atmonostorm/derecho/journal"
)

func testPersistence(t *testing.T, factory Factory) {
	t.Run("SequentialEventIDs", testSequentialEventIDs(factory))
	t.Run("LoadFromAfterID", testLoadFromAfterID(factory))
	t.Run("Deduplication", testDeduplication(factory))
	t.Run("LoadEmptyWorkflow", testLoadEmptyWorkflow(factory))
}

func testSequentialEventIDs(factory Factory) func(t *testing.T) {
	return func(t *testing.T) {
		store, _ := newTestStore(t, factory)
		ctx := context.Background()

		runID, err := store.CreateWorkflow(ctx, "wf-1", "TestWorkflow", nil, baseTime)
		if err != nil {
			t.Fatalf("CreateWorkflow failed: %v", err)
		}

		events, err := store.Load(ctx, "wf-1", runID)
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}

		if len(events) != 2 {
			t.Fatalf("expected 2 initial events, got %d", len(events))
		}

		if events[0].Base().ID != 1 {
			t.Errorf("first event ID = %d, want 1", events[0].Base().ID)
		}
		if events[1].Base().ID != 2 {
			t.Errorf("second event ID = %d, want 2", events[1].Base().ID)
		}

		ids, err := store.Append(ctx, "wf-1", runID, []journal.Event{
			journal.WorkflowTaskCompleted{},
			journal.WorkflowTaskScheduled{},
		}, 2)
		if err != nil {
			t.Fatalf("Append failed: %v", err)
		}

		if len(ids) != 2 {
			t.Fatalf("expected 2 event IDs, got %d", len(ids))
		}
		if ids[0] != 3 {
			t.Errorf("third event ID = %d, want 3", ids[0])
		}
		if ids[1] != 4 {
			t.Errorf("fourth event ID = %d, want 4", ids[1])
		}
	}
}

func testLoadFromAfterID(factory Factory) func(t *testing.T) {
	return func(t *testing.T) {
		store, _ := newTestStore(t, factory)
		ctx := context.Background()

		runID, err := store.CreateWorkflow(ctx, "wf-1", "TestWorkflow", nil, baseTime)
		if err != nil {
			t.Fatalf("CreateWorkflow failed: %v", err)
		}

		_, err = store.Append(ctx, "wf-1", runID, []journal.Event{
			journal.WorkflowTaskCompleted{},
			journal.ActivityScheduled{Name: "test"},
		}, 2)
		if err != nil {
			t.Fatalf("Append failed: %v", err)
		}

		events, err := store.LoadFrom(ctx, "wf-1", runID, 2)
		if err != nil {
			t.Fatalf("LoadFrom failed: %v", err)
		}

		if len(events) != 2 {
			t.Fatalf("expected 2 events after ID 2, got %d", len(events))
		}

		if events[0].Base().ID != 3 {
			t.Errorf("first returned event ID = %d, want 3", events[0].Base().ID)
		}
		if events[1].Base().ID != 4 {
			t.Errorf("second returned event ID = %d, want 4", events[1].Base().ID)
		}
	}
}

func testDeduplication(factory Factory) func(t *testing.T) {
	return func(t *testing.T) {
		store, _ := newTestStore(t, factory)
		ctx := context.Background()

		runID, err := store.CreateWorkflow(ctx, "wf-1", "TestWorkflow", nil, baseTime)
		if err != nil {
			t.Fatalf("CreateWorkflow failed: %v", err)
		}

		_, err = store.Append(ctx, "wf-1", runID, []journal.Event{
			journal.WorkflowTaskCompleted{},
		}, 2)
		if err != nil {
			t.Fatalf("first Append failed: %v", err)
		}

		_, err = store.Append(ctx, "wf-1", runID, []journal.Event{
			journal.WorkflowTaskCompleted{},
		}, 2)
		if err != journal.ErrAlreadyProcessed {
			t.Errorf("second Append error = %v, want ErrAlreadyProcessed", err)
		}
	}
}

func testLoadEmptyWorkflow(factory Factory) func(t *testing.T) {
	return func(t *testing.T) {
		store, _ := newTestStore(t, factory)
		ctx := context.Background()

		events, err := store.Load(ctx, "nonexistent", "run-1")
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}

		if len(events) != 0 {
			t.Errorf("expected 0 events for nonexistent workflow, got %d", len(events))
		}
	}
}
