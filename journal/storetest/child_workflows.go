package storetest

import (
	"context"
	"testing"

	"github.com/atmonostorm/derecho/journal"
)

func testChildWorkflows(t *testing.T, factory Factory) {
	t.Run("DuplicateChildWorkflowIDCreatesMultipleRuns", testDuplicateChildWorkflowID(factory))
	t.Run("MultipleChildWorkflowsInSingleAppend", func(t *testing.T) {
		store, _ := newTestStore(t, factory)
		ctx := context.Background()

		// Create parent workflow
		runID, err := store.CreateWorkflow(ctx, "parent-wf", "ParentWorkflow", nil, baseTime)
		if err != nil {
			t.Fatal(err)
		}

		// Simulate workflow task processing
		tasks, err := store.WaitForWorkflowTasks(ctx, "worker-1", 10)
		if err != nil {
			t.Fatal(err)
		}
		if len(tasks) != 1 {
			t.Fatalf("expected 1 task, got %d", len(tasks))
		}

		// Append multiple ChildWorkflowScheduled events at once.
		// Each ChildWorkflowScheduled triggers side effects that insert
		// ChildWorkflowStarted events. The store must handle this correctly
		// by refreshing lastEventID after each side effect.
		events := []journal.Event{
			journal.ChildWorkflowScheduled{
				WorkflowID:   "child-1",
				WorkflowType: "ChildWorkflow",
				Input:        []byte(`{}`),
			},
			journal.ChildWorkflowScheduled{
				WorkflowID:   "child-2",
				WorkflowType: "ChildWorkflow",
				Input:        []byte(`{}`),
			},
			journal.ChildWorkflowScheduled{
				WorkflowID:   "child-3",
				WorkflowType: "ChildWorkflow",
				Input:        []byte(`{}`),
			},
		}

		// This should succeed. Before the fix, this would fail with
		// UNIQUE constraint violation because Append didn't refresh
		// lastEventID after ChildWorkflowScheduled side effects.
		_, err = store.Append(ctx, "parent-wf", runID, events, 0)
		if err != nil {
			t.Fatalf("Append failed: %v", err)
		}

		// Verify all events were inserted correctly
		allEvents, err := store.Load(ctx, "parent-wf", runID)
		if err != nil {
			t.Fatal(err)
		}

		// Expected events:
		// 1: WorkflowStarted
		// 2: WorkflowTaskScheduled
		// 3: WorkflowTaskStarted (from task claim)
		// 4: ChildWorkflowScheduled (child-1)
		// 5: ChildWorkflowStarted (child-1) - from side effect
		// 6: ChildWorkflowScheduled (child-2)
		// 7: ChildWorkflowStarted (child-2) - from side effect
		// 8: ChildWorkflowScheduled (child-3)
		// 9: ChildWorkflowStarted (child-3) - from side effect
		expectedCount := 9
		if len(allEvents) != expectedCount {
			t.Errorf("expected %d events, got %d", expectedCount, len(allEvents))
			for i, ev := range allEvents {
				t.Logf("  %d: %s (id=%d)", i, ev.EventType(), ev.Base().ID)
			}
		}

		// Extract child runIDs from ChildWorkflowStarted events in parent
		childRunIDs := make(map[string]string)
		for _, ev := range allEvents {
			if started, ok := ev.(journal.ChildWorkflowStarted); ok {
				// Find the corresponding ChildWorkflowScheduled to get the workflow ID
				for _, ev2 := range allEvents {
					if sched, ok := ev2.(journal.ChildWorkflowScheduled); ok && ev2.Base().ID == started.Base().ScheduledByID {
						childRunIDs[sched.WorkflowID] = started.ChildRunID
						break
					}
				}
			}
		}

		// Verify all children have runIDs
		for _, childID := range []string{"child-1", "child-2", "child-3"} {
			childRunID, ok := childRunIDs[childID]
			if !ok {
				t.Errorf("no ChildWorkflowStarted event found for %s", childID)
				continue
			}

			childEvents, err := store.Load(ctx, childID, childRunID)
			if err != nil {
				t.Errorf("failed to load child %s: %v", childID, err)
				continue
			}
			// Child workflow should have WorkflowStarted and WorkflowTaskScheduled
			if len(childEvents) < 2 {
				t.Errorf("child %s has %d events, expected at least 2", childID, len(childEvents))
			}
		}
	})
}

// testDuplicateChildWorkflowID verifies idempotency: when two different parents
// schedule children with the same WorkflowID, only one child is created.
// Without this, duplicate activity executions occur in production.
func testDuplicateChildWorkflowID(factory Factory) func(t *testing.T) {
	return func(t *testing.T) {
		store, _ := newTestStore(t, factory)
		ctx := context.Background()

		parentRunID1, err := store.CreateWorkflow(ctx, "parent-wf-1", "ParentWorkflow", nil, baseTime)
		if err != nil {
			t.Fatal(err)
		}

		parentRunID2, err := store.CreateWorkflow(ctx, "parent-wf-2", "ParentWorkflow", nil, baseTime)
		if err != nil {
			t.Fatal(err)
		}

		tasks, err := store.WaitForWorkflowTasks(ctx, "worker-1", 10)
		if err != nil {
			t.Fatal(err)
		}
		if len(tasks) < 2 {
			t.Fatalf("expected at least 2 tasks for both runs, got %d", len(tasks))
		}

		childScheduled := journal.ChildWorkflowScheduled{
			WorkflowID:   "child-deterministic-id",
			WorkflowType: "ChildWorkflow",
			Input:        []byte(`{}`),
		}

		_, err = store.Append(ctx, "parent-wf-1", parentRunID1, []journal.Event{childScheduled}, 0)
		if err != nil {
			t.Fatalf("parent 1 append failed: %v", err)
		}

		_, err = store.Append(ctx, "parent-wf-2", parentRunID2, []journal.Event{childScheduled}, 0)
		if err != nil {
			t.Fatalf("parent 2 append failed: %v", err)
		}

		var childRunIDs []string

		events1, _ := store.Load(ctx, "parent-wf-1", parentRunID1)
		for _, ev := range events1 {
			if started, ok := ev.(journal.ChildWorkflowStarted); ok {
				childRunIDs = append(childRunIDs, started.ChildRunID)
			}
		}

		events2, _ := store.Load(ctx, "parent-wf-2", parentRunID2)
		for _, ev := range events2 {
			if started, ok := ev.(journal.ChildWorkflowStarted); ok {
				childRunIDs = append(childRunIDs, started.ChildRunID)
			}
		}

		if len(childRunIDs) != 2 {
			t.Fatalf("expected 2 ChildWorkflowStarted events (one per parent), got %d", len(childRunIDs))
		}

		if childRunIDs[0] != childRunIDs[1] {
			t.Fatalf("idempotency failure: expected same child run ID from both parents, got %s and %s", childRunIDs[0], childRunIDs[1])
		}

		childEvents, err := store.Load(ctx, "child-deterministic-id", childRunIDs[0])
		if err != nil {
			t.Fatalf("failed to load child: %v", err)
		}

		if len(childEvents) < 2 {
			t.Fatalf("expected child to have at least 2 events, got %d", len(childEvents))
		}
	}
}
