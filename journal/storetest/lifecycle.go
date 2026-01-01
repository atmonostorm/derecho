package storetest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/atmonostorm/derecho/journal"
)

func testLifecycle(t *testing.T, factory Factory) {
	t.Run("CreateWorkflowEvents", testCreateWorkflowEvents(factory))
	t.Run("InitialStatus", testInitialStatus(factory))
	t.Run("StatusAfterCompletion", testStatusAfterCompletion(factory))
	t.Run("StatusAfterFailure", testStatusAfterFailure(factory))
	t.Run("WaitForCompletionImmediate", testWaitForCompletionImmediate(factory))
	t.Run("CreateWorkflowDuplicateError", testCreateWorkflowDuplicateError(factory))
	t.Run("CreateWorkflowAllowsAfterCompletion", testCreateWorkflowAllowsAfterCompletion(factory))
}

func testCreateWorkflowEvents(factory Factory) func(t *testing.T) {
	return func(t *testing.T) {
		store, _ := newTestStore(t, factory)
		ctx := context.Background()

		runID, err := store.CreateWorkflow(ctx, "wf-1", "TestWorkflow", []byte(`"input"`), baseTime)
		if err != nil {
			t.Fatalf("CreateWorkflow failed: %v", err)
		}

		events, err := store.Load(ctx, "wf-1", runID)
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}

		if len(events) != 2 {
			t.Fatalf("expected 2 events, got %d", len(events))
		}

		if events[0].EventType() != journal.TypeWorkflowStarted {
			t.Errorf("first event type = %s, want WorkflowStarted", events[0].EventType())
		}
		if events[1].EventType() != journal.TypeWorkflowTaskScheduled {
			t.Errorf("second event type = %s, want WorkflowTaskScheduled", events[1].EventType())
		}

		started, ok := events[0].(journal.WorkflowStarted)
		if !ok {
			t.Fatalf("first event not WorkflowStarted")
		}
		if started.WorkflowType != "TestWorkflow" {
			t.Errorf("WorkflowType = %s, want TestWorkflow", started.WorkflowType)
		}
		if string(started.Args) != `"input"` {
			t.Errorf("Args = %s, want %q", started.Args, `"input"`)
		}
	}
}

func testInitialStatus(factory Factory) func(t *testing.T) {
	return func(t *testing.T) {
		store, _ := newTestStore(t, factory)
		ctx := context.Background()

		runID, err := store.CreateWorkflow(ctx, "wf-1", "TestWorkflow", nil, baseTime)
		if err != nil {
			t.Fatalf("CreateWorkflow failed: %v", err)
		}

		status, err := store.GetStatus(ctx, "wf-1", runID)
		if err != nil {
			t.Fatalf("GetStatus failed: %v", err)
		}

		if status != journal.WorkflowStatusRunning {
			t.Errorf("status = %v, want Running", status)
		}
	}
}

func testStatusAfterCompletion(factory Factory) func(t *testing.T) {
	return func(t *testing.T) {
		store, _ := newTestStore(t, factory)
		ctx := context.Background()

		runID, err := store.CreateWorkflow(ctx, "wf-1", "TestWorkflow", nil, baseTime)
		if err != nil {
			t.Fatalf("CreateWorkflow failed: %v", err)
		}

		_, err = store.Append(ctx, "wf-1", runID, []journal.Event{
			journal.WorkflowTaskCompleted{},
			journal.WorkflowCompleted{Result: []byte(`"done"`)},
		}, 2)
		if err != nil {
			t.Fatalf("Append failed: %v", err)
		}

		status, err := store.GetStatus(ctx, "wf-1", runID)
		if err != nil {
			t.Fatalf("GetStatus failed: %v", err)
		}

		if status != journal.WorkflowStatusCompleted {
			t.Errorf("status = %v, want Completed", status)
		}
	}
}

func testStatusAfterFailure(factory Factory) func(t *testing.T) {
	return func(t *testing.T) {
		store, _ := newTestStore(t, factory)
		ctx := context.Background()

		runID, err := store.CreateWorkflow(ctx, "wf-1", "TestWorkflow", nil, baseTime)
		if err != nil {
			t.Fatalf("CreateWorkflow failed: %v", err)
		}

		_, err = store.Append(ctx, "wf-1", runID, []journal.Event{
			journal.WorkflowTaskCompleted{},
			journal.WorkflowFailed{Error: &journal.Error{Message: "test error"}},
		}, 2)
		if err != nil {
			t.Fatalf("Append failed: %v", err)
		}

		status, err := store.GetStatus(ctx, "wf-1", runID)
		if err != nil {
			t.Fatalf("GetStatus failed: %v", err)
		}

		if status != journal.WorkflowStatusFailed {
			t.Errorf("status = %v, want Failed", status)
		}
	}
}

func testWaitForCompletionImmediate(factory Factory) func(t *testing.T) {
	return func(t *testing.T) {
		store, _ := newTestStore(t, factory)
		ctx := context.Background()

		runID, err := store.CreateWorkflow(ctx, "wf-1", "TestWorkflow", nil, baseTime)
		if err != nil {
			t.Fatalf("CreateWorkflow failed: %v", err)
		}

		_, err = store.Append(ctx, "wf-1", runID, []journal.Event{
			journal.WorkflowTaskCompleted{},
			journal.WorkflowCompleted{Result: []byte(`"result"`)},
		}, 2)
		if err != nil {
			t.Fatalf("Append failed: %v", err)
		}

		ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()

		event, err := store.WaitForCompletion(ctx, "wf-1", runID)
		if err != nil {
			t.Fatalf("WaitForCompletion failed: %v", err)
		}

		completed, ok := event.(journal.WorkflowCompleted)
		if !ok {
			t.Fatalf("event type = %T, want WorkflowCompleted", event)
		}
		if string(completed.Result) != `"result"` {
			t.Errorf("Result = %s, want %q", completed.Result, `"result"`)
		}
	}
}

func testCreateWorkflowDuplicateError(factory Factory) func(t *testing.T) {
	return func(t *testing.T) {
		store, _ := newTestStore(t, factory)
		ctx := context.Background()

		_, err := store.CreateWorkflow(ctx, "wf-dup", "TestWorkflow", nil, baseTime)
		if err != nil {
			t.Fatalf("first CreateWorkflow failed: %v", err)
		}

		_, err = store.CreateWorkflow(ctx, "wf-dup", "TestWorkflow", nil, baseTime)
		if !errors.Is(err, journal.ErrWorkflowAlreadyRunning) {
			t.Errorf("second CreateWorkflow error = %v, want ErrWorkflowAlreadyRunning", err)
		}
	}
}

func testCreateWorkflowAllowsAfterCompletion(factory Factory) func(t *testing.T) {
	return func(t *testing.T) {
		store, _ := newTestStore(t, factory)
		ctx := context.Background()

		runID1, err := store.CreateWorkflow(ctx, "wf-reuse", "TestWorkflow", nil, baseTime)
		if err != nil {
			t.Fatalf("first CreateWorkflow failed: %v", err)
		}

		_, err = store.Append(ctx, "wf-reuse", runID1, []journal.Event{
			journal.WorkflowTaskCompleted{},
			journal.WorkflowCompleted{Result: []byte(`"done"`)},
		}, 2)
		if err != nil {
			t.Fatalf("Append failed: %v", err)
		}

		runID2, err := store.CreateWorkflow(ctx, "wf-reuse", "TestWorkflow", nil, baseTime)
		if err != nil {
			t.Fatalf("second CreateWorkflow after completion failed: %v", err)
		}

		if runID1 == runID2 {
			t.Errorf("second run got same runID as first: %s", runID1)
		}
	}
}
