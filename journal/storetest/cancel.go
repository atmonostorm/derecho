package storetest

import (
	"context"
	"testing"
	"time"

	"github.com/atmonostorm/derecho/journal"
)

func testCancel(t *testing.T, factory Factory) {
	t.Run("CancelCreatesEvent", testCancelCreatesEvent(factory))
	t.Run("CancelSchedulesTask", testCancelSchedulesTask(factory))
	t.Run("CancelSetsStatus", testCancelSetsStatus(factory))
}

func testCancelCreatesEvent(factory Factory) func(t *testing.T) {
	return func(t *testing.T) {
		store, _ := newTestStore(t, factory)
		ctx := context.Background()

		runID, err := store.CreateWorkflow(ctx, "wf-1", "TestWorkflow", nil, baseTime)
		if err != nil {
			t.Fatalf("CreateWorkflow failed: %v", err)
		}

		err = store.CancelWorkflow(ctx, "wf-1", "test reason")
		if err != nil {
			t.Fatalf("CancelWorkflow failed: %v", err)
		}

		events, err := store.Load(ctx, "wf-1", runID)
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}

		var found bool
		for _, e := range events {
			if cancel, ok := e.(journal.WorkflowCancelRequested); ok {
				found = true
				if cancel.Reason != "test reason" {
					t.Errorf("Reason = %s, want 'test reason'", cancel.Reason)
				}
			}
		}

		if !found {
			t.Error("WorkflowCancelRequested event not found in history")
		}
	}
}

func testCancelSchedulesTask(factory Factory) func(t *testing.T) {
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
			t.Fatalf("expected 1 initial task, got %d", len(tasks))
		}

		err = store.CancelWorkflow(ctx, "wf-1", "test reason")
		if err != nil {
			t.Fatalf("CancelWorkflow failed: %v", err)
		}

		ctx2, cancel2 := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel2()

		tasks, err = store.WaitForWorkflowTasks(ctx2, "worker-1", 10)
		if err != nil {
			t.Fatalf("second WaitForWorkflowTasks failed: %v", err)
		}
		if len(tasks) != 1 {
			t.Fatalf("expected 1 cancel-triggered task, got %d", len(tasks))
		}
	}
}

func testCancelSetsStatus(factory Factory) func(t *testing.T) {
	return func(t *testing.T) {
		store, _ := newTestStore(t, factory)
		ctx := context.Background()

		runID, err := store.CreateWorkflow(ctx, "wf-1", "TestWorkflow", nil, baseTime)
		if err != nil {
			t.Fatalf("CreateWorkflow failed: %v", err)
		}

		err = store.CancelWorkflow(ctx, "wf-1", "test reason")
		if err != nil {
			t.Fatalf("CancelWorkflow failed: %v", err)
		}

		status, err := store.GetStatus(ctx, "wf-1", runID)
		if err != nil {
			t.Fatalf("GetStatus failed: %v", err)
		}

		if status != journal.WorkflowStatusCancelling {
			t.Errorf("status = %v, want Cancelling", status)
		}
	}
}
