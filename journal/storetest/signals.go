package storetest

import (
	"context"
	"testing"
	"time"

	"github.com/atmonostorm/derecho/journal"
)

func testSignals(t *testing.T, factory Factory) {
	t.Run("SignalCreatesEvent", testSignalCreatesEvent(factory))
	t.Run("SignalSchedulesTask", testSignalSchedulesTask(factory))
}

func testSignalCreatesEvent(factory Factory) func(t *testing.T) {
	return func(t *testing.T) {
		store, _ := newTestStore(t, factory)
		ctx := context.Background()

		runID, err := store.CreateWorkflow(ctx, "wf-1", "TestWorkflow", nil, baseTime)
		if err != nil {
			t.Fatalf("CreateWorkflow failed: %v", err)
		}

		err = store.SignalWorkflow(ctx, "wf-1", "test-signal", []byte(`"payload"`))
		if err != nil {
			t.Fatalf("SignalWorkflow failed: %v", err)
		}

		events, err := store.Load(ctx, "wf-1", runID)
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}

		var found bool
		for _, e := range events {
			if sig, ok := e.(journal.SignalReceived); ok {
				found = true
				if sig.SignalName != "test-signal" {
					t.Errorf("SignalName = %s, want test-signal", sig.SignalName)
				}
				if string(sig.Payload) != `"payload"` {
					t.Errorf("Payload = %s, want %q", sig.Payload, `"payload"`)
				}
			}
		}

		if !found {
			t.Error("SignalReceived event not found in history")
		}
	}
}

func testSignalSchedulesTask(factory Factory) func(t *testing.T) {
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

		err = store.SignalWorkflow(ctx, "wf-1", "test-signal", nil)
		if err != nil {
			t.Fatalf("SignalWorkflow failed: %v", err)
		}

		ctx2, cancel2 := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel2()

		tasks, err = store.WaitForWorkflowTasks(ctx2, "worker-1", 10)
		if err != nil {
			t.Fatalf("second WaitForWorkflowTasks failed: %v", err)
		}
		if len(tasks) != 1 {
			t.Fatalf("expected 1 signal-triggered task, got %d", len(tasks))
		}
	}
}
