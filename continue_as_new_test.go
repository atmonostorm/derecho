package derecho_test

import (
	"testing"

	"github.com/atmonostorm/derecho"
	"github.com/atmonostorm/derecho/journal"
)

func TestContinueAsNew_Basic(t *testing.T) {
	store := derecho.NewMemoryStore()
	engine := mustEngine(t, store)

	iterations := 0
	derecho.RegisterWorkflow(engine, "continue-test", func(ctx derecho.Context, count int) (int, error) {
		iterations++
		if count < 3 {
			return 0, derecho.NewContinueAsNewError(count + 1)
		}
		return count, nil
	})

	client := engine.Client()
	workflowWorker := engine.WorkflowWorker()

	run, err := client.StartWorkflow(t.Context(), "continue-test", "wf-1", 1)
	if err != nil {
		t.Fatal(err)
	}

	if err := workflowWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	status, err := store.GetStatus(t.Context(), "wf-1", run.RunID())
	if err != nil {
		t.Fatal(err)
	}
	if status != journal.WorkflowStatusContinuedAsNew {
		t.Errorf("GetStatus() = %s, want ContinuedAsNew", status)
	}

	if err := workflowWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := workflowWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	if iterations != 3 {
		t.Errorf("iterations = %d, want 3", iterations)
	}
}

func TestContinueAsNew_IDSemantics(t *testing.T) {
	store := derecho.NewMemoryStore()
	engine := mustEngine(t, store)

	var seenWorkflowIDs, seenRunIDs []string
	derecho.RegisterWorkflow(engine, "id-test", func(ctx derecho.Context, count int) (int, error) {
		info := derecho.GetInfo(ctx)
		seenWorkflowIDs = append(seenWorkflowIDs, info.WorkflowID)
		seenRunIDs = append(seenRunIDs, info.RunID)
		if count < 2 {
			return 0, derecho.NewContinueAsNewError(count + 1)
		}
		return count, nil
	})

	client := engine.Client()
	workflowWorker := engine.WorkflowWorker()

	_, err := client.StartWorkflow(t.Context(), "id-test", "my-workflow-id", 1)
	if err != nil {
		t.Fatal(err)
	}

	if err := workflowWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := workflowWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	if len(seenWorkflowIDs) != 2 {
		t.Fatalf("len(seenWorkflowIDs) = %d, want 2", len(seenWorkflowIDs))
	}

	// Workflow ID preserved across runs
	for i, id := range seenWorkflowIDs {
		if id != "my-workflow-id" {
			t.Errorf("seenWorkflowIDs[%d] = %q, want \"my-workflow-id\"", i, id)
		}
	}

	// Run IDs differ
	if seenRunIDs[0] == seenRunIDs[1] {
		t.Errorf("run IDs should differ: both are %q", seenRunIDs[0])
	}
}

func TestContinueAsNew_WithStructInput(t *testing.T) {
	store := derecho.NewMemoryStore()
	engine := mustEngine(t, store)

	type State struct {
		Count int    `json:"count"`
		Name  string `json:"name"`
	}

	var finalState State
	derecho.RegisterWorkflow(engine, "struct-test", func(ctx derecho.Context, state State) (State, error) {
		state.Count++
		if state.Count < 3 {
			return State{}, derecho.NewContinueAsNewError(state)
		}
		finalState = state
		return state, nil
	})

	client := engine.Client()
	workflowWorker := engine.WorkflowWorker()

	_, err := client.StartWorkflow(t.Context(), "struct-test", "wf-1", State{Count: 0, Name: "test"})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 3; i++ {
		if err := workflowWorker.Process(t.Context()); err != nil {
			t.Fatal(err)
		}
	}

	if finalState.Count != 3 {
		t.Errorf("finalState.Count = %d, want 3", finalState.Count)
	}
	if finalState.Name != "test" {
		t.Errorf("finalState.Name = %q, want \"test\"", finalState.Name)
	}
}

func TestContinueAsNew_Replay(t *testing.T) {
	store := derecho.NewMemoryStore()
	engine := mustEngine(t, store)

	continueWorkflow := func(ctx derecho.Context, count int) (int, error) {
		if count < 2 {
			return 0, derecho.NewContinueAsNewError(count + 1)
		}
		return count, nil
	}

	derecho.RegisterWorkflow(engine, "replay-continue", continueWorkflow)

	client := engine.Client()
	workflowWorker := engine.WorkflowWorker()

	run, err := client.StartWorkflow(t.Context(), "replay-continue", "wf-1", 1)
	if err != nil {
		t.Fatal(err)
	}

	if err := workflowWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	events, err := store.Load(t.Context(), "wf-1", run.RunID())
	if err != nil {
		t.Fatal(err)
	}

	if err := derecho.Replay(continueWorkflow, events); err != nil {
		t.Fatalf("Replay() = %v, want nil", err)
	}
}

func TestContinueAsNew_ChildWorkflow(t *testing.T) {
	store := derecho.NewMemoryStore()
	engine := mustEngine(t, store)

	childRef := derecho.NewChildWorkflowRef[int, int]("continuing-child")

	derecho.RegisterWorkflow(engine, "continuing-child", func(ctx derecho.Context, count int) (int, error) {
		if count < 3 {
			return 0, derecho.NewContinueAsNewError(count + 1)
		}
		return count, nil
	})

	derecho.RegisterWorkflow(engine, "parent-of-continuing", func(ctx derecho.Context, _ struct{}) (int, error) {
		future := childRef.Execute(ctx, "child-1", 1)
		return future.Get(ctx)
	})

	client := engine.Client()
	worker := engine.WorkflowWorker()

	run, err := client.StartWorkflow(t.Context(), "parent-of-continuing", "parent-1", struct{}{})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 20; i++ {
		if err := worker.Process(t.Context()); err != nil {
			t.Fatal(err)
		}

		var result int
		err = run.Get(t.Context(), &result, derecho.NonBlocking())
		if err == derecho.ErrNotCompleted {
			continue
		}
		if err != nil {
			t.Fatalf("workflow failed: %v", err)
		}
		if result != 3 {
			t.Errorf("expected 3, got %d", result)
		}
		return
	}

	t.Fatal("parent workflow did not complete after 20 iterations")
}
