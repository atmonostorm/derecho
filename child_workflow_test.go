package derecho_test

import (
	"errors"
	"testing"

	"github.com/atmonostorm/derecho"
	"github.com/atmonostorm/derecho/journal"
)

func TestChildWorkflow_Simple(t *testing.T) {
	store := derecho.NewMemoryStore()
	engine := derecho.NewEngine(store)

	childRef := derecho.NewChildWorkflowRef[string, string]("child")

	derecho.RegisterWorkflow(engine, "child", func(ctx derecho.Context, input string) (string, error) {
		return "Hello, " + input + "!", nil
	})

	derecho.RegisterWorkflow(engine, "parent", func(ctx derecho.Context, input string) (string, error) {
		future := childRef.Execute(ctx, "child-1", input)
		return future.Get(ctx)
	})

	client := engine.Client()
	worker := engine.WorkflowWorker()

	run, err := client.StartWorkflow(t.Context(), "parent", "parent-1", "World")
	if err != nil {
		t.Fatal(err)
	}

	if err := worker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := worker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := worker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	var result string
	if err := run.Get(t.Context(), &result); err != nil {
		t.Fatal(err)
	}

	if result != "Hello, World!" {
		t.Errorf("expected 'Hello, World!', got %q", result)
	}
}

func TestChildWorkflow_Failure(t *testing.T) {
	store := derecho.NewMemoryStore()
	engine := derecho.NewEngine(store)

	childRef := derecho.NewChildWorkflowRef[string, string]("failing-child")

	derecho.RegisterWorkflow(engine, "failing-child", func(ctx derecho.Context, input string) (string, error) {
		return "", errors.New("child failed")
	})

	derecho.RegisterWorkflow(engine, "parent-with-failing-child", func(ctx derecho.Context, input string) (string, error) {
		future := childRef.Execute(ctx, "child-1", input)
		return future.Get(ctx)
	})

	client := engine.Client()
	worker := engine.WorkflowWorker()

	run, err := client.StartWorkflow(t.Context(), "parent-with-failing-child", "parent-1", "input")
	if err != nil {
		t.Fatal(err)
	}

	if err := worker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := worker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := worker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	var result string
	err = run.Get(t.Context(), &result)
	if err == nil {
		t.Fatal("expected error from child workflow")
	}
}

func TestChildWorkflow_ParallelChildren(t *testing.T) {
	store := derecho.NewMemoryStore()
	engine := derecho.NewEngine(store)

	childRef := derecho.NewChildWorkflowRef[int, int]("doubler")

	derecho.RegisterWorkflow(engine, "doubler", func(ctx derecho.Context, input int) (int, error) {
		return input * 2, nil
	})

	derecho.RegisterWorkflow(engine, "parallel-parent", func(ctx derecho.Context, input int) (int, error) {
		f1 := childRef.Execute(ctx, "child-1", input)
		f2 := childRef.Execute(ctx, "child-2", input+1)
		f3 := childRef.Execute(ctx, "child-3", input+2)

		r1, err := f1.Get(ctx)
		if err != nil {
			return 0, err
		}
		r2, err := f2.Get(ctx)
		if err != nil {
			return 0, err
		}
		r3, err := f3.Get(ctx)
		if err != nil {
			return 0, err
		}

		return r1 + r2 + r3, nil
	})

	client := engine.Client()
	worker := engine.WorkflowWorker()

	run, err := client.StartWorkflow(t.Context(), "parallel-parent", "parent-1", 10)
	if err != nil {
		t.Fatal(err)
	}

	// Process until parent completes - order of child completion is non-deterministic
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
			t.Fatalf("unexpected error: %v", err)
		}
		// (10*2) + (11*2) + (12*2) = 20 + 22 + 24 = 66
		if result != 66 {
			t.Errorf("expected 66, got %d", result)
		}
		return
	}

	t.Fatal("workflow did not complete after 20 iterations")
}

func TestChildWorkflow_EventSequence(t *testing.T) {
	store := derecho.NewMemoryStore()
	engine := derecho.NewEngine(store)

	childRef := derecho.NewChildWorkflowRef[string, string]("event-child")

	derecho.RegisterWorkflow(engine, "event-child", func(ctx derecho.Context, input string) (string, error) {
		return "done", nil
	})

	derecho.RegisterWorkflow(engine, "event-parent", func(ctx derecho.Context, input string) (string, error) {
		future := childRef.Execute(ctx, "child-1", input)
		return future.Get(ctx)
	})

	client := engine.Client()
	worker := engine.WorkflowWorker()

	run, err := client.StartWorkflow(t.Context(), "event-parent", "parent-1", "input")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 3; i++ {
		if err := worker.Process(t.Context()); err != nil {
			t.Fatal(err)
		}
	}

	events, err := store.Load(t.Context(), "parent-1", run.RunID())
	if err != nil {
		t.Fatal(err)
	}

	var foundScheduled, foundStarted, foundCompleted bool
	var scheduledID int

	for _, ev := range events {
		switch e := ev.(type) {
		case journal.ChildWorkflowScheduled:
			foundScheduled = true
			scheduledID = e.Base().ID
			if e.WorkflowType != "event-child" {
				t.Errorf("expected workflow type 'event-child', got %q", e.WorkflowType)
			}
			if e.WorkflowID != "child-1" {
				t.Errorf("expected workflow ID 'child-1', got %q", e.WorkflowID)
			}
		case journal.ChildWorkflowStarted:
			foundStarted = true
			if e.Base().ScheduledByID != scheduledID {
				t.Errorf("ChildWorkflowStarted.ScheduledByID = %d, want %d", e.Base().ScheduledByID, scheduledID)
			}
		case journal.ChildWorkflowCompleted:
			foundCompleted = true
			if e.Base().ScheduledByID != scheduledID {
				t.Errorf("ChildWorkflowCompleted.ScheduledByID = %d, want %d", e.Base().ScheduledByID, scheduledID)
			}
		}
	}

	if !foundScheduled {
		t.Error("ChildWorkflowScheduled event not found")
	}
	if !foundStarted {
		t.Error("ChildWorkflowStarted event not found")
	}
	if !foundCompleted {
		t.Error("ChildWorkflowCompleted event not found")
	}
}

func TestChildWorkflow_ParentReplay(t *testing.T) {
	store := derecho.NewMemoryStore()
	engine := derecho.NewEngine(store)

	childRef := derecho.NewChildWorkflowRef[string, string]("replay-child")

	childWorkflow := func(ctx derecho.Context, input string) (string, error) {
		return "child:" + input, nil
	}

	parentWorkflow := func(ctx derecho.Context, input string) (string, error) {
		future := childRef.Execute(ctx, "child-1", input)
		result, err := future.Get(ctx)
		if err != nil {
			return "", err
		}
		return "parent:" + result, nil
	}

	derecho.RegisterWorkflow(engine, "replay-child", childWorkflow)
	derecho.RegisterWorkflow(engine, "replay-parent", parentWorkflow)

	client := engine.Client()
	worker := engine.WorkflowWorker()

	run, err := client.StartWorkflow(t.Context(), "replay-parent", "parent-1", "hello")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		if err := worker.Process(t.Context()); err != nil {
			t.Fatal(err)
		}
		var result string
		err = run.Get(t.Context(), &result, derecho.NonBlocking())
		if err == derecho.ErrNotCompleted {
			continue
		}
		if err != nil {
			t.Fatalf("workflow failed: %v", err)
		}
		if result != "parent:child:hello" {
			t.Fatalf("expected 'parent:child:hello', got %q", result)
		}
		break
	}

	events, err := store.Load(t.Context(), "parent-1", run.RunID())
	if err != nil {
		t.Fatal(err)
	}

	var hasChildScheduled bool
	for _, ev := range events {
		if _, ok := ev.(journal.ChildWorkflowScheduled); ok {
			hasChildScheduled = true
			break
		}
	}
	if !hasChildScheduled {
		t.Fatal("expected ChildWorkflowScheduled in parent's event history")
	}

	err = derecho.Replay(parentWorkflow, events)
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}
}

func TestChildWorkflow_ParentClosePolicyTerminate(t *testing.T) {
	store := derecho.NewMemoryStore()
	engine := derecho.NewEngine(store)

	childRef := derecho.NewChildWorkflowRef[string, string]("slow-child")

	// Child workflow that just returns - it won't matter because parent completes first
	derecho.RegisterWorkflow(engine, "slow-child", func(ctx derecho.Context, input string) (string, error) {
		return "child done", nil
	})

	// Parent that spawns a child with default policy (Terminate), then immediately returns
	derecho.RegisterWorkflow(engine, "quick-parent", func(ctx derecho.Context, input string) (string, error) {
		// Spawn child but don't wait for it
		_ = childRef.Execute(ctx, "child-1", input)
		return "parent done", nil
	})

	client := engine.Client()
	worker := engine.WorkflowWorker()

	run, err := client.StartWorkflow(t.Context(), "quick-parent", "parent-1", "input")
	if err != nil {
		t.Fatal(err)
	}

	if err := worker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	var result string
	if err := run.Get(t.Context(), &result); err != nil {
		t.Fatal(err)
	}
	if result != "parent done" {
		t.Errorf("expected 'parent done', got %q", result)
	}
}
