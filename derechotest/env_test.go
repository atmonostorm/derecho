package derechotest

import (
	"errors"
	"testing"
	"time"

	"github.com/atmonostorm/derecho"
)

var (
	greetActivity  = derecho.NewActivityRef[string, string]("greet")
	doubleActivity = derecho.NewActivityRef[int, int]("double")
)

func greetingWorkflow(ctx derecho.Context, name string) (string, error) {
	greeting, err := greetActivity.Execute(ctx, name).Get(ctx)
	if err != nil {
		return "", err
	}
	return greeting, nil
}

func TestRun_Stubbing(t *testing.T) {
	t.Run("fixed value", func(t *testing.T) {
		env := New(t)
		env.StubActivity("greet", "Hello, Alice!")

		result, err := Run(env, greetingWorkflow, "Alice")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result != "Hello, Alice!" {
			t.Errorf("got %q, want %q", result, "Hello, Alice!")
		}
		env.AssertActivityCalled("greet")
		env.AssertActivityCalledTimes("greet", 1)
	})

	t.Run("typed stub function", func(t *testing.T) {
		env := New(t)
		Stub(env, greetActivity, func(name string) (string, error) {
			return "Hi, " + name + "!", nil
		})

		result, err := Run(env, greetingWorkflow, "Bob")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result != "Hi, Bob!" {
			t.Errorf("got %q, want %q", result, "Hi, Bob!")
		}
	})
}

func multiActivityWorkflow(ctx derecho.Context, n int) (int, error) {
	doubled, err := doubleActivity.Execute(ctx, n).Get(ctx)
	if err != nil {
		return 0, err
	}
	tripled, err := doubleActivity.Execute(ctx, doubled).Get(ctx)
	if err != nil {
		return 0, err
	}
	return tripled, nil
}

func TestRun_MultipleActivities(t *testing.T) {
	env := New(t)
	Stub(env, doubleActivity, func(n int) (int, error) {
		return n * 2, nil
	})

	result, err := Run(env, multiActivityWorkflow, 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result != 20 {
		t.Errorf("got %d, want %d", result, 20)
	}

	env.AssertActivityCalledTimes("double", 2)
}

func TestRun_ActivityError(t *testing.T) {
	env := New(t)
	env.StubActivityError("greet", errors.New("connection failed"))

	_, err := Run(env, greetingWorkflow, "Alice")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func sleepWorkflow(ctx derecho.Context, _ struct{}) (string, error) {
	derecho.Sleep(ctx, 5*time.Second)
	return "done", nil
}

func TestRun_TimerAutoAdvance(t *testing.T) {
	startTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	env := New(t, WithStartTime(startTime))

	result, err := Run(env, sleepWorkflow, struct{}{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result != "done" {
		t.Errorf("got %q, want %q", result, "done")
	}
}

func TestAssertActivityCalledWith(t *testing.T) {
	env := New(t)
	Stub(env, greetActivity, func(name string) (string, error) {
		return "Hello!", nil
	})

	_, err := Run(env, greetingWorkflow, "Charlie")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	env.AssertActivityCalledWith("greet", "Charlie")
}

func TestEvents(t *testing.T) {
	env := New(t)
	env.StubActivity("greet", "Hello!")

	_, err := Run(env, greetingWorkflow, "Test")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	events := env.Events()
	if len(events) == 0 {
		t.Error("expected events, got none")
	}
}

var childWorkflowRef = derecho.NewChildWorkflowRef[string, string]("child-processor")

func parentWorkflow(ctx derecho.Context, input string) (string, error) {
	future := childWorkflowRef.Execute(ctx, "child-1", input)
	return future.Get(ctx)
}

func TestRun_ChildWorkflowStub(t *testing.T) {
	t.Run("fixed value", func(t *testing.T) {
		env := New(t)
		env.StubChildWorkflow("child-processor", "processed result")

		result, err := Run(env, parentWorkflow, "input")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result != "processed result" {
			t.Errorf("got %q, want %q", result, "processed result")
		}
		env.AssertChildWorkflowCalled("child-processor")
		env.AssertChildWorkflowCalledTimes("child-processor", 1)
	})

	t.Run("typed stub function", func(t *testing.T) {
		env := New(t)
		StubChild(env, childWorkflowRef, func(workflowID string, input string) (string, error) {
			return "child[" + workflowID + "] got: " + input, nil
		})

		result, err := Run(env, parentWorkflow, "test input")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result != "child[child-1] got: test input" {
			t.Errorf("got %q, want %q", result, "child[child-1] got: test input")
		}
	})
}

func TestRun_ChildWorkflowError(t *testing.T) {
	env := New(t)
	env.StubChildWorkflowError("child-processor", errors.New("child failed"))

	_, err := Run(env, parentWorkflow, "input")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestAssertChildWorkflowCalledWithID(t *testing.T) {
	env := New(t)
	env.StubChildWorkflow("child-processor", "result")

	_, err := Run(env, parentWorkflow, "input")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	env.AssertChildWorkflowCalledWithID("child-processor", "child-1")
}

func TestChildWorkflowCalls(t *testing.T) {
	env := New(t)
	env.StubChildWorkflow("child-processor", "result")

	_, err := Run(env, parentWorkflow, "input")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	calls := env.ChildWorkflowCalls("child-processor")
	if len(calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(calls))
	}

	if calls[0].WorkflowID != "child-1" {
		t.Errorf("expected workflow ID 'child-1', got %q", calls[0].WorkflowID)
	}
}

var batchedChildRef = derecho.NewChildWorkflowRef[int, struct{}]("batched-child")

func batchedChildWorkflow(ctx derecho.Context, itemCount int) (int, error) {
	const maxConcurrent = 3
	var futures []derecho.Future[struct{}]
	var completedCount int

	for i := 0; i < itemCount; i++ {
		future := batchedChildRef.Execute(ctx, "child-"+string(rune('A'+i)), i)
		futures = append(futures, future)

		if len(futures) >= maxConcurrent || i == itemCount-1 {
			waitCount := len(futures) / 2
			if waitCount == 0 && len(futures) > 0 {
				waitCount = 1
			}

			for j := 0; j < waitCount && j < len(futures); j++ {
				_, _ = futures[j].Get(ctx)
				completedCount++
			}

			if waitCount < len(futures) {
				futures = futures[waitCount:]
			} else {
				futures = nil
			}
		}
	}

	for _, f := range futures {
		_, _ = f.Get(ctx)
		completedCount++
	}

	return completedCount, nil
}

func TestRun_BatchedChildWorkflows(t *testing.T) {
	env := New(t)
	env.StubChildWorkflow("batched-child", struct{}{})

	itemCount := 13
	completedCount, err := Run(env, batchedChildWorkflow, itemCount)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should have launched all 13 children
	env.AssertChildWorkflowCalledTimes("batched-child", itemCount)

	// Should have completed all 13
	if completedCount != itemCount {
		t.Errorf("expected %d completed, got %d", itemCount, completedCount)
	}
}
