package derecho

import (
	"context"
	"testing"
	"time"

	"github.com/atmonostorm/derecho/journal"
)

func testWorkflowInfo() WorkflowInfo {
	return WorkflowInfo{
		WorkflowID:   "test-wf",
		RunID:        "test-run",
		WorkflowType: "TestWorkflow",
		StartTime:    time.Now(),
	}
}

func TestScheduler_SimpleWorkflow(t *testing.T) {
	state := NewStubExecutionState()
	ran := false

	s := NewScheduler(state, func(ctx Context) {
		ran = true
	}, testWorkflowInfo())

	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	if !ran {
		t.Error("workflow did not run")
	}
}

func TestScheduler_CooperativeScheduling(t *testing.T) {
	state := NewStubExecutionState()
	var order []int
	ready := false
	done := false

	s := NewScheduler(state, func(ctx Context) {
		order = append(order, 1)
		Go(ctx, func(ctx Context) {
			order = append(order, 2)
			ready = true
		})
		order = append(order, 3)
		Await(ctx, func() bool { return ready })
		done = true
		order = append(order, 4)
	}, testWorkflowInfo())

	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	// fiber-0 runs (1, 3), yields on Await, fiber-1 runs (2, sets ready), fiber-0 resumes (4)
	if len(order) != 4 {
		t.Fatalf("expected 4 entries, got %d", len(order))
	}
	if order[0] != 1 || order[1] != 3 || order[2] != 2 || order[3] != 4 {
		t.Errorf("unexpected order: %v", order)
	}
	if !done {
		t.Error("await did not complete")
	}
}

func TestScheduler_DeadlockDetection(t *testing.T) {
	state := NewStubExecutionState()

	s := NewScheduler(state, func(ctx Context) {
		// illegal blocking call - triggers deadlock detection
		time.Sleep(2 * time.Second)
	}, testWorkflowInfo())

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	err := s.Advance(ctx, 0, time.Now())
	if err == nil {
		t.Fatal("expected deadlock error")
	}
}

func TestScheduler_Close(t *testing.T) {
	state := NewStubExecutionState()
	fiberExited := make(chan struct{})

	s := NewScheduler(state, func(ctx Context) {
		defer close(fiberExited)
		Await(ctx, func() bool { return false })
	}, testWorkflowInfo())

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	_ = s.Advance(ctx, 0, time.Now())

	s.Close()

	select {
	case <-fiberExited:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("fiber did not exit after Close")
	}
}

func TestScheduler_PanicRecovery(t *testing.T) {
	state := NewStubExecutionState()

	s := NewScheduler(state, func(ctx Context) {
		panic("workflow panic!")
	}, testWorkflowInfo())

	err := s.Advance(t.Context(), 0, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(state.Events()) != 1 {
		t.Fatalf("expected 1 event, got %d", len(state.Events()))
	}

	failed, ok := state.Events()[0].(journal.WorkflowFailed)
	if !ok {
		t.Fatalf("expected journal.WorkflowFailed, got %T", state.Events()[0])
	}

	if failed.Error == nil {
		t.Fatal("expected error in payload")
	}

	if failed.Error.Kind != journal.ErrorKindPanic {
		t.Errorf("expected ErrorKindPanic, got %s", failed.Error.Kind)
	}

	if failed.Error.Message != "panic: workflow panic!" {
		t.Errorf("unexpected error message: %s", failed.Error.Message)
	}
}

func TestScheduler_PanicInChildFiber(t *testing.T) {
	state := NewStubExecutionState()

	s := NewScheduler(state, func(ctx Context) {
		Go(ctx, func(ctx Context) {
			panic("child panic!")
		})
	}, testWorkflowInfo())

	err := s.Advance(t.Context(), 0, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(state.Events()) != 1 {
		t.Fatalf("expected 1 event, got %d", len(state.Events()))
	}

	failed, ok := state.Events()[0].(journal.WorkflowFailed)
	if !ok {
		t.Fatalf("expected journal.WorkflowFailed, got %T", state.Events()[0])
	}

	if failed.Error.Kind != journal.ErrorKindPanic {
		t.Errorf("expected ErrorKindPanic, got %s", failed.Error.Kind)
	}
}

func TestScheduler_PanicDiscardsPartialEvents(t *testing.T) {
	state := NewStubExecutionState()

	activity := NewActivityRef[string, string]("test")

	s := NewScheduler(state, func(ctx Context) {
		activity.Execute(ctx, "input")
		panic("after activity")
	}, testWorkflowInfo())

	err := s.Advance(t.Context(), 0, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(state.Events()) != 1 {
		t.Fatalf("expected 1 event (WorkflowFailed only), got %d", len(state.Events()))
	}

	if _, ok := state.Events()[0].(journal.WorkflowFailed); !ok {
		t.Fatalf("expected journal.WorkflowFailed, got %T", state.Events()[0])
	}
}
