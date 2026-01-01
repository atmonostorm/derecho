package derecho

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/atmonostorm/derecho/journal"
)

func TestBindWorkflowInput_Success(t *testing.T) {
	state := NewStubExecutionState()

	wf := func(ctx Context, input string) (string, error) {
		return "hello " + input, nil
	}

	wrapped := BindWorkflowInput(wf, []byte(`"world"`))
	s := NewScheduler(state, wrapped, testWorkflowInfo())

	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	if len(state.Events()) != 1 {
		t.Fatalf("expected 1 event, got %d", len(state.Events()))
	}

	ev, ok := state.Events()[0].(journal.WorkflowCompleted)
	if !ok {
		t.Fatalf("expected journal.WorkflowCompleted, got %T", state.Events()[0])
	}

	var result string
	if err := json.Unmarshal(ev.Result, &result); err != nil {
		t.Fatal(err)
	}
	if result != "hello world" {
		t.Errorf("expected 'hello world', got %q", result)
	}
}

func TestBindWorkflowInput_WorkflowError(t *testing.T) {
	state := NewStubExecutionState()

	wf := func(ctx Context, input string) (string, error) {
		return "", errors.New("workflow failed")
	}

	wrapped := BindWorkflowInput(wf, []byte(`"input"`))
	s := NewScheduler(state, wrapped, testWorkflowInfo())

	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	if len(state.Events()) != 1 {
		t.Fatalf("expected 1 event, got %d", len(state.Events()))
	}

	ev, ok := state.Events()[0].(journal.WorkflowFailed)
	if !ok {
		t.Fatalf("expected journal.WorkflowFailed, got %T", state.Events()[0])
	}

	if ev.Error == nil || ev.Error.Error() != "workflow failed" {
		t.Errorf("expected 'workflow failed' error, got %v", ev.Error)
	}
}

func TestBindWorkflowInput_InvalidInputJSON(t *testing.T) {
	state := NewStubExecutionState()

	wf := func(ctx Context, input int) (int, error) {
		return input * 2, nil
	}

	wrapped := BindWorkflowInput(wf, []byte(`not valid json`))
	s := NewScheduler(state, wrapped, testWorkflowInfo())

	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	if len(state.Events()) != 1 {
		t.Fatalf("expected 1 event, got %d", len(state.Events()))
	}

	ev, ok := state.Events()[0].(journal.WorkflowFailed)
	if !ok {
		t.Fatalf("expected journal.WorkflowFailed, got %T", state.Events()[0])
	}

	if ev.Error == nil {
		t.Error("expected unmarshal error")
	}
}

func TestBindWorkflowInput_StructInput(t *testing.T) {
	state := NewStubExecutionState()

	type Input struct {
		Name  string `json:"name"`
		Count int    `json:"count"`
	}
	type Output struct {
		Message string `json:"message"`
	}

	wf := func(ctx Context, input Input) (Output, error) {
		return Output{Message: input.Name + "!"}, nil
	}

	inputJSON, _ := json.Marshal(Input{Name: "test", Count: 42})
	wrapped := BindWorkflowInput(wf, inputJSON)
	s := NewScheduler(state, wrapped, testWorkflowInfo())

	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	ev := state.Events()[0].(journal.WorkflowCompleted)
	var result Output
	json.Unmarshal(ev.Result, &result)

	if result.Message != "test!" {
		t.Errorf("expected 'test!', got %q", result.Message)
	}
}

func TestBindWorkflowInput_EmptyInput(t *testing.T) {
	state := NewStubExecutionState()

	wf := func(ctx Context, input struct{}) (string, error) {
		return "no input needed", nil
	}

	wrapped := BindWorkflowInput(wf, []byte("{}"))
	s := NewScheduler(state, wrapped, testWorkflowInfo())

	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	ev := state.Events()[0].(journal.WorkflowCompleted)
	var result string
	json.Unmarshal(ev.Result, &result)

	if result != "no input needed" {
		t.Errorf("expected 'no input needed', got %q", result)
	}
}

func TestProcessWorkflow_CompletionPurgesCache(t *testing.T) {
	store := NewMemoryStore()
	cache := newSchedulerCache(10)

	reg := newRegistry("workflow")
	reg.register("simple", func(ctx Context, input string) (string, error) {
		return "done", nil
	})

	w := &workflowWorker{
		store:    store,
		cache:    cache,
		resolver: reg,
		workerID: "test-worker",
		codec:    DefaultCodec,
		logger:   testLogger(),
	}

	ctx := t.Context()
	_, err := store.CreateWorkflow(ctx, "wf-1", "simple", []byte(`"hello"`), time.Now())
	if err != nil {
		t.Fatal(err)
	}

	tasks, err := store.WaitForWorkflowTasks(ctx, "test-worker", 1)
	if err != nil {
		t.Fatal(err)
	}

	if err := w.processWorkflow(ctx, tasks[0]); err != nil {
		t.Fatal(err)
	}

	if cache.AvailableSlots() != 10 {
		t.Error("completed workflow should not remain in cache")
	}
}

func TestProcessWorkflow_CachedCompletionClosesScheduler(t *testing.T) {
	store := NewMemoryStore()
	cache := newSchedulerCache(10)

	actRef := NewActivityRef[string, string]("greet")

	reg := newRegistry("workflow")
	reg.register("with-activity", func(ctx Context, input string) (string, error) {
		future := actRef.Execute(ctx, input)
		return future.Get(ctx)
	})

	w := &workflowWorker{
		store:    store,
		cache:    cache,
		resolver: reg,
		workerID: "test-worker",
		codec:    DefaultCodec,
		logger:   testLogger(),
	}

	ctx := t.Context()
	runID, err := store.CreateWorkflow(ctx, "wf-1", "with-activity", []byte(`"world"`), time.Now())
	if err != nil {
		t.Fatal(err)
	}

	tasks, err := store.WaitForWorkflowTasks(ctx, "test-worker", 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.processWorkflow(ctx, tasks[0]); err != nil {
		t.Fatal(err)
	}

	// Scheduler should be cached (workflow blocked on activity)
	cached := cache.Get("wf-1", runID)
	if cached == nil {
		t.Fatal("expected scheduler to be cached after scheduling activity")
	}
	sched := cached.sched
	cache.Put("wf-1", runID, cached)

	// Find the ActivityScheduled event ID
	events, _ := store.Load(ctx, "wf-1", runID)
	var scheduledID int
	for _, ev := range events {
		if as, ok := ev.(journal.ActivityScheduled); ok {
			scheduledID = as.Base().ID
			break
		}
	}
	if scheduledID == 0 {
		t.Fatal("ActivityScheduled event not found")
	}

	// Complete the activity and schedule a new workflow task
	resultJSON, _ := json.Marshal("hello world")
	store.Append(ctx, "wf-1", runID, []journal.Event{
		journal.ActivityCompleted{
			BaseEvent: journal.BaseEvent{ScheduledByID: scheduledID},
			Result:    resultJSON,
		},
	}, 0)
	store.Append(ctx, "wf-1", runID, []journal.Event{
		journal.WorkflowTaskScheduled{},
	}, 0)

	// Process the second task (cache hit path, workflow completes)
	tasks, err = store.WaitForWorkflowTasks(ctx, "test-worker", 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.processWorkflow(ctx, tasks[0]); err != nil {
		t.Fatal(err)
	}

	if sched.fibers != nil {
		t.Error("scheduler fibers should be nil after workflow completion")
	}
	if cache.AvailableSlots() != 10 {
		t.Error("completed workflow should not remain in cache")
	}
}

func TestValidateSignature(t *testing.T) {
	tests := []struct {
		name    string
		fn      any
		wantErr string
	}{
		{
			name:    "nil",
			fn:      nil,
			wantErr: "must be a function",
		},
		{
			name:    "not a function",
			fn:      "string",
			wantErr: "must be a function",
		},
		{
			name:    "no params",
			fn:      func() (string, error) { return "", nil },
			wantErr: "must have 2 parameters",
		},
		{
			name:    "one param",
			fn:      func(ctx Context) (string, error) { return "", nil },
			wantErr: "must have 2 parameters",
		},
		{
			name:    "wrong first param",
			fn:      func(s string, i int) (string, error) { return "", nil },
			wantErr: "first parameter must be derecho.Context",
		},
		{
			name:    "no return values",
			fn:      func(ctx Context, i int) {},
			wantErr: "must have 2 return values",
		},
		{
			name:    "one return value",
			fn:      func(ctx Context, i int) string { return "" },
			wantErr: "must have 2 return values",
		},
		{
			name:    "wrong second return",
			fn:      func(ctx Context, i int) (string, string) { return "", "" },
			wantErr: "second return value must be error",
		},
		{
			name:    "valid workflow",
			fn:      func(ctx Context, i int) (string, error) { return "", nil },
			wantErr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateSignature(tt.fn, derechoContextType, "derecho.Context")
			if tt.wantErr == "" {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			} else {
				if err == nil {
					t.Errorf("expected error containing %q", tt.wantErr)
				} else if !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf("error %q should contain %q", err.Error(), tt.wantErr)
				}
			}
		})
	}

	t.Run("valid activity", func(t *testing.T) {
		fn := func(ctx context.Context, i int) (string, error) { return "", nil }
		if err := validateSignature(fn, stdContextType, "context.Context"); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("activity wrong context", func(t *testing.T) {
		fn := func(s string, i int) (string, error) { return "", nil }
		err := validateSignature(fn, stdContextType, "context.Context")
		if err == nil || !strings.Contains(err.Error(), "context.Context") {
			t.Errorf("expected context.Context error, got %v", err)
		}
	})
}
