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
