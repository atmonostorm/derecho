package derechotest

import (
	"encoding/json"

	"github.com/atmonostorm/derecho"
)

// StubActivity stubs an activity to return a fixed value.
func (e *TestEnv) StubActivity(name string, result any) *TestEnv {
	e.stubs[name] = &activityStub{result: result}
	return e
}

// StubActivityError stubs an activity to return an error.
func (e *TestEnv) StubActivityError(name string, err error) *TestEnv {
	e.stubs[name] = &activityStub{err: err}
	return e
}

// StubActivityFunc stubs an activity with a function for dynamic responses.
func (e *TestEnv) StubActivityFunc(name string, fn func(json.RawMessage) (any, error)) *TestEnv {
	e.stubs[name] = &activityStub{fn: fn}
	return e
}

// Stub provides type-safe activity stubbing using an ActivityRef.
func Stub[I, O any](e *TestEnv, ref derecho.ActivityRef[I, O], fn func(I) (O, error)) *TestEnv {
	e.stubs[ref.Name()] = &activityStub{
		fn: func(inputJSON json.RawMessage) (any, error) {
			var input I
			if len(inputJSON) > 0 {
				if err := json.Unmarshal(inputJSON, &input); err != nil {
					return nil, err
				}
			}
			return fn(input)
		},
	}
	return e
}

// StubActivityRetry stubs an activity to fail until a specified attempt, then return a result.
func (e *TestEnv) StubActivityRetry(name string, failUntilAttempt int, failWith error, successResult any) *TestEnv {
	e.stubs[name] = &activityStub{
		result:           successResult,
		failUntilAttempt: failUntilAttempt,
		failWith:         failWith,
	}
	return e
}

// StubChildWorkflow stubs a child workflow to return a fixed value.
func (e *TestEnv) StubChildWorkflow(name string, result any) *TestEnv {
	e.childStubs[name] = &childWorkflowStub{result: result}
	return e
}

// StubChildWorkflowError stubs a child workflow to return an error.
func (e *TestEnv) StubChildWorkflowError(name string, err error) *TestEnv {
	e.childStubs[name] = &childWorkflowStub{err: err}
	return e
}

// StubChildWorkflowFunc stubs a child workflow with a function for dynamic responses.
func (e *TestEnv) StubChildWorkflowFunc(name string, fn func(workflowID string, input json.RawMessage) (any, error)) *TestEnv {
	e.childStubs[name] = &childWorkflowStub{fn: fn}
	return e
}

// StubChild provides type-safe child workflow stubbing using a ChildWorkflowRef.
func StubChild[I, O any](e *TestEnv, ref derecho.ChildWorkflowRef[I, O], fn func(workflowID string, input I) (O, error)) *TestEnv {
	e.childStubs[ref.Name()] = &childWorkflowStub{
		fn: func(workflowID string, inputJSON json.RawMessage) (any, error) {
			var input I
			if len(inputJSON) > 0 {
				if err := json.Unmarshal(inputJSON, &input); err != nil {
					return nil, err
				}
			}
			return fn(workflowID, input)
		},
	}
	return e
}
