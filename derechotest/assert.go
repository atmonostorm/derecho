package derechotest

import (
	"bytes"
	"encoding/json"

	"github.com/atmonostorm/derecho/journal"
)

// ActivityCalls returns all calls made to the named activity.
func (e *TestEnv) ActivityCalls(name string) []ActivityCall {
	return e.calls[name]
}

// Events returns all events recorded during the test.
func (e *TestEnv) Events() []journal.Event {
	if e.state == nil {
		return nil
	}
	return e.state.Events()
}

// AssertActivityCalled fails if the named activity was never called.
func (e *TestEnv) AssertActivityCalled(name string) {
	e.t.Helper()
	if len(e.calls[name]) == 0 {
		e.t.Errorf("derechotest: expected activity %q to be called, but it was not", name)
	}
}

// AssertActivityNotCalled fails if the named activity was called.
func (e *TestEnv) AssertActivityNotCalled(name string) {
	e.t.Helper()
	if len(e.calls[name]) > 0 {
		e.t.Errorf("derechotest: expected activity %q to not be called, but it was called %d time(s)", name, len(e.calls[name]))
	}
}

// AssertActivityCalledTimes fails if the activity wasn't called exactly n times.
func (e *TestEnv) AssertActivityCalledTimes(name string, n int) {
	e.t.Helper()
	got := len(e.calls[name])
	if got != n {
		e.t.Errorf("derechotest: expected activity %q to be called %d time(s), got %d", name, n, got)
	}
}

// AssertActivityCalledWith fails if the activity was never called with the given input.
func (e *TestEnv) AssertActivityCalledWith(name string, expectedInput any) {
	e.t.Helper()
	calls := e.calls[name]
	if len(calls) == 0 {
		e.t.Errorf("derechotest: expected activity %q to be called, but it was not", name)
		return
	}

	expectedJSON, err := json.Marshal(expectedInput)
	if err != nil {
		e.t.Fatalf("derechotest: marshal expected input: %v", err)
	}

	for _, call := range calls {
		if bytes.Equal(call.Input, expectedJSON) {
			return
		}
	}

	e.t.Errorf("derechotest: activity %q was called %d time(s), but never with expected input %s", name, len(calls), expectedJSON)
}

// AssertNoActivityErrors fails if any activity returned an error.
func (e *TestEnv) AssertNoActivityErrors() {
	e.t.Helper()
	for name, calls := range e.calls {
		for i, call := range calls {
			if call.Error != nil {
				e.t.Errorf("derechotest: activity %q call %d returned error: %v", name, i, call.Error)
			}
		}
	}
}

// ChildWorkflowCalls returns all calls made to the named child workflow.
func (e *TestEnv) ChildWorkflowCalls(name string) []ChildWorkflowCall {
	return e.childCalls[name]
}

// AssertChildWorkflowCalled fails if the named child workflow was never called.
func (e *TestEnv) AssertChildWorkflowCalled(name string) {
	e.t.Helper()
	if len(e.childCalls[name]) == 0 {
		e.t.Errorf("derechotest: expected child workflow %q to be called, but it was not", name)
	}
}

// AssertChildWorkflowCalledTimes fails if the child workflow wasn't called exactly n times.
func (e *TestEnv) AssertChildWorkflowCalledTimes(name string, n int) {
	e.t.Helper()
	got := len(e.childCalls[name])
	if got != n {
		e.t.Errorf("derechotest: expected child workflow %q to be called %d time(s), got %d", name, n, got)
	}
}

// AssertChildWorkflowCalledWithID fails if the child workflow was never called with the given workflowID.
func (e *TestEnv) AssertChildWorkflowCalledWithID(name, workflowID string) {
	e.t.Helper()
	calls := e.childCalls[name]
	if len(calls) == 0 {
		e.t.Errorf("derechotest: expected child workflow %q to be called, but it was not", name)
		return
	}

	for _, call := range calls {
		if call.WorkflowID == workflowID {
			return
		}
	}

	e.t.Errorf("derechotest: child workflow %q was called %d time(s), but never with workflowID %q", name, len(calls), workflowID)
}
