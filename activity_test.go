package derecho

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/atmonostorm/derecho/journal"
)

func TestActivity_ScheduleAndComplete(t *testing.T) {
	state := NewStubExecutionState()

	myActivity := NewActivityRef[string, string]("greet")

	var result string
	var resultErr error

	wf := func(ctx Context) {
		future := myActivity.Execute(ctx, "world")
		result, resultErr = future.Get(ctx)
	}

	s := NewScheduler(state, wf, testWorkflowInfo())

	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	if len(state.events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(state.events))
	}

	scheduled, ok := state.events[0].(journal.ActivityScheduled)
	if !ok {
		t.Fatalf("expected journal.ActivityScheduled, got %T", state.events[0])
	}
	if scheduled.Name != "greet" {
		t.Errorf("expected activity name 'greet', got %q", scheduled.Name)
	}

	var inputArg string
	json.Unmarshal(scheduled.Input, &inputArg)
	if inputArg != "world" {
		t.Errorf("expected input 'world', got %q", inputArg)
	}

	resultJSON, _ := json.Marshal("hello world")
	state.AddExternalEvent(journal.ActivityCompleted{
		BaseEvent: journal.BaseEvent{ScheduledByID: scheduled.ID},
		Result:    resultJSON,
	})

	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	if resultErr != nil {
		t.Errorf("unexpected error: %v", resultErr)
	}
	if result != "hello world" {
		t.Errorf("expected 'hello world', got %q", result)
	}
}

func TestActivity_ScheduleAndFail(t *testing.T) {
	state := NewStubExecutionState()

	myActivity := NewActivityRef[int, int]("double")

	var result int
	var resultErr error

	wf := func(ctx Context) {
		future := myActivity.Execute(ctx, 21)
		result, resultErr = future.Get(ctx)
	}

	s := NewScheduler(state, wf, testWorkflowInfo())

	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	scheduled := state.events[0].(journal.ActivityScheduled)

	state.AddExternalEvent(journal.ActivityFailed{
		BaseEvent: journal.BaseEvent{ScheduledByID: scheduled.ID},
		Error:     errActivityFailed,
	})

	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	if resultErr != errActivityFailed {
		t.Errorf("expected errActivityFailed, got %v", resultErr)
	}
	if result != 0 {
		t.Errorf("expected zero result, got %d", result)
	}
}

var errActivityFailed = journal.NewError(journal.ErrorKindApplication, "activity failed")

func TestActivity_StructIO(t *testing.T) {
	state := NewStubExecutionState()

	type Input struct {
		Name  string `json:"name"`
		Count int    `json:"count"`
	}
	type Output struct {
		Message string `json:"message"`
		Total   int    `json:"total"`
	}

	myActivity := NewActivityRef[Input, Output]("process")

	var result Output
	var resultErr error

	wf := func(ctx Context) {
		future := myActivity.Execute(ctx, Input{Name: "test", Count: 5})
		result, resultErr = future.Get(ctx)
	}

	s := NewScheduler(state, wf, testWorkflowInfo())

	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	scheduled := state.events[0].(journal.ActivityScheduled)
	var input Input
	json.Unmarshal(scheduled.Input, &input)
	if input.Name != "test" || input.Count != 5 {
		t.Errorf("unexpected input: %+v", input)
	}

	outputJSON, _ := json.Marshal(Output{Message: "done", Total: 10})
	state.AddExternalEvent(journal.ActivityCompleted{
		BaseEvent: journal.BaseEvent{ScheduledByID: scheduled.ID},
		Result:    outputJSON,
	})

	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	if resultErr != nil {
		t.Errorf("unexpected error: %v", resultErr)
	}
	if result.Message != "done" || result.Total != 10 {
		t.Errorf("unexpected result: %+v", result)
	}
}

func TestActivity_MultipleActivities(t *testing.T) {
	state := NewStubExecutionState()

	activityA := NewActivityRef[string, string]("a")
	activityB := NewActivityRef[string, string]("b")

	var resultA, resultB string

	wf := func(ctx Context) {
		futureA := activityA.Execute(ctx, "input-a")
		futureB := activityB.Execute(ctx, "input-b")

		resultA, _ = futureA.Get(ctx)
		resultB, _ = futureB.Get(ctx)
	}

	s := NewScheduler(state, wf, testWorkflowInfo())

	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	if len(state.events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(state.events))
	}

	ev0 := state.events[0].(journal.ActivityScheduled)
	ev1 := state.events[1].(journal.ActivityScheduled)
	if ev0.Name != "a" || ev1.Name != "b" {
		t.Errorf("unexpected activity names: %s, %s", ev0.Name, ev1.Name)
	}

	resultAJSON, _ := json.Marshal("result-a")
	resultBJSON, _ := json.Marshal("result-b")
	state.AddExternalEvent(journal.ActivityCompleted{
		BaseEvent: journal.BaseEvent{ScheduledByID: ev0.ID},
		Result:    resultAJSON,
	})
	state.AddExternalEvent(journal.ActivityCompleted{
		BaseEvent: journal.BaseEvent{ScheduledByID: ev1.ID},
		Result:    resultBJSON,
	})

	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	if resultA != "result-a" {
		t.Errorf("expected 'result-a', got %q", resultA)
	}
	if resultB != "result-b" {
		t.Errorf("expected 'result-b', got %q", resultB)
	}
}

func TestActivity_EncodeError(t *testing.T) {
	state := NewStubExecutionState()

	type BadInput struct {
		Ch chan int `json:"ch"`
	}

	myActivity := NewActivityRef[BadInput, string]("bad")

	var resultErr error

	wf := func(ctx Context) {
		future := myActivity.Execute(ctx, BadInput{Ch: make(chan int)})
		_, resultErr = future.Get(ctx)
	}

	s := NewScheduler(state, wf, testWorkflowInfo())

	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	if len(state.events) != 0 {
		t.Errorf("expected 0 events, got %d", len(state.events))
	}

	if resultErr == nil {
		t.Fatal("expected encode error")
	}

	if !strings.Contains(resultErr.Error(), "encode activity input") {
		t.Errorf("expected encode error message, got %q", resultErr.Error())
	}
}

func TestFuture_CachesResult(t *testing.T) {
	state := NewStubExecutionState()

	myActivity := NewActivityRef[string, string]("cache-test")

	var result1, result2 string
	var err1, err2 error

	wf := func(ctx Context) {
		future := myActivity.Execute(ctx, "input")
		result1, err1 = future.Get(ctx)
		result2, err2 = future.Get(ctx) // second Get should return cached result
	}

	s := NewScheduler(state, wf, testWorkflowInfo())

	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	scheduled := state.events[0].(journal.ActivityScheduled)

	resultJSON, _ := json.Marshal("cached-value")
	state.AddExternalEvent(journal.ActivityCompleted{
		BaseEvent: journal.BaseEvent{ScheduledByID: scheduled.ID},
		Result:    resultJSON,
	})

	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	if err1 != nil || err2 != nil {
		t.Errorf("unexpected errors: %v, %v", err1, err2)
	}
	if result1 != "cached-value" || result2 != "cached-value" {
		t.Errorf("expected both results to be 'cached-value', got %q and %q", result1, result2)
	}
}
