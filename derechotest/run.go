package derechotest

import (
	"context"
	"encoding/json"

	"github.com/atmonostorm/derecho"
	"github.com/atmonostorm/derecho/journal"
)

// Run executes a workflow synchronously, auto-completing stubbed activities and timers.
// Returns the workflow result or error.
func Run[I, O any](env *TestEnv, wf derecho.Workflow[I, O], input I) (O, error) {
	var zero O

	codec := derecho.DefaultCodec
	inputJSON, err := codec.Encode(input)
	if err != nil {
		env.t.Fatalf("derechotest: encode input: %v", err)
		return zero, err
	}

	env.state = derecho.NewStubExecutionState()
	info := derecho.WorkflowInfo{
		WorkflowID:   "test-workflow",
		RunID:        "test-run",
		WorkflowType: "test",
		StartTime:    env.currentTime,
	}

	workflowFn := derecho.BindWorkflowInput(wf, inputJSON)
	sched := derecho.NewScheduler(env.state, workflowFn, info)

	const maxIterations = 1000
	for i := 0; i < maxIterations; i++ {
		eventsBefore := len(env.state.Events())

		if err := sched.Advance(context.Background(), 0, env.currentTime); err != nil {
			return zero, err
		}

		env.processActivities()
		env.processChildWorkflows()
		env.processTimers()

		result, wfErr, done := env.checkCompletion()
		if done {
			if wfErr != nil {
				return zero, wfErr
			}
			var out O
			if len(result) > 0 {
				if err := codec.Decode(result, &out); err != nil {
					env.t.Fatalf("derechotest: decode result: %v", err)
					return zero, err
				}
			}
			return out, nil
		}

		eventsAfter := len(env.state.Events())
		if eventsBefore == eventsAfter {
			env.t.Fatalf("derechotest: workflow made no progress - stuck at %d events", eventsAfter)
		}
	}

	env.t.Fatalf("derechotest: workflow did not complete after %d iterations", maxIterations)
	return zero, nil // unreachable but required by compiler
}

func (e *TestEnv) processActivities() {
	for _, ev := range e.state.Events() {
		scheduled, ok := ev.(journal.ActivityScheduled)
		if !ok {
			continue
		}

		resolved := e.state.GetByScheduledID(scheduled.ID)
		alreadyCompleted := false
		for _, r := range resolved {
			switch r.(type) {
			case journal.ActivityCompleted, journal.ActivityFailed:
				alreadyCompleted = true
			}
		}
		if alreadyCompleted {
			continue
		}

		stub, ok := e.stubs[scheduled.Name]
		if !ok {
			e.t.Fatalf("derechotest: unstubbed activity %q", scheduled.Name)
		}

		attemptState := e.getAttemptState(scheduled.ID)
		attemptState.attempt++

		if attemptState.attempt == 1 {
			e.state.AddExternalEvent(journal.ActivityStarted{
				BaseEvent: journal.BaseEvent{ScheduledByID: scheduled.ID},
				WorkerID:  "test-worker",
				StartedAt: e.currentTime,
			})
		}

		result, err := stub.execute(scheduled.Input, attemptState.attempt)

		call := ActivityCall{Input: scheduled.Input, Attempt: attemptState.attempt}
		if err != nil {
			call.Error = err

			if scheduled.RetryPolicy != nil && e.shouldRetry(scheduled.RetryPolicy, attemptState.attempt) {
				e.calls[scheduled.Name] = append(e.calls[scheduled.Name], call)
				continue
			}

			e.state.AddExternalEvent(journal.ActivityFailed{
				BaseEvent: journal.BaseEvent{ScheduledByID: scheduled.ID},
				Error:     journal.ToError(err),
			})
		} else {
			resultJSON, encErr := json.Marshal(result)
			if encErr != nil {
				e.t.Fatalf("derechotest: encode activity result for %q: %v", scheduled.Name, encErr)
			}
			call.Output = resultJSON
			e.state.AddExternalEvent(journal.ActivityCompleted{
				BaseEvent: journal.BaseEvent{ScheduledByID: scheduled.ID},
				Result:    resultJSON,
			})
		}
		e.calls[scheduled.Name] = append(e.calls[scheduled.Name], call)
	}
}

func (e *TestEnv) getAttemptState(scheduledID int) *activityAttemptState {
	if e.attempts == nil {
		e.attempts = make(map[int]*activityAttemptState)
	}
	if e.attempts[scheduledID] == nil {
		e.attempts[scheduledID] = &activityAttemptState{}
	}
	return e.attempts[scheduledID]
}

func (e *TestEnv) shouldRetry(policy *journal.RetryPolicyPayload, attempt int) bool {
	if policy.MaxAttempts > 0 && attempt >= policy.MaxAttempts {
		return false
	}
	return true
}

func (e *TestEnv) processChildWorkflows() {
	for _, ev := range e.state.Events() {
		scheduled, ok := ev.(journal.ChildWorkflowScheduled)
		if !ok {
			continue
		}

		resolved := e.state.GetByScheduledID(scheduled.ID)
		alreadyCompleted := false
		for _, r := range resolved {
			switch r.(type) {
			case journal.ChildWorkflowCompleted, journal.ChildWorkflowFailed:
				alreadyCompleted = true
			}
		}
		if alreadyCompleted {
			continue
		}

		stub, ok := e.childStubs[scheduled.WorkflowType]
		if !ok {
			e.t.Fatalf("derechotest: unstubbed child workflow %q", scheduled.WorkflowType)
		}

		e.state.AddExternalEvent(journal.ChildWorkflowStarted{
			BaseEvent:  journal.BaseEvent{ScheduledByID: scheduled.ID},
			ChildRunID: "test-run",
			StartedAt:  e.currentTime,
		})

		result, err := stub.execute(scheduled.WorkflowID, scheduled.Input)

		call := ChildWorkflowCall{
			WorkflowID: scheduled.WorkflowID,
			Input:      scheduled.Input,
		}
		if err != nil {
			call.Error = err
			e.state.AddExternalEvent(journal.ChildWorkflowFailed{
				BaseEvent:       journal.BaseEvent{ScheduledByID: scheduled.ID},
				ChildWorkflowID: scheduled.WorkflowID,
				ChildRunID:      "test-run",
				Error:           journal.ToError(err),
			})
		} else {
			resultJSON, encErr := json.Marshal(result)
			if encErr != nil {
				e.t.Fatalf("derechotest: encode child workflow result for %q: %v", scheduled.WorkflowType, encErr)
			}
			call.Output = resultJSON
			e.state.AddExternalEvent(journal.ChildWorkflowCompleted{
				BaseEvent:       journal.BaseEvent{ScheduledByID: scheduled.ID},
				ChildWorkflowID: scheduled.WorkflowID,
				ChildRunID:      "test-run",
				Result:          resultJSON,
			})
		}
		e.childCalls[scheduled.WorkflowType] = append(e.childCalls[scheduled.WorkflowType], call)
	}
}

func (e *TestEnv) processTimers() {
	for _, ev := range e.state.Events() {
		scheduled, ok := ev.(journal.TimerScheduled)
		if !ok {
			continue
		}

		resolved := e.state.GetByScheduledID(scheduled.ID)
		alreadyFired := false
		for _, r := range resolved {
			if _, ok := r.(journal.TimerFired); ok {
				alreadyFired = true
			}
		}
		if alreadyFired {
			continue
		}

		if scheduled.FireAt.After(e.currentTime) {
			e.currentTime = scheduled.FireAt
		}

		e.state.AddExternalEvent(journal.TimerFired{
			BaseEvent: journal.BaseEvent{ScheduledByID: scheduled.ID},
			FiredAt:   e.currentTime,
		})
	}
}

func (e *TestEnv) checkCompletion() (result []byte, err error, done bool) {
	for _, ev := range e.state.Events() {
		switch v := ev.(type) {
		case journal.WorkflowCompleted:
			return v.Result, nil, true
		case journal.WorkflowFailed:
			if v.Error != nil {
				return nil, v.Error, true
			}
			return nil, nil, true
		}
	}
	return nil, nil, false
}
