package derecho

import (
	"testing"
	"time"

	"github.com/atmonostorm/derecho/journal"
)

func TestAwaitWithTimeout_ConditionMet(t *testing.T) {
	state := NewStubExecutionState()

	conditionReady := false
	var result bool

	wf := func(ctx Context) {
		result = AwaitWithTimeout(ctx, time.Hour, func() bool {
			return conditionReady
		})
	}

	s := NewScheduler(state, wf, testWorkflowInfo())

	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	conditionReady = true
	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	if !result {
		t.Error("expected true (condition met), got false")
	}

	var hasTimerCancelled bool
	for _, ev := range state.NewEvents() {
		if _, ok := ev.(journal.TimerCancelled); ok {
			hasTimerCancelled = true
			break
		}
	}
	if !hasTimerCancelled {
		t.Error("expected TimerCancelled event")
	}
}

func TestAwaitWithTimeout_Timeout(t *testing.T) {
	state := NewStubExecutionState()

	var result bool

	wf := func(ctx Context) {
		result = AwaitWithTimeout(ctx, time.Hour, func() bool {
			return false // never true
		})
	}

	s := NewScheduler(state, wf, testWorkflowInfo())

	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	var timerID int
	for _, ev := range state.NewEvents() {
		if scheduled, ok := ev.(journal.TimerScheduled); ok {
			timerID = scheduled.ID
			break
		}
	}

	state.AddExternalEvent(journal.TimerFired{
		BaseEvent: journal.BaseEvent{ScheduledByID: timerID},
		FiredAt:   time.Now(),
	})

	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	if result {
		t.Error("expected false (timeout), got true")
	}
}

func TestAwaitWithTimeout_ImmediateTrue(t *testing.T) {
	state := NewStubExecutionState()

	var result bool

	wf := func(ctx Context) {
		result = AwaitWithTimeout(ctx, time.Hour, func() bool {
			return true // immediately true
		})
	}

	s := NewScheduler(state, wf, testWorkflowInfo())

	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	if !result {
		t.Error("expected true (immediate condition), got false")
	}
}

func TestAwaitWithTimeout_ConditionBecomesTrue(t *testing.T) {
	state := NewStubExecutionState()

	counter := 0
	var result bool

	wf := func(ctx Context) {
		result = AwaitWithTimeout(ctx, time.Hour, func() bool {
			counter++
			return counter >= 3
		})
	}

	s := NewScheduler(state, wf, testWorkflowInfo())

	for i := 0; i < 5; i++ {
		if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
			t.Fatal(err)
		}
	}

	if !result {
		t.Error("expected true after counter reached 3")
	}
	if counter < 3 {
		t.Errorf("counter = %d, want >= 3", counter)
	}
}
