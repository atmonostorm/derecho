package derecho

import (
	"testing"
	"time"

	"github.com/atmonostorm/derecho/journal"
)

func TestTimerCancel_BeforeFire(t *testing.T) {
	state := NewStubExecutionState()

	var timerErr error

	wf := func(ctx Context) {
		timer := NewTimer(ctx, time.Hour)
		if err := CancelTimer(ctx, timer); err != nil {
			t.Errorf("CancelTimer failed: %v", err)
		}
		_, timerErr = timer.Get(ctx)
	}

	s := NewScheduler(state, wf, testWorkflowInfo())

	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	if timerErr != ErrTimerCancelled {
		t.Errorf("expected ErrTimerCancelled, got %v", timerErr)
	}

	var hasTimerScheduled, hasTimerCancelled bool
	for _, ev := range state.Events() {
		switch ev.(type) {
		case journal.TimerScheduled:
			hasTimerScheduled = true
		case journal.TimerCancelled:
			hasTimerCancelled = true
		}
	}

	if !hasTimerScheduled {
		t.Error("expected TimerScheduled event")
	}
	if !hasTimerCancelled {
		t.Error("expected TimerCancelled event")
	}
}

func TestTimerCancel_AlreadyFired(t *testing.T) {
	state := NewStubExecutionState()

	var cancelErr error

	wf := func(ctx Context) {
		timer := NewTimer(ctx, time.Hour)
		_, _ = timer.Get(ctx)
		cancelErr = CancelTimer(ctx, timer)
	}

	s := NewScheduler(state, wf, testWorkflowInfo())

	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	scheduled := state.Events()[0].(journal.TimerScheduled)
	state.AddExternalEvent(journal.TimerFired{
		BaseEvent: journal.BaseEvent{ScheduledByID: scheduled.ID},
		FiredAt:   time.Now(),
	})

	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	if cancelErr == nil {
		t.Error("expected error when cancelling already-fired timer")
	}
	if cancelErr.Error() != "derecho: timer already completed" {
		t.Errorf("unexpected error message: %v", cancelErr)
	}
}

func TestTimerCancel_MultipleTimers(t *testing.T) {
	state := NewStubExecutionState()

	var timer1Err, timer2Err error
	var timer2Result time.Time

	wf := func(ctx Context) {
		timer1 := NewTimer(ctx, time.Hour)
		timer2 := NewTimer(ctx, time.Hour)

		if err := CancelTimer(ctx, timer1); err != nil {
			t.Errorf("CancelTimer failed: %v", err)
		}

		_, timer1Err = timer1.Get(ctx)
		timer2Result, timer2Err = timer2.Get(ctx)
	}

	s := NewScheduler(state, wf, testWorkflowInfo())

	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	var timer2ScheduledID int
	for _, ev := range state.Events() {
		if scheduled, ok := ev.(journal.TimerScheduled); ok {
			timer2ScheduledID = scheduled.ID
		}
	}

	now := time.Now()
	state.AddExternalEvent(journal.TimerFired{
		BaseEvent: journal.BaseEvent{ScheduledByID: timer2ScheduledID},
		FiredAt:   now,
	})

	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	if timer1Err != ErrTimerCancelled {
		t.Errorf("timer1 expected ErrTimerCancelled, got %v", timer1Err)
	}

	if timer2Err != nil {
		t.Errorf("timer2 unexpected error: %v", timer2Err)
	}

	if !timer2Result.Equal(now) {
		t.Errorf("timer2 result = %v, want %v", timer2Result, now)
	}
}

func TestTimerCancel_FutureCachesResult(t *testing.T) {
	state := NewStubExecutionState()

	var err1, err2 error

	wf := func(ctx Context) {
		timer := NewTimer(ctx, time.Hour)
		CancelTimer(ctx, timer)
		_, err1 = timer.Get(ctx)
		_, err2 = timer.Get(ctx)
	}

	s := NewScheduler(state, wf, testWorkflowInfo())

	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	if err1 != ErrTimerCancelled {
		t.Errorf("first Get: expected ErrTimerCancelled, got %v", err1)
	}
	if err2 != ErrTimerCancelled {
		t.Errorf("second Get: expected ErrTimerCancelled, got %v", err2)
	}
}
