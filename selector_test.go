package derecho

import (
	"testing"
	"time"

	"github.com/atmonostorm/derecho/journal"
)

func TestSelector_SingleTimer(t *testing.T) {
	state := NewStubExecutionState()

	var fired bool
	var result time.Time

	wf := func(ctx Context) {
		timer := NewTimer(ctx, time.Hour)

		selector := NewSelector()
		AddFuture(selector, timer, func(firedAt time.Time, err error) {
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			fired = true
			result = firedAt
		})
		selector.Select(ctx)
	}

	s := NewScheduler(state, wf, testWorkflowInfo())

	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	scheduled := state.Events()[0].(journal.TimerScheduled)
	now := time.Now()
	state.AddExternalEvent(journal.TimerFired{
		BaseEvent: journal.BaseEvent{ScheduledByID: scheduled.ID},
		FiredAt:   now,
	})

	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	if !fired {
		t.Error("callback not fired")
	}
	if !result.Equal(now) {
		t.Errorf("result = %v, want %v", result, now)
	}
}

func TestSelector_FirstOfTwo(t *testing.T) {
	state := NewStubExecutionState()

	var firstFired, secondFired bool

	wf := func(ctx Context) {
		timer1 := NewTimer(ctx, time.Hour)
		timer2 := NewTimer(ctx, 2*time.Hour)

		selector := NewSelector()
		AddFuture(selector, timer1, func(_ time.Time, _ error) {
			firstFired = true
		})
		AddFuture(selector, timer2, func(_ time.Time, _ error) {
			secondFired = true
		})
		selector.Select(ctx)
	}

	s := NewScheduler(state, wf, testWorkflowInfo())

	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	var timer1ID int
	for _, ev := range state.Events() {
		if scheduled, ok := ev.(journal.TimerScheduled); ok {
			timer1ID = scheduled.ID
			break
		}
	}

	state.AddExternalEvent(journal.TimerFired{
		BaseEvent: journal.BaseEvent{ScheduledByID: timer1ID},
		FiredAt:   time.Now(),
	})

	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	if !firstFired {
		t.Error("first callback not fired")
	}
	if secondFired {
		t.Error("second callback should not fire")
	}
}

func TestSelector_SecondOfTwo(t *testing.T) {
	state := NewStubExecutionState()

	var firstFired, secondFired bool

	wf := func(ctx Context) {
		timer1 := NewTimer(ctx, time.Hour)
		timer2 := NewTimer(ctx, 2*time.Hour)

		selector := NewSelector()
		AddFuture(selector, timer1, func(_ time.Time, _ error) {
			firstFired = true
		})
		AddFuture(selector, timer2, func(_ time.Time, _ error) {
			secondFired = true
		})
		selector.Select(ctx)
	}

	s := NewScheduler(state, wf, testWorkflowInfo())

	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	var timer2ID int
	for _, ev := range state.Events() {
		if scheduled, ok := ev.(journal.TimerScheduled); ok {
			timer2ID = scheduled.ID
		}
	}

	state.AddExternalEvent(journal.TimerFired{
		BaseEvent: journal.BaseEvent{ScheduledByID: timer2ID},
		FiredAt:   time.Now(),
	})

	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	if firstFired {
		t.Error("first callback should not fire")
	}
	if !secondFired {
		t.Error("second callback not fired")
	}
}

func TestSelector_HasPending(t *testing.T) {
	state := NewStubExecutionState()

	var hasPendingBefore, hasPendingAfter bool

	wf := func(ctx Context) {
		timer := NewTimer(ctx, time.Hour)

		selector := NewSelector()
		AddFuture(selector, timer, func(_ time.Time, _ error) {})

		hasPendingBefore = selector.HasPending()
		selector.Select(ctx)
		hasPendingAfter = selector.HasPending()
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

	if !hasPendingBefore {
		t.Error("HasPending should be true before Select")
	}
	if hasPendingAfter {
		t.Error("HasPending should be false after Select")
	}
}

func TestSelector_MultipleCalls(t *testing.T) {
	state := NewStubExecutionState()

	var call1, call2 bool

	wf := func(ctx Context) {
		timer1 := NewTimer(ctx, time.Hour)
		timer2 := NewTimer(ctx, 2*time.Hour)

		selector := NewSelector()
		AddFuture(selector, timer1, func(_ time.Time, _ error) {
			call1 = true
		})
		AddFuture(selector, timer2, func(_ time.Time, _ error) {
			call2 = true
		})

		selector.Select(ctx)
		selector.Select(ctx)
	}

	s := NewScheduler(state, wf, testWorkflowInfo())

	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	var timer1ID, timer2ID int
	for _, ev := range state.Events() {
		if scheduled, ok := ev.(journal.TimerScheduled); ok {
			if timer1ID == 0 {
				timer1ID = scheduled.ID
			} else {
				timer2ID = scheduled.ID
			}
		}
	}

	state.AddExternalEvent(journal.TimerFired{
		BaseEvent: journal.BaseEvent{ScheduledByID: timer1ID},
		FiredAt:   time.Now(),
	})

	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	if !call1 {
		t.Error("first callback not fired after first Select")
	}
	if call2 {
		t.Error("second callback should not fire after first Select")
	}

	state.AddExternalEvent(journal.TimerFired{
		BaseEvent: journal.BaseEvent{ScheduledByID: timer2ID},
		FiredAt:   time.Now(),
	})

	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	if !call2 {
		t.Error("second callback not fired after second Select")
	}
}

func TestSelector_EmptyCases(t *testing.T) {
	state := NewStubExecutionState()

	completed := false

	wf := func(ctx Context) {
		selector := NewSelector()
		selector.Select(ctx)
		completed = true
	}

	s := NewScheduler(state, wf, testWorkflowInfo())

	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	if !completed {
		t.Error("Select with no cases should return immediately")
	}
}

func TestSelector_CancelledTimer(t *testing.T) {
	state := NewStubExecutionState()

	var gotErr error
	selectCalled := false

	wf := func(ctx Context) {
		timer := NewTimer(ctx, time.Hour)
		CancelTimer(ctx, timer)

		// Yield to let the cancellation event commit
		Await(ctx, func() bool { return selectCalled })

		selector := NewSelector()
		AddFuture(selector, timer, func(_ time.Time, err error) {
			gotErr = err
		})
		selector.Select(ctx)
	}

	s := NewScheduler(state, wf, testWorkflowInfo())

	// First advance: timer scheduled, cancelled, workflow waiting
	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	// Signal to continue and advance again - cancellation now visible
	selectCalled = true
	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	if gotErr != ErrTimerCancelled {
		t.Errorf("expected ErrTimerCancelled, got %v", gotErr)
	}
}
