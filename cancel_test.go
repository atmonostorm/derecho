package derecho

import (
	"testing"
	"time"

	"github.com/atmonostorm/derecho/journal"
)

func TestCancel_FutureResolves(t *testing.T) {
	state := NewStubExecutionState()

	var cancelInfo CancelInfo
	var gotCancel bool

	s := NewScheduler(state, func(ctx Context) {
		sel := NewSelector()
		AddFuture(sel, Cancelled(ctx), func(info CancelInfo, _ error) {
			cancelInfo = info
			gotCancel = true
		})
		sel.Select(ctx)
	}, testWorkflowInfo())

	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}
	if gotCancel {
		t.Error("should not have received cancel yet")
	}

	state.AddExternalEvent(journal.WorkflowCancelRequested{
		Reason:      "test cancel",
		RequestedAt: time.Now(),
	})
	state.AddExternalEvent(journal.WorkflowTaskScheduled{})

	if err := s.Advance(t.Context(), 2, time.Now()); err != nil {
		t.Fatal(err)
	}

	if !gotCancel {
		t.Error("should have received cancel")
	}
	if cancelInfo.Reason != "test cancel" {
		t.Errorf("unexpected reason: %s", cancelInfo.Reason)
	}
}

func TestCancel_CtxErrReturnsCancelled(t *testing.T) {
	state := NewStubExecutionState()

	var errBeforeCancel, errAfterCancel error

	s := NewScheduler(state, func(ctx Context) {
		errBeforeCancel = ctx.Err()

		sel := NewSelector()
		AddFuture(sel, Cancelled(ctx), func(_ CancelInfo, _ error) {
			errAfterCancel = ctx.Err()
		})
		sel.Select(ctx)
	}, testWorkflowInfo())

	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	if errBeforeCancel != nil {
		t.Errorf("ctx.Err() should be nil before cancel, got: %v", errBeforeCancel)
	}

	state.AddExternalEvent(journal.WorkflowCancelRequested{
		Reason:      "test",
		RequestedAt: time.Now(),
	})
	state.AddExternalEvent(journal.WorkflowTaskScheduled{})

	if err := s.Advance(t.Context(), 2, time.Now()); err != nil {
		t.Fatal(err)
	}

	if errAfterCancel != ErrCancelled {
		t.Errorf("ctx.Err() should be ErrCancelled after cancel, got: %v", errAfterCancel)
	}
}

func TestCancel_ActivityWinsRace(t *testing.T) {
	state := NewStubExecutionState()

	activity := NewActivityRef[string, string]("test")
	var activityResult string
	var cancelled bool

	s := NewScheduler(state, func(ctx Context) {
		future := activity.Execute(ctx, "input")

		sel := NewSelector()
		AddFuture(sel, future, func(result string, err error) {
			activityResult = result
		})
		AddFuture(sel, Cancelled(ctx), func(_ CancelInfo, _ error) {
			cancelled = true
		})
		sel.Select(ctx)
	}, testWorkflowInfo())

	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	var activityScheduledID int
	for _, ev := range state.Events() {
		if ev.EventType() == journal.TypeActivityScheduled {
			activityScheduledID = ev.Base().ID
			break
		}
	}

	state.AddExternalEvent(journal.ActivityCompleted{
		BaseEvent: journal.BaseEvent{ScheduledByID: activityScheduledID},
		Result:    []byte(`"done"`),
	})
	state.AddExternalEvent(journal.WorkflowTaskScheduled{})

	scheduledTaskID := state.Events()[len(state.Events())-1].Base().ID
	if err := s.Advance(t.Context(), scheduledTaskID, time.Now()); err != nil {
		t.Fatal(err)
	}

	if activityResult != "done" {
		t.Errorf("expected activity result 'done', got: %s", activityResult)
	}
	if cancelled {
		t.Error("should not have been cancelled")
	}
}

func TestCancel_CancelWinsRace(t *testing.T) {
	state := NewStubExecutionState()

	activity := NewActivityRef[string, string]("test")
	var activityCompleted bool
	var cancelled bool

	s := NewScheduler(state, func(ctx Context) {
		future := activity.Execute(ctx, "input")

		sel := NewSelector()
		AddFuture(sel, future, func(_ string, _ error) {
			activityCompleted = true
		})
		AddFuture(sel, Cancelled(ctx), func(_ CancelInfo, _ error) {
			cancelled = true
		})
		sel.Select(ctx)
	}, testWorkflowInfo())

	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	state.AddExternalEvent(journal.WorkflowCancelRequested{
		Reason:      "cancelled",
		RequestedAt: time.Now(),
	})
	state.AddExternalEvent(journal.WorkflowTaskScheduled{})

	if err := s.Advance(t.Context(), 2, time.Now()); err != nil {
		t.Fatal(err)
	}

	if activityCompleted {
		t.Error("activity should not have completed")
	}
	if !cancelled {
		t.Error("should have been cancelled")
	}
}

func TestCancel_GracefulTermination(t *testing.T) {
	state := NewStubExecutionState()

	workflowFn := func(ctx Context, _ string) (string, error) {
		_, _ = Cancelled(ctx).Get(ctx)
		return "", ErrCancelled
	}

	s := NewScheduler(state, BindWorkflowInput(workflowFn, []byte(`"input"`)), testWorkflowInfo())

	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	state.AddExternalEvent(journal.WorkflowCancelRequested{
		Reason:      "graceful shutdown",
		RequestedAt: time.Now(),
	})
	state.AddExternalEvent(journal.WorkflowTaskScheduled{})

	lastEventID := state.Events()[len(state.Events())-1].Base().ID
	if err := s.Advance(t.Context(), lastEventID, time.Now()); err != nil {
		t.Fatal(err)
	}

	var foundCancelled bool
	for _, ev := range state.Events() {
		if cancelled, ok := ev.(journal.WorkflowCancelled); ok {
			foundCancelled = true
			if cancelled.Forced {
				t.Error("expected Forced=false for graceful cancellation")
			}
			break
		}
	}
	if !foundCancelled {
		t.Error("expected WorkflowCancelled event")
	}
}

func TestCancel_ForceTerminationOnTimeout(t *testing.T) {
	state := NewStubExecutionState()

	activity := NewActivityRef[string, string]("slow")
	var activityStarted bool

	s := NewScheduler(state, func(ctx Context) {
		_, _ = Cancelled(ctx).Get(ctx)
		activity.Execute(ctx, "input")
		activityStarted = true
		Sleep(ctx, time.Hour)
	}, testWorkflowInfo())

	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	baseTime := time.Now()

	state.AddExternalEvent(journal.WorkflowCancelRequested{
		Reason:        "shutdown",
		RequestedAt:   baseTime,
		CancelTimeout: 5 * time.Minute,
	})
	state.AddExternalEvent(journal.WorkflowTaskScheduled{})

	lastEventID := state.Events()[len(state.Events())-1].Base().ID
	if err := s.Advance(t.Context(), lastEventID, baseTime.Add(time.Second)); err != nil {
		t.Fatal(err)
	}

	if !activityStarted {
		t.Error("activity should have started")
	}

	state.AddExternalEvent(journal.WorkflowTaskScheduled{})
	lastEventID = state.Events()[len(state.Events())-1].Base().ID

	if err := s.Advance(t.Context(), lastEventID, baseTime.Add(6*time.Minute)); err != nil {
		t.Fatal(err)
	}

	var foundCancelled bool
	for _, ev := range state.Events() {
		if cancelled, ok := ev.(journal.WorkflowCancelled); ok {
			foundCancelled = true
			if !cancelled.Forced {
				t.Error("expected Forced=true for timeout cancellation")
			}
			if cancelled.RemainingFibers < 1 {
				t.Error("expected at least 1 remaining fiber")
			}
			break
		}
	}
	if !foundCancelled {
		t.Error("expected WorkflowCancelled event")
	}
}

func TestCancel_MultipleCleanupActivities(t *testing.T) {
	state := NewStubExecutionState()

	cleanup1 := NewActivityRef[string, string]("cleanup1")
	cleanup2 := NewActivityRef[string, string]("cleanup2")
	cleanup3 := NewActivityRef[string, string]("cleanup3")

	var cleanupStarted int
	var cleanupCompleted int

	s := NewScheduler(state, func(ctx Context) {
		info, _ := Cancelled(ctx).Get(ctx)
		if info.Reason != "shutdown" {
			return
		}

		f1 := cleanup1.Execute(ctx, "c1")
		f2 := cleanup2.Execute(ctx, "c2")
		f3 := cleanup3.Execute(ctx, "c3")
		cleanupStarted = 3

		f1.Get(ctx)
		f2.Get(ctx)
		f3.Get(ctx)
		cleanupCompleted = 3
	}, testWorkflowInfo())

	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	if cleanupStarted != 0 {
		t.Error("cleanup should not have started yet")
	}

	state.AddExternalEvent(journal.WorkflowCancelRequested{
		Reason:      "shutdown",
		RequestedAt: time.Now(),
	})
	state.AddExternalEvent(journal.WorkflowTaskScheduled{})

	lastEventID := state.Events()[len(state.Events())-1].Base().ID
	if err := s.Advance(t.Context(), lastEventID, time.Now()); err != nil {
		t.Fatal(err)
	}

	if cleanupStarted != 3 {
		t.Errorf("expected 3 cleanup activities started, got %d", cleanupStarted)
	}

	var scheduledActivities []int
	for _, ev := range state.Events() {
		if ev.EventType() == journal.TypeActivityScheduled {
			scheduledActivities = append(scheduledActivities, ev.Base().ID)
		}
	}

	if len(scheduledActivities) != 3 {
		t.Errorf("expected 3 activity scheduled events, got %d", len(scheduledActivities))
	}

	for _, scheduledID := range scheduledActivities {
		state.AddExternalEvent(journal.ActivityCompleted{
			BaseEvent: journal.BaseEvent{ScheduledByID: scheduledID},
			Result:    []byte(`"done"`),
		})
	}
	state.AddExternalEvent(journal.WorkflowTaskScheduled{})

	lastEventID = state.Events()[len(state.Events())-1].Base().ID
	if err := s.Advance(t.Context(), lastEventID, time.Now()); err != nil {
		t.Fatal(err)
	}

	if cleanupCompleted != 3 {
		t.Errorf("expected 3 cleanup activities completed, got %d", cleanupCompleted)
	}
}

func TestCancel_Replay(t *testing.T) {
	baseTime := time.Now()
	events := []journal.Event{
		journal.WorkflowStarted{
			BaseEvent:    journal.BaseEvent{ID: 1},
			WorkflowType: "test-workflow",
			Args:         []byte(`"input"`),
			StartedAt:    baseTime,
		},
		journal.WorkflowTaskScheduled{BaseEvent: journal.BaseEvent{ID: 2}},
		journal.WorkflowCancelRequested{
			BaseEvent:   journal.BaseEvent{ID: 3},
			Reason:      "test cancel",
			RequestedAt: baseTime.Add(time.Second),
		},
		journal.WorkflowTaskScheduled{BaseEvent: journal.BaseEvent{ID: 4}},
		journal.WorkflowCancelled{
			BaseEvent: journal.BaseEvent{ID: 5},
			Forced:    false,
		},
	}

	workflowFn := func(ctx Context, _ string) (string, error) {
		_, _ = Cancelled(ctx).Get(ctx)
		return "", ErrCancelled
	}

	result, err := ReplayWithResult(workflowFn, events)
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}

	if !result.Complete {
		t.Error("expected workflow to be complete")
	}

	if result.Error != ErrCancelled {
		t.Errorf("expected ErrCancelled, got %v", result.Error)
	}

	if len(result.NewEvents) != 0 {
		t.Errorf("expected no new events during replay, got %d", len(result.NewEvents))
	}
}
