package derecho_test

import (
	"testing"
	"time"

	"github.com/atmonostorm/derecho"
	"github.com/atmonostorm/derecho/journal"
)

func TestTimerWorker_FiresReadyTimers(t *testing.T) {
	clock := derecho.NewFakeClock(time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC))
	store := derecho.NewMemoryStore(derecho.WithStoreClock(clock))
	engine := derecho.NewEngine(store, derecho.WithClock(clock))

	derecho.RegisterWorkflow(engine, "sleeper", func(ctx derecho.Context, _ struct{}) (struct{}, error) {
		derecho.Sleep(ctx, time.Hour)
		return struct{}{}, nil
	})

	client := engine.Client()
	workflowWorker := engine.WorkflowWorker()
	timerWorker := engine.TimerWorker()

	run, err := client.StartWorkflow(t.Context(), "sleeper", "wf-1", struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	runID := run.RunID()

	if err := workflowWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	if err := timerWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	events, _ := store.Load(t.Context(), "wf-1", runID)
	if hasTimerFired(events) {
		t.Error("timer should not fire before FireAt time")
	}

	clock.Advance(time.Hour)

	if err := timerWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	events, _ = store.Load(t.Context(), "wf-1", runID)
	if !hasTimerFired(events) {
		t.Error("timer should fire after advancing past FireAt")
	}
}

func TestTimerWorker_FiredAtMatchesClockTime(t *testing.T) {
	baseTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	clock := derecho.NewFakeClock(baseTime)
	store := derecho.NewMemoryStore(derecho.WithStoreClock(clock))
	engine := derecho.NewEngine(store, derecho.WithClock(clock))

	derecho.RegisterWorkflow(engine, "sleeper", func(ctx derecho.Context, _ struct{}) (struct{}, error) {
		derecho.Sleep(ctx, time.Minute)
		return struct{}{}, nil
	})

	client := engine.Client()
	workflowWorker := engine.WorkflowWorker()
	timerWorker := engine.TimerWorker()

	run, err := client.StartWorkflow(t.Context(), "sleeper", "wf-1", struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	runID := run.RunID()

	if err := workflowWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	expectedFiredAt := baseTime.Add(time.Minute)
	clock.Set(expectedFiredAt)

	if err := timerWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	events, _ := store.Load(t.Context(), "wf-1", runID)
	for _, ev := range events {
		if fired, ok := ev.(journal.TimerFired); ok {
			if !fired.FiredAt.Equal(expectedFiredAt) {
				t.Errorf("FiredAt = %v, want %v", fired.FiredAt, expectedFiredAt)
			}
			return
		}
	}
	t.Error("TimerFired event not found")
}

func TestTimerWorker_TimerNotReadyYet(t *testing.T) {
	clock := derecho.NewFakeClock(time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC))
	store := derecho.NewMemoryStore(derecho.WithStoreClock(clock))
	engine := derecho.NewEngine(store, derecho.WithClock(clock))

	derecho.RegisterWorkflow(engine, "sleeper", func(ctx derecho.Context, _ struct{}) (struct{}, error) {
		derecho.Sleep(ctx, time.Hour)
		return struct{}{}, nil
	})

	client := engine.Client()
	workflowWorker := engine.WorkflowWorker()
	timerWorker := engine.TimerWorker()

	run, err := client.StartWorkflow(t.Context(), "sleeper", "wf-1", struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	runID := run.RunID()

	if err := workflowWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	clock.Advance(time.Hour - time.Nanosecond)

	if err := timerWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	events, _ := store.Load(t.Context(), "wf-1", runID)
	if hasTimerFired(events) {
		t.Error("timer should not fire 1ns before FireAt")
	}

	clock.Advance(time.Nanosecond)

	if err := timerWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	events, _ = store.Load(t.Context(), "wf-1", runID)
	if !hasTimerFired(events) {
		t.Error("timer should fire exactly at FireAt")
	}
}

func TestTimerWorker_MultipleTimersSameFireAt(t *testing.T) {
	baseTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	clock := derecho.NewFakeClock(baseTime)
	store := derecho.NewMemoryStore(derecho.WithStoreClock(clock))
	engine := derecho.NewEngine(store, derecho.WithClock(clock))

	derecho.RegisterWorkflow(engine, "multi-timer", func(ctx derecho.Context, _ struct{}) (struct{}, error) {
		derecho.NewTimer(ctx, time.Hour)
		derecho.NewTimer(ctx, time.Hour)
		derecho.NewTimer(ctx, time.Hour)
		derecho.Await(ctx, func() bool { return false })
		return struct{}{}, nil
	})

	client := engine.Client()
	workflowWorker := engine.WorkflowWorker()
	timerWorker := engine.TimerWorker()

	run, err := client.StartWorkflow(t.Context(), "multi-timer", "wf-1", struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	runID := run.RunID()

	if err := workflowWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	clock.Advance(time.Hour)

	if err := timerWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	events, _ := store.Load(t.Context(), "wf-1", runID)
	firedCount := 0
	for _, ev := range events {
		if _, ok := ev.(journal.TimerFired); ok {
			firedCount++
		}
	}

	if firedCount != 3 {
		t.Errorf("expected 3 TimerFired events, got %d", firedCount)
	}
}

func hasTimerFired(events []journal.Event) bool {
	for _, ev := range events {
		if _, ok := ev.(journal.TimerFired); ok {
			return true
		}
	}
	return false
}
