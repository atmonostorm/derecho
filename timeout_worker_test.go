package derecho_test

import (
	"context"
	"testing"
	"time"

	"github.com/atmonostorm/derecho"
	"github.com/atmonostorm/derecho/journal"
)

func TestTimeoutWorker_ScheduleToStartTimeout(t *testing.T) {
	clock := derecho.NewFakeClock(time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC))
	store := derecho.NewMemoryStore(derecho.WithStoreClock(clock))
	engine := derecho.NewEngine(store, derecho.WithClock(clock))

	derecho.RegisterActivity(engine, "slow", func(ctx context.Context, _ struct{}) (struct{}, error) {
		return struct{}{}, nil
	})

	activityRef := derecho.NewActivityRef[struct{}, struct{}]("slow")

	derecho.RegisterWorkflow(engine, "timeout-test", func(ctx derecho.Context, _ struct{}) (struct{}, error) {
		future := activityRef.Execute(ctx, struct{}{}, derecho.WithScheduleToStartTimeout(time.Minute))
		return future.Get(ctx)
	})

	client := engine.Client()
	workflowWorker := engine.WorkflowWorker()
	timeoutWorker := engine.TimeoutWorker()

	run, err := client.StartWorkflow(t.Context(), "timeout-test", "wf-1", struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	runID := run.RunID()

	if err := workflowWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	if err := timeoutWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	events, _ := store.Load(t.Context(), "wf-1", runID)
	if hasTimeoutFailure(events) {
		t.Error("timeout should not fire before deadline")
	}

	clock.Advance(time.Minute)

	if err := timeoutWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	events, _ = store.Load(t.Context(), "wf-1", runID)
	if !hasTimeoutFailure(events) {
		t.Error("schedule_to_start timeout should fire after deadline")
	}

	if !hasTimeoutKind(events, "schedule_to_start") {
		t.Error("expected schedule_to_start timeout kind")
	}
}

func TestTimeoutWorker_StartToCloseTimeout_Passive(t *testing.T) {
	clock := derecho.NewFakeClock(time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC))
	store := derecho.NewMemoryStore(derecho.WithStoreClock(clock))
	engine := derecho.NewEngine(store, derecho.WithClock(clock))

	activityStarted := make(chan struct{})
	activityBlock := make(chan struct{})

	derecho.RegisterActivity(engine, "blocking", func(ctx context.Context, _ struct{}) (struct{}, error) {
		close(activityStarted)
		<-activityBlock
		return struct{}{}, nil
	})

	activityRef := derecho.NewActivityRef[struct{}, struct{}]("blocking")

	derecho.RegisterWorkflow(engine, "timeout-test", func(ctx derecho.Context, _ struct{}) (struct{}, error) {
		future := activityRef.Execute(ctx, struct{}{}, derecho.WithStartToCloseTimeout(time.Minute))
		return future.Get(ctx)
	})

	client := engine.Client()
	workflowWorker := engine.WorkflowWorker()
	activityWorker := engine.ActivityWorker()
	timeoutWorker := engine.TimeoutWorker()

	run, err := client.StartWorkflow(t.Context(), "timeout-test", "wf-1", struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	runID := run.RunID()

	if err := workflowWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	go func() {
		activityWorker.Process(t.Context())
	}()

	<-activityStarted

	if err := timeoutWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	events, _ := store.Load(t.Context(), "wf-1", runID)
	if hasTimeoutFailure(events) {
		t.Error("timeout should not fire before deadline")
	}

	clock.Advance(time.Minute)

	if err := timeoutWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	events, _ = store.Load(t.Context(), "wf-1", runID)
	if !hasTimeoutFailure(events) {
		t.Error("start_to_close timeout should fire after deadline")
	}

	if !hasTimeoutKind(events, "start_to_close") {
		t.Error("expected start_to_close timeout kind")
	}

	close(activityBlock)
}

func TestTimeoutWorker_ScheduleToCloseTimeout(t *testing.T) {
	clock := derecho.NewFakeClock(time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC))
	store := derecho.NewMemoryStore(derecho.WithStoreClock(clock))
	engine := derecho.NewEngine(store, derecho.WithClock(clock))

	derecho.RegisterActivity(engine, "slow", func(ctx context.Context, _ struct{}) (struct{}, error) {
		return struct{}{}, nil
	})

	activityRef := derecho.NewActivityRef[struct{}, struct{}]("slow")

	derecho.RegisterWorkflow(engine, "timeout-test", func(ctx derecho.Context, _ struct{}) (struct{}, error) {
		future := activityRef.Execute(ctx, struct{}{}, derecho.WithScheduleToCloseTimeout(time.Hour))
		return future.Get(ctx)
	})

	client := engine.Client()
	workflowWorker := engine.WorkflowWorker()
	timeoutWorker := engine.TimeoutWorker()

	run, err := client.StartWorkflow(t.Context(), "timeout-test", "wf-1", struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	runID := run.RunID()

	if err := workflowWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	clock.Advance(time.Hour - time.Second)

	if err := timeoutWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	events, _ := store.Load(t.Context(), "wf-1", runID)
	if hasTimeoutFailure(events) {
		t.Error("timeout should not fire before deadline")
	}

	clock.Advance(time.Second)

	if err := timeoutWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	events, _ = store.Load(t.Context(), "wf-1", runID)
	if !hasTimeoutFailure(events) {
		t.Error("schedule_to_close timeout should fire after deadline")
	}

	if !hasTimeoutKind(events, "schedule_to_close") {
		t.Error("expected schedule_to_close timeout kind")
	}
}

func TestTimeoutWorker_NoTimeoutIfActivityCompletes(t *testing.T) {
	clock := derecho.NewFakeClock(time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC))
	store := derecho.NewMemoryStore(derecho.WithStoreClock(clock))
	engine := derecho.NewEngine(store, derecho.WithClock(clock))

	derecho.RegisterActivity(engine, "fast", func(ctx context.Context, _ struct{}) (string, error) {
		return "done", nil
	})

	activityRef := derecho.NewActivityRef[struct{}, string]("fast")

	derecho.RegisterWorkflow(engine, "timeout-test", func(ctx derecho.Context, _ struct{}) (string, error) {
		future := activityRef.Execute(ctx, struct{}{},
			derecho.WithScheduleToStartTimeout(time.Minute),
			derecho.WithStartToCloseTimeout(time.Minute),
			derecho.WithScheduleToCloseTimeout(time.Hour))
		return future.Get(ctx)
	})

	client := engine.Client()
	workflowWorker := engine.WorkflowWorker()
	activityWorker := engine.ActivityWorker()
	timeoutWorker := engine.TimeoutWorker()

	run, err := client.StartWorkflow(t.Context(), "timeout-test", "wf-1", struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	runID := run.RunID()

	if err := workflowWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	if err := activityWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	clock.Advance(2 * time.Hour)

	if err := timeoutWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	events, _ := store.Load(t.Context(), "wf-1", runID)
	if hasTimeoutFailure(events) {
		t.Error("timeout should not fire if activity completed")
	}

	if !hasActivityCompleted(events) {
		t.Error("expected activity to complete successfully")
	}
}

func TestTimeoutWorker_RetryStopsWhenScheduleToCloseBudgetExhausted(t *testing.T) {
	clock := derecho.NewFakeClock(time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC))
	store := derecho.NewMemoryStore(derecho.WithStoreClock(clock))
	engine := derecho.NewEngine(store, derecho.WithClock(clock))

	attempts := 0
	derecho.RegisterActivity(engine, "flaky", func(ctx context.Context, _ struct{}) (struct{}, error) {
		attempts++
		return struct{}{}, context.DeadlineExceeded
	})

	activityRef := derecho.NewActivityRef[struct{}, struct{}]("flaky")

	derecho.RegisterWorkflow(engine, "retry-budget", func(ctx derecho.Context, _ struct{}) (struct{}, error) {
		policy := derecho.RetryPolicy{MaxAttempts: 10, InitialInterval: time.Second}
		future := activityRef.Execute(ctx, struct{}{},
			derecho.WithRetry(policy),
			derecho.WithScheduleToCloseTimeout(5*time.Second))
		return future.Get(ctx)
	})

	client := engine.Client()
	workflowWorker := engine.WorkflowWorker()
	activityWorker := engine.ActivityWorker()
	timeoutWorker := engine.TimeoutWorker()

	run, err := client.StartWorkflow(t.Context(), "retry-budget", "wf-1", struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	runID := run.RunID()

	if err := workflowWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	if err := activityWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)

	if err := activityWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}
	clock.Advance(2 * time.Second)

	if err := activityWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}
	clock.Advance(3 * time.Second)

	if err := timeoutWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	events, _ := store.Load(t.Context(), "wf-1", runID)
	if !hasTimeoutFailure(events) {
		t.Error("expected timeout failure when schedule_to_close budget exhausted")
	}

	if attempts > 3 {
		t.Errorf("expected at most 3 attempts before budget exhausted, got %d", attempts)
	}
}

func TestTimeoutWorker_ActiveStartToCloseEnforcement(t *testing.T) {
	store := derecho.NewMemoryStore()
	engine := derecho.NewEngine(store)

	derecho.RegisterActivity(engine, "context-aware", func(ctx context.Context, _ struct{}) (struct{}, error) {
		<-ctx.Done()
		return struct{}{}, ctx.Err()
	})

	activityRef := derecho.NewActivityRef[struct{}, struct{}]("context-aware")

	derecho.RegisterWorkflow(engine, "active-timeout", func(ctx derecho.Context, _ struct{}) (struct{}, error) {
		future := activityRef.Execute(ctx, struct{}{}, derecho.WithStartToCloseTimeout(50*time.Millisecond))
		return future.Get(ctx)
	})

	client := engine.Client()
	workflowWorker := engine.WorkflowWorker()
	activityWorker := engine.ActivityWorker()

	run, err := client.StartWorkflow(t.Context(), "active-timeout", "wf-1", struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	runID := run.RunID()

	if err := workflowWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	if err := activityWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	if err := workflowWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	events, _ := store.Load(t.Context(), "wf-1", runID)
	if !hasTimeoutFailure(events) {
		t.Error("expected timeout failure from active context enforcement")
	}

	var result struct{}
	err = run.Get(t.Context(), &result)
	if err == nil {
		t.Fatal("expected error from timeout")
	}

	jErr, ok := err.(*journal.Error)
	if !ok {
		t.Fatalf("expected *journal.Error, got %T: %v", err, err)
	}
	if jErr.Kind != journal.ErrorKindTimeout {
		t.Errorf("expected ErrorKindTimeout, got %s", jErr.Kind)
	}
}

func TestTimeoutWorker_MultipleTimeoutsFirstWins(t *testing.T) {
	clock := derecho.NewFakeClock(time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC))
	store := derecho.NewMemoryStore(derecho.WithStoreClock(clock))
	engine := derecho.NewEngine(store, derecho.WithClock(clock))

	derecho.RegisterActivity(engine, "slow", func(ctx context.Context, _ struct{}) (struct{}, error) {
		return struct{}{}, nil
	})

	activityRef := derecho.NewActivityRef[struct{}, struct{}]("slow")

	derecho.RegisterWorkflow(engine, "multi-timeout", func(ctx derecho.Context, _ struct{}) (struct{}, error) {
		future := activityRef.Execute(ctx, struct{}{},
			derecho.WithScheduleToStartTimeout(time.Minute),
			derecho.WithScheduleToCloseTimeout(time.Hour))
		return future.Get(ctx)
	})

	client := engine.Client()
	workflowWorker := engine.WorkflowWorker()
	timeoutWorker := engine.TimeoutWorker()

	run, err := client.StartWorkflow(t.Context(), "multi-timeout", "wf-1", struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	runID := run.RunID()

	if err := workflowWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	clock.Advance(time.Minute)

	if err := timeoutWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	events, _ := store.Load(t.Context(), "wf-1", runID)
	if !hasTimeoutKind(events, "schedule_to_start") {
		t.Error("expected schedule_to_start timeout (fires first)")
	}

	clock.Advance(time.Hour)

	if err := timeoutWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	events, _ = store.Load(t.Context(), "wf-1", runID)
	timeoutCount := countTimeoutFailures(events)
	if timeoutCount != 1 {
		t.Errorf("expected exactly 1 timeout event, got %d", timeoutCount)
	}
}

func hasTimeoutFailure(events []journal.Event) bool {
	for _, ev := range events {
		if failed, ok := ev.(journal.ActivityFailed); ok {
			if failed.Error.Kind == journal.ErrorKindTimeout {
				return true
			}
		}
	}
	return false
}

func hasTimeoutKind(events []journal.Event, kind string) bool {
	for _, ev := range events {
		if failed, ok := ev.(journal.ActivityFailed); ok {
			if failed.Error.Kind == journal.ErrorKindTimeout {
				expected := "activity timeout: " + kind
				if failed.Error.Message == expected {
					return true
				}
			}
		}
	}
	return false
}

func hasActivityCompleted(events []journal.Event) bool {
	for _, ev := range events {
		if _, ok := ev.(journal.ActivityCompleted); ok {
			return true
		}
	}
	return false
}

func countTimeoutFailures(events []journal.Event) int {
	count := 0
	for _, ev := range events {
		if failed, ok := ev.(journal.ActivityFailed); ok {
			if failed.Error.Kind == journal.ErrorKindTimeout {
				count++
			}
		}
	}
	return count
}
