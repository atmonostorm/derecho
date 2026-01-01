package derecho_test

import (
	"context"
	"testing"
	"time"

	"github.com/atmonostorm/derecho"
)

func TestHeartbeat_ActivityCompletes(t *testing.T) {
	clock := derecho.NewFakeClock(time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC))
	store := derecho.NewMemoryStore(derecho.WithStoreClock(clock))
	engine := mustEngine(t, store, derecho.WithClock(clock))

	derecho.RegisterActivity(engine, "heartbeating", func(ctx context.Context, _ struct{}) (string, error) {
		derecho.Heartbeat(ctx, "progress-1")
		derecho.Heartbeat(ctx, "progress-2")
		return "done", nil
	})

	activityRef := derecho.NewActivityRef[struct{}, string]("heartbeating")

	derecho.RegisterWorkflow(engine, "heartbeat-test", func(ctx derecho.Context, _ struct{}) (string, error) {
		future := activityRef.Execute(ctx, struct{}{}, derecho.WithHeartbeatTimeout(time.Minute))
		return future.Get(ctx)
	})

	client := engine.Client()
	workflowWorker := engine.WorkflowWorker()
	activityWorker := engine.ActivityWorker()

	run, err := client.StartWorkflow(t.Context(), "heartbeat-test", "wf-1", struct{}{})
	if err != nil {
		t.Fatal(err)
	}

	if err := workflowWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	if err := activityWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	if err := workflowWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	var result string
	if err := run.Get(t.Context(), &result); err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}

	if result != "done" {
		t.Errorf("expected result 'done', got %q", result)
	}
}

func TestHeartbeat_TimeoutWithoutHeartbeat(t *testing.T) {
	clock := derecho.NewFakeClock(time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC))
	store := derecho.NewMemoryStore(derecho.WithStoreClock(clock))
	engine := mustEngine(t, store, derecho.WithClock(clock))

	activityStarted := make(chan struct{})
	activityBlock := make(chan struct{})

	derecho.RegisterActivity(engine, "no-heartbeat", func(ctx context.Context, _ struct{}) (struct{}, error) {
		close(activityStarted)
		<-activityBlock
		return struct{}{}, nil
	})

	activityRef := derecho.NewActivityRef[struct{}, struct{}]("no-heartbeat")

	derecho.RegisterWorkflow(engine, "heartbeat-test", func(ctx derecho.Context, _ struct{}) (struct{}, error) {
		future := activityRef.Execute(ctx, struct{}{}, derecho.WithHeartbeatTimeout(time.Minute))
		return future.Get(ctx)
	})

	client := engine.Client()
	workflowWorker := engine.WorkflowWorker()
	activityWorker := engine.ActivityWorker()
	timeoutWorker := engine.TimeoutWorker()

	run, err := client.StartWorkflow(t.Context(), "heartbeat-test", "wf-1", struct{}{})
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
		t.Error("heartbeat timeout should not fire before deadline")
	}

	clock.Advance(time.Minute)

	if err := timeoutWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	events, _ = store.Load(t.Context(), "wf-1", runID)
	if !hasTimeoutFailure(events) {
		t.Error("heartbeat timeout should fire when activity doesn't heartbeat")
	}

	if !hasTimeoutKind(events, "heartbeat") {
		t.Error("expected heartbeat timeout kind")
	}

	close(activityBlock)
}

func TestHeartbeat_ResetsDeadline(t *testing.T) {
	clock := derecho.NewFakeClock(time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC))
	store := derecho.NewMemoryStore(derecho.WithStoreClock(clock))
	engine := mustEngine(t, store, derecho.WithClock(clock))

	activityStarted := make(chan struct{})
	heartbeatDone := make(chan struct{})
	activityComplete := make(chan struct{})

	derecho.RegisterActivity(engine, "long-heartbeat", func(ctx context.Context, _ struct{}) (struct{}, error) {
		close(activityStarted)
		<-heartbeatDone
		return struct{}{}, nil
	})

	activityRef := derecho.NewActivityRef[struct{}, struct{}]("long-heartbeat")

	derecho.RegisterWorkflow(engine, "heartbeat-test", func(ctx derecho.Context, _ struct{}) (struct{}, error) {
		future := activityRef.Execute(ctx, struct{}{}, derecho.WithHeartbeatTimeout(time.Minute))
		return future.Get(ctx)
	})

	client := engine.Client()
	workflowWorker := engine.WorkflowWorker()
	activityWorker := engine.ActivityWorker()
	timeoutWorker := engine.TimeoutWorker()

	run, err := client.StartWorkflow(t.Context(), "heartbeat-test", "wf-1", struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	runID := run.RunID()

	if err := workflowWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	go func() {
		activityWorker.Process(t.Context())
		close(activityComplete)
	}()

	<-activityStarted

	// Simulate heartbeats from the store level (since activity is blocked)
	// scheduledAt = 4 (WorkflowStarted=1, WorkflowTaskScheduled=2, WorkflowTaskStarted=3, ActivityScheduled=4)
	clock.Advance(30 * time.Second)
	store.RecordHeartbeat(t.Context(), "wf-1", runID, 4, []byte(`"checkpoint-1"`))

	clock.Advance(30 * time.Second)
	store.RecordHeartbeat(t.Context(), "wf-1", runID, 4, []byte(`"checkpoint-2"`))

	clock.Advance(30 * time.Second)
	store.RecordHeartbeat(t.Context(), "wf-1", runID, 4, []byte(`"checkpoint-3"`))

	// Total elapsed: 90 seconds with heartbeats every 30s
	// HeartbeatTimeout is 60s, but each heartbeat resets the deadline

	if err := timeoutWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	events, _ := store.Load(t.Context(), "wf-1", runID)
	if hasTimeoutFailure(events) {
		t.Error("activity should not timeout because heartbeats reset the deadline")
	}

	clock.Advance(time.Minute)

	if err := timeoutWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	events, _ = store.Load(t.Context(), "wf-1", runID)
	if !hasTimeoutFailure(events) {
		t.Error("activity should timeout after heartbeat deadline passes")
	}

	close(heartbeatDone)
	<-activityComplete
}

func TestHeartbeat_NoOpWithoutConfiguration(t *testing.T) {
	store := derecho.NewMemoryStore()
	engine := mustEngine(t, store)

	var heartbeatCalled bool

	derecho.RegisterActivity(engine, "no-timeout", func(ctx context.Context, _ struct{}) (struct{}, error) {
		err := derecho.Heartbeat(ctx, "some-progress")
		heartbeatCalled = true
		if err != nil {
			return struct{}{}, err
		}
		return struct{}{}, nil
	})

	activityRef := derecho.NewActivityRef[struct{}, struct{}]("no-timeout")

	derecho.RegisterWorkflow(engine, "heartbeat-test", func(ctx derecho.Context, _ struct{}) (struct{}, error) {
		// No heartbeat timeout configured
		future := activityRef.Execute(ctx, struct{}{})
		return future.Get(ctx)
	})

	client := engine.Client()
	workflowWorker := engine.WorkflowWorker()
	activityWorker := engine.ActivityWorker()

	run, err := client.StartWorkflow(t.Context(), "heartbeat-test", "wf-1", struct{}{})
	if err != nil {
		t.Fatal(err)
	}

	if err := workflowWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	if err := activityWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	if err := workflowWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	var result struct{}
	if err := run.Get(t.Context(), &result); err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}

	if !heartbeatCalled {
		t.Error("heartbeat function was not called")
	}
}

func TestHeartbeat_StaleHeartbeatIgnored(t *testing.T) {
	clock := derecho.NewFakeClock(time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC))
	store := derecho.NewMemoryStore(derecho.WithStoreClock(clock))
	engine := mustEngine(t, store, derecho.WithClock(clock))

	derecho.RegisterActivity(engine, "fast", func(ctx context.Context, _ struct{}) (string, error) {
		return "done", nil
	})

	activityRef := derecho.NewActivityRef[struct{}, string]("fast")

	derecho.RegisterWorkflow(engine, "heartbeat-test", func(ctx derecho.Context, _ struct{}) (string, error) {
		future := activityRef.Execute(ctx, struct{}{}, derecho.WithHeartbeatTimeout(time.Minute))
		return future.Get(ctx)
	})

	client := engine.Client()
	workflowWorker := engine.WorkflowWorker()
	activityWorker := engine.ActivityWorker()

	run, err := client.StartWorkflow(t.Context(), "heartbeat-test", "wf-1", struct{}{})
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

	// Activity already completed - this heartbeat should be a no-op
	err = store.RecordHeartbeat(t.Context(), "wf-1", runID, 4, []byte(`"late-checkpoint"`))
	if err != nil {
		t.Errorf("stale heartbeat should not return error, got: %v", err)
	}
}
