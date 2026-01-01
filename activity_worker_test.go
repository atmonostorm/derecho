package derecho_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/atmonostorm/derecho"
	"github.com/atmonostorm/derecho/journal"
)

func mustEngine(t *testing.T, store journal.Store, opts ...derecho.EngineOption) *derecho.Engine {
	opts = append(opts, derecho.WithEngineLogger(slog.New(slog.NewTextHandler(io.Discard, nil))))
	engine, err := derecho.NewEngine(store, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return engine
}

func TestActivityWorker_ExecutesActivity(t *testing.T) {
	store := derecho.NewMemoryStore()
	engine := mustEngine(t, store)

	derecho.RegisterActivity(engine, "greet", func(ctx context.Context, name string) (string, error) {
		return "Hello, " + name + "!", nil
	})

	activityRef := derecho.NewActivityRef[string, string]("greet")

	derecho.RegisterWorkflow(engine, "greeter", func(ctx derecho.Context, name string) (string, error) {
		future := activityRef.Execute(ctx, name)
		return future.Get(ctx)
	})

	client := engine.Client()
	workflowWorker := engine.WorkflowWorker()
	activityWorker := engine.ActivityWorker()

	run, err := client.StartWorkflow(t.Context(), "greeter", "wf-1", "World")
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
		t.Fatal(err)
	}

	if result != "Hello, World!" {
		t.Errorf("expected 'Hello, World!', got %q", result)
	}
}

func TestActivityWorker_ActivityFails(t *testing.T) {
	store := derecho.NewMemoryStore()
	engine := mustEngine(t, store)

	derecho.RegisterActivity(engine, "fail", func(ctx context.Context, _ string) (string, error) {
		return "", errors.New("intentional failure")
	})

	activityRef := derecho.NewActivityRef[string, string]("fail")

	derecho.RegisterWorkflow(engine, "failing", func(ctx derecho.Context, input string) (string, error) {
		future := activityRef.Execute(ctx, input)
		return future.Get(ctx)
	})

	client := engine.Client()
	workflowWorker := engine.WorkflowWorker()
	activityWorker := engine.ActivityWorker()

	run, err := client.StartWorkflow(t.Context(), "failing", "wf-1", "input")
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
	err = run.Get(t.Context(), &result)
	if err == nil {
		t.Fatal("expected error")
	}

	wErr, ok := err.(*journal.Error)
	if !ok {
		t.Fatalf("expected *journal.Error, got %T", err)
	}

	if wErr.Kind != journal.ErrorKindApplication {
		t.Errorf("expected ErrorKindApplication, got %s", wErr.Kind)
	}
}

func TestActivityWorker_UnregisteredActivity(t *testing.T) {
	store := derecho.NewMemoryStore()
	engine := mustEngine(t, store)

	activityRef := derecho.NewActivityRef[string, string]("unregistered")

	derecho.RegisterWorkflow(engine, "uses-unregistered", func(ctx derecho.Context, input string) (string, error) {
		future := activityRef.Execute(ctx, input)
		return future.Get(ctx)
	})

	client := engine.Client()
	workflowWorker := engine.WorkflowWorker()
	activityWorker := engine.ActivityWorker()

	run, err := client.StartWorkflow(t.Context(), "uses-unregistered", "wf-1", "input")
	if err != nil {
		t.Fatal(err)
	}

	// Workflow schedules the unregistered activity
	if err := workflowWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	// Activity worker handles unregistered activity by failing it (not crashing)
	if err := activityWorker.Process(t.Context()); err != nil {
		t.Fatal("activity worker should not crash on unregistered activity:", err)
	}

	// Workflow receives the failure and propagates it
	if err := workflowWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	// Verify workflow failed with the expected error
	var result string
	err = run.Get(t.Context(), &result)
	if err == nil {
		t.Fatal("expected workflow to fail")
	}

	if !strings.Contains(err.Error(), "activity not registered: unregistered") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestActivityWorker_GetActivityInfo(t *testing.T) {
	store := derecho.NewMemoryStore()
	engine := mustEngine(t, store)

	var capturedInfo derecho.ActivityInfo

	derecho.RegisterActivity(engine, "capture-info", func(ctx context.Context, _ struct{}) (string, error) {
		capturedInfo = derecho.GetActivityInfo(ctx)
		return capturedInfo.IdempotencyKey(), nil
	})

	activityRef := derecho.NewActivityRef[struct{}, string]("capture-info")

	derecho.RegisterWorkflow(engine, "info-test", func(ctx derecho.Context, _ struct{}) (string, error) {
		future := activityRef.Execute(ctx, struct{}{})
		return future.Get(ctx)
	})

	client := engine.Client()
	workflowWorker := engine.WorkflowWorker()
	activityWorker := engine.ActivityWorker()

	run, err := client.StartWorkflow(t.Context(), "info-test", "wf-1", struct{}{})
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
		t.Fatal(err)
	}

	if capturedInfo.WorkflowID != "wf-1" {
		t.Errorf("expected WorkflowID 'wf-1', got %q", capturedInfo.WorkflowID)
	}
	if capturedInfo.ActivityName != "capture-info" {
		t.Errorf("expected ActivityName 'capture-info', got %q", capturedInfo.ActivityName)
	}
	if capturedInfo.Attempt != 1 {
		t.Errorf("expected Attempt 1, got %d", capturedInfo.Attempt)
	}
	if capturedInfo.ScheduledAt == 0 {
		t.Error("expected ScheduledAt to be set")
	}

	expectedKey := "wf-1/4/1" // workflowID/scheduledAt/attempt
	if result != expectedKey {
		t.Errorf("expected idempotency key %q, got %q", expectedKey, result)
	}
}

func TestClient_CancelWorkflow(t *testing.T) {
	store := derecho.NewMemoryStore()
	engine := mustEngine(t, store)

	var gotCancelReason string
	var workflowCancelled bool

	derecho.RegisterWorkflow(engine, "cancellable", func(ctx derecho.Context, _ string) (string, error) {
		info, _ := derecho.Cancelled(ctx).Get(ctx)
		gotCancelReason = info.Reason
		workflowCancelled = true
		return "", derecho.ErrCancelled
	})

	client := engine.Client()
	workflowWorker := engine.WorkflowWorker()

	run, err := client.StartWorkflow(t.Context(), "cancellable", "wf-cancel-1", "input")
	if err != nil {
		t.Fatal(err)
	}

	if err := workflowWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	if workflowCancelled {
		t.Error("workflow should not be cancelled yet")
	}

	if err := client.CancelWorkflow(t.Context(), "wf-cancel-1", "user requested"); err != nil {
		t.Fatal(err)
	}

	if err := workflowWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	if !workflowCancelled {
		t.Error("workflow should be cancelled")
	}
	if gotCancelReason != "user requested" {
		t.Errorf("expected reason 'user requested', got %q", gotCancelReason)
	}

	var result string
	err = run.Get(t.Context(), &result)
	if !errors.Is(err, derecho.ErrCancelled) {
		t.Errorf("expected ErrCancelled, got %v", err)
	}
}

func TestClient_CancelWorkflowWithCleanup(t *testing.T) {
	store := derecho.NewMemoryStore()
	engine := mustEngine(t, store)

	var cleanupExecuted bool

	derecho.RegisterActivity(engine, "cleanup", func(ctx context.Context, _ string) (string, error) {
		cleanupExecuted = true
		return "cleaned up", nil
	})

	cleanupActivity := derecho.NewActivityRef[string, string]("cleanup")

	derecho.RegisterWorkflow(engine, "cancellable-with-cleanup", func(ctx derecho.Context, _ string) (string, error) {
		_, _ = derecho.Cancelled(ctx).Get(ctx)

		future := cleanupActivity.Execute(ctx, "cleanup data")
		_, err := future.Get(ctx)
		if err != nil {
			return "", err
		}

		return "", derecho.ErrCancelled
	})

	client := engine.Client()
	workflowWorker := engine.WorkflowWorker()
	activityWorker := engine.ActivityWorker()

	run, err := client.StartWorkflow(t.Context(), "cancellable-with-cleanup", "wf-cleanup-1", "input")
	if err != nil {
		t.Fatal(err)
	}

	if err := workflowWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	if err := client.CancelWorkflow(t.Context(), "wf-cleanup-1", "shutdown"); err != nil {
		t.Fatal(err)
	}

	if err := workflowWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	if cleanupExecuted {
		t.Error("cleanup should not be executed yet")
	}

	if err := activityWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	if !cleanupExecuted {
		t.Error("cleanup should be executed")
	}

	if err := workflowWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	var result string
	err = run.Get(t.Context(), &result)
	if !errors.Is(err, derecho.ErrCancelled) {
		t.Errorf("expected ErrCancelled, got %v", err)
	}
}

func TestActivityWorker_StructIO(t *testing.T) {
	store := derecho.NewMemoryStore()
	engine := mustEngine(t, store)

	type Input struct {
		Name  string `json:"name"`
		Count int    `json:"count"`
	}
	type Output struct {
		Message string `json:"message"`
		Total   int    `json:"total"`
	}

	derecho.RegisterActivity(engine, "process", func(ctx context.Context, input Input) (Output, error) {
		return Output{
			Message: "Processed " + input.Name,
			Total:   input.Count * 2,
		}, nil
	})

	activityRef := derecho.NewActivityRef[Input, Output]("process")

	derecho.RegisterWorkflow(engine, "processor", func(ctx derecho.Context, input Input) (Output, error) {
		future := activityRef.Execute(ctx, input)
		return future.Get(ctx)
	})

	client := engine.Client()
	workflowWorker := engine.WorkflowWorker()
	activityWorker := engine.ActivityWorker()

	run, err := client.StartWorkflow(t.Context(), "processor", "wf-1", Input{Name: "test", Count: 5})
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

	var result Output
	if err := run.Get(t.Context(), &result); err != nil {
		t.Fatal(err)
	}

	if result.Message != "Processed test" {
		t.Errorf("expected 'Processed test', got %q", result.Message)
	}
	if result.Total != 10 {
		t.Errorf("expected 10, got %d", result.Total)
	}
}
