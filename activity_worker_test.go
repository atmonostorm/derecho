package derecho_test

import (
	"context"
	"errors"
	"testing"

	"github.com/atmonostorm/derecho"
	"github.com/atmonostorm/derecho/journal"
)

func TestActivityWorker_ExecutesActivity(t *testing.T) {
	store := derecho.NewMemoryStore()
	engine := derecho.NewEngine(store)

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
	engine := derecho.NewEngine(store)

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
	engine := derecho.NewEngine(store)

	activityRef := derecho.NewActivityRef[string, string]("unregistered")

	derecho.RegisterWorkflow(engine, "uses-unregistered", func(ctx derecho.Context, input string) (string, error) {
		future := activityRef.Execute(ctx, input)
		return future.Get(ctx)
	})

	client := engine.Client()
	workflowWorker := engine.WorkflowWorker()
	activityWorker := engine.ActivityWorker()

	_, err := client.StartWorkflow(t.Context(), "uses-unregistered", "wf-1", "input")
	if err != nil {
		t.Fatal(err)
	}

	if err := workflowWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	err = activityWorker.Process(t.Context())
	if err == nil {
		t.Fatal("expected error for unregistered activity")
	}

	if err.Error() != "derecho: activity not registered: unregistered" {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestActivityWorker_GetActivityInfo(t *testing.T) {
	store := derecho.NewMemoryStore()
	engine := derecho.NewEngine(store)

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

func TestActivityWorker_StructIO(t *testing.T) {
	store := derecho.NewMemoryStore()
	engine := derecho.NewEngine(store)

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
