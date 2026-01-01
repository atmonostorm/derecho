package derecho_test

import (
	"testing"

	"github.com/atmonostorm/derecho"
	"github.com/atmonostorm/derecho/journal"
)

func TestEngine_SimpleWorkflow(t *testing.T) {
	store := derecho.NewMemoryStore()
	engine := mustEngine(t, store)

	derecho.RegisterWorkflow(engine, "greet", func(ctx derecho.Context, name string) (string, error) {
		return "Hello, " + name + "!", nil
	})

	client := engine.Client()
	worker := engine.WorkflowWorker()

	run, err := client.StartWorkflow(t.Context(), "greet", "wf-1", "World")
	if err != nil {
		t.Fatal(err)
	}

	if err := worker.Process(t.Context()); err != nil {
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

func TestEngine_WorkflowWithActivity(t *testing.T) {
	store := derecho.NewMemoryStore()
	engine := mustEngine(t, store)

	activityRef := derecho.NewActivityRef[int, int]("double")

	derecho.RegisterWorkflow(engine, "compute", func(ctx derecho.Context, input int) (int, error) {
		future := activityRef.Execute(ctx, input)
		result, err := future.Get(ctx)
		return result, err
	})

	client := engine.Client()
	worker := engine.WorkflowWorker()

	run, err := client.StartWorkflow(t.Context(), "compute", "wf-1", 21)
	if err != nil {
		t.Fatal(err)
	}

	if err := worker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	events, _ := store.Load(t.Context(), "wf-1", run.RunID())
	foundActivity := false
	for _, ev := range events {
		if ev.EventType() == journal.TypeActivityScheduled {
			foundActivity = true
			break
		}
	}
	if !foundActivity {
		t.Error("expected ActivityScheduled event")
	}
}

func TestEngine_WorkflowTaskEvents(t *testing.T) {
	store := derecho.NewMemoryStore()
	engine := mustEngine(t, store, derecho.WithWorkerID("test-worker"))

	derecho.RegisterWorkflow(engine, "simple", func(ctx derecho.Context, input string) (string, error) {
		return "done", nil
	})

	client := engine.Client()
	worker := engine.WorkflowWorker()

	run, err := client.StartWorkflow(t.Context(), "simple", "wf-1", "input")
	if err != nil {
		t.Fatal(err)
	}

	if err := worker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	events, _ := store.Load(t.Context(), "wf-1", run.RunID())

	var taskScheduledID int
	var foundStarted, foundCompleted bool
	var startedScheduledBy, completedScheduledBy int

	for _, ev := range events {
		base := ev.Base()
		switch ev.EventType() {
		case journal.TypeWorkflowTaskScheduled:
			taskScheduledID = base.ID
		case journal.TypeWorkflowTaskStarted:
			foundStarted = true
			startedScheduledBy = base.ScheduledByID
			if started, ok := ev.(journal.WorkflowTaskStarted); ok {
				if started.WorkerID != "test-worker" {
					t.Errorf("expected WorkerID 'test-worker', got %q", started.WorkerID)
				}
			}
		case journal.TypeWorkflowTaskCompleted:
			foundCompleted = true
			completedScheduledBy = base.ScheduledByID
		}
	}

	if taskScheduledID == 0 {
		t.Fatal("WorkflowTaskScheduled event not found")
	}
	if !foundStarted {
		t.Error("WorkflowTaskStarted event not found")
	}
	if !foundCompleted {
		t.Error("WorkflowTaskCompleted event not found")
	}

	if startedScheduledBy != taskScheduledID {
		t.Errorf("WorkflowTaskStarted.ScheduledByID = %d, want %d", startedScheduledBy, taskScheduledID)
	}
	if completedScheduledBy != taskScheduledID {
		t.Errorf("WorkflowTaskCompleted.ScheduledByID = %d, want %d", completedScheduledBy, taskScheduledID)
	}
}
