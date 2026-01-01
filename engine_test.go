package derecho_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/atmonostorm/derecho"
	"github.com/atmonostorm/derecho/journal"
)

type failingActivityStore struct {
	*derecho.MemoryStore
	err error
}

func (s *failingActivityStore) WaitForActivityTasks(ctx context.Context, workerID string, max int) ([]journal.PendingActivityTask, error) {
	return nil, s.err
}

func TestEngine_RunReturnsWorkerError(t *testing.T) {
	sentinel := errors.New("activity store broken")
	store := &failingActivityStore{
		MemoryStore: derecho.NewMemoryStore(),
		err:         sentinel,
	}

	engine := mustEngine(t, store)
	derecho.RegisterWorkflow(engine, "noop", func(ctx derecho.Context, _ struct{}) (struct{}, error) {
		return struct{}{}, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := engine.Run(ctx)
	if !errors.Is(err, sentinel) {
		t.Errorf("Run() = %v, want %v", err, sentinel)
	}
}

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

func TestRun_GetContinuedAsNew(t *testing.T) {
	store := derecho.NewMemoryStore()
	engine := mustEngine(t, store)

	derecho.RegisterWorkflow(engine, "looper", func(ctx derecho.Context, n int) (int, error) {
		return 0, derecho.NewContinueAsNewError(n + 1)
	})

	client := engine.Client()
	worker := engine.WorkflowWorker()

	run, err := client.StartWorkflow(t.Context(), "looper", "wf-can", 0)
	if err != nil {
		t.Fatal(err)
	}

	if err := worker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	var result int
	err = run.Get(t.Context(), &result)
	if !errors.Is(err, derecho.ErrContinuedAsNew) {
		t.Errorf("Run.Get() = %v, want ErrContinuedAsNew", err)
	}
}

func TestEngine_UnknownWorkflowTypeFails(t *testing.T) {
	store := derecho.NewMemoryStore()
	engine := mustEngine(t, store)

	client := engine.Client()
	worker := engine.WorkflowWorker()

	run, err := client.StartWorkflow(t.Context(), "nonexistent", "wf-unk", "hello")
	if err != nil {
		t.Fatal(err)
	}

	if err := worker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	status, err := store.GetStatus(t.Context(), "wf-unk", run.RunID())
	if err != nil {
		t.Fatal(err)
	}
	if status != journal.WorkflowStatusFailed {
		t.Errorf("workflow status = %v, want Failed", status)
	}
}
