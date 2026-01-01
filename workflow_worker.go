package derecho

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"time"

	"github.com/atmonostorm/derecho/journal"
)

type workflowEventEmitter interface {
	emitWorkflowCompleted(result []byte)
	emitWorkflowFailed(err error)
	emitWorkflowCancelled()
	emitContinueAsNew(input []byte)
}

type Workflow[I, O any] func(Context, I) (O, error)

type workflowWorker struct {
	store              journal.Store
	cache              *schedulerCache
	resolver           WorkflowResolver
	workerID           string
	codec              Codec
	defaultRetryPolicy *RetryPolicy
	logger             *slog.Logger
}

func (w *workflowWorker) Process(ctx context.Context) error {
	available := w.cache.AvailableSlots()
	if available == 0 {
		if err := w.waitForCacheSlot(ctx); err != nil {
			return err
		}
		available = w.cache.AvailableSlots()
	}

	pending, err := w.store.WaitForWorkflowTasks(ctx, w.workerID, available)
	if err != nil {
		return err
	}

	for _, task := range pending {
		if err := w.processWorkflow(ctx, task); err != nil {
			w.logger.Error("derecho: workflow processing failed",
				"workflow_id", task.WorkflowID,
				"run_id", task.RunID,
				"error", err)
			w.cache.Remove(task.WorkflowID, task.RunID)
		}
	}
	return nil
}

// waitForCacheSlot polls until cache has capacity or context cancelled.
// Prevents hot loop when cache is full of workflows with pending work.
func (w *workflowWorker) waitForCacheSlot(ctx context.Context) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if w.cache.AvailableSlots() > 0 {
				return nil
			}
		}
	}
}

func (w *workflowWorker) processWorkflow(ctx context.Context, task journal.PendingWorkflowTask) error {
	cached := w.cache.Get(task.WorkflowID, task.RunID)

	var sched *Scheduler
	var state *executionState

	if cached != nil {
		sched = cached.sched
		state = cached.state

		newEvents, err := w.store.LoadFrom(ctx, task.WorkflowID, task.RunID, state.LastEventID())
		if err != nil {
			w.logger.Error("derecho: failed to load events from cache",
				"workflow_id", task.WorkflowID,
				"run_id", task.RunID,
				"error", err)
			return err
		}
		state.AddEvents(newEvents)
	} else {
		events, err := w.store.Load(ctx, task.WorkflowID, task.RunID)
		if err != nil {
			w.logger.Error("derecho: failed to load workflow events",
				"workflow_id", task.WorkflowID,
				"run_id", task.RunID,
				"error", err)
			return err
		}

		state = newExecutionState(task.WorkflowID, task.RunID, events, w.store)

		var startedEvent journal.WorkflowStarted
		for _, ev := range events {
			if ws, ok := ev.(journal.WorkflowStarted); ok {
				startedEvent = ws
				break
			}
		}

		if startedEvent.WorkflowType == "" {
			w.logger.Error("derecho: no WorkflowStarted event found",
				"workflow_id", task.WorkflowID,
				"run_id", task.RunID)
			return fmt.Errorf("derecho: no WorkflowStarted event found for %s/%s", task.WorkflowID, task.RunID)
		}

		fn, ok := w.resolver.ResolveWorkflow(startedEvent.WorkflowType)
		if !ok {
			w.logger.Error("derecho: unknown workflow type",
				"workflow_id", task.WorkflowID,
				"workflow_type", startedEvent.WorkflowType)
			return fmt.Errorf("derecho: unknown workflow type: %s", startedEvent.WorkflowType)
		}

		info := WorkflowInfo{
			WorkflowID:   task.WorkflowID,
			RunID:        task.RunID,
			WorkflowType: startedEvent.WorkflowType,
			StartTime:    startedEvent.StartedAt,
		}

		workflowFn := BindWorkflowInput(fn, startedEvent.Args)

		var schedOpts []SchedulerOption
		schedOpts = append(schedOpts, WithCodec(w.codec))
		if w.defaultRetryPolicy != nil {
			schedOpts = append(schedOpts, WithSchedulerRetryPolicy(*w.defaultRetryPolicy))
		}
		sched = NewScheduler(state, workflowFn, info, schedOpts...)
	}

	if err := sched.Advance(ctx, task.ScheduledAt, task.StartedAt); err != nil {
		w.logger.Error("derecho: workflow advance failed",
			"workflow_id", task.WorkflowID,
			"run_id", task.RunID,
			"error", err)
		w.cache.Remove(task.WorkflowID, task.RunID)
		sched.Close()
		return err
	}

	if _, err := w.store.Append(ctx, task.WorkflowID, task.RunID, []journal.Event{
		journal.WorkflowTaskCompleted{
			BaseEvent: journal.BaseEvent{ScheduledByID: task.ScheduledAt},
		},
	}, 0); err != nil {
		return err
	}

	pendingEvents := state.PendingEvents()
	completed := w.isWorkflowCompleted(pendingEvents)
	if completed {
		w.cache.Remove(task.WorkflowID, task.RunID)
	} else {
		cs := &cachedScheduler{sched: sched, state: state}
		if !w.cache.Put(task.WorkflowID, task.RunID, cs) {
			sched.Close()
		}
	}

	return nil
}

func (w *workflowWorker) isWorkflowCompleted(events []journal.Event) bool {
	for _, ev := range events {
		switch ev.(type) {
		case journal.WorkflowCompleted, journal.WorkflowFailed, journal.WorkflowCancelled:
			return true
		}
	}
	return false
}

// BindWorkflowInput binds encoded input to a typed workflow, returning an executable func(Context).
// The returned function handles input unmarshaling and emits WorkflowCompleted/Failed events.
func BindWorkflowInput(fn any, encodedInput []byte) func(Context) {
	return func(ctx Context) {
		wctx, ok := ctx.(interface {
			workflowEventEmitter
			codecProvider
		})
		if !ok {
			panic(panicOutsideWorkflow)
		}
		codec := wctx.codec()

		fnVal := reflect.ValueOf(fn)
		fnType := fnVal.Type()

		inputType := fnType.In(1)
		inputPtr := reflect.New(inputType)

		if err := codec.Decode(encodedInput, inputPtr.Interface()); err != nil {
			wctx.emitWorkflowFailed(fmt.Errorf("decode input: %w", err))
			return
		}

		results := fnVal.Call([]reflect.Value{
			reflect.ValueOf(ctx),
			inputPtr.Elem(),
		})

		if !results[1].IsNil() {
			err := results[1].Interface().(error)
			var continueErr *ContinueAsNewError
			if errors.As(err, &continueErr) {
				inputJSON, encErr := codec.Encode(continueErr.Input)
				if encErr != nil {
					wctx.emitWorkflowFailed(fmt.Errorf("encode continue-as-new input: %w", encErr))
					return
				}
				wctx.emitContinueAsNew(inputJSON)
				return
			}
			if errors.Is(err, ErrCancelled) {
				wctx.emitWorkflowCancelled()
				return
			}
			wctx.emitWorkflowFailed(err)
			return
		}

		resultJSON, err := codec.Encode(results[0].Interface())
		if err != nil {
			wctx.emitWorkflowFailed(fmt.Errorf("encode result: %w", err))
			return
		}

		wctx.emitWorkflowCompleted(resultJSON)
	}
}
