package derecho

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/atmonostorm/derecho/journal"
)

type activityWorker struct {
	store    journal.Store
	resolver ActivityResolver
	workerID string
	codec    Codec
	clock    Clock
}

func (w *activityWorker) Process(ctx context.Context) error {
	pending, err := w.store.WaitForActivityTasks(ctx, w.workerID)
	if err != nil {
		return err
	}

	for _, task := range pending {
		if err := w.processActivity(ctx, task); err != nil {
			return err
		}
	}
	return nil
}

func (w *activityWorker) processActivity(ctx context.Context, task journal.PendingActivityTask) error {
	fn, ok := w.resolver.ResolveActivity(task.ActivityName)
	if !ok {
		return fmt.Errorf("derecho: activity not registered: %s", task.ActivityName)
	}

	attempt := task.Attempt
	if attempt == 0 {
		attempt = 1
	}

	// Only emit ActivityStarted on first attempt - retries are operational, not journal state
	if attempt == 1 {
		_, err := w.store.Append(ctx, task.WorkflowID, task.RunID, []journal.Event{
			journal.ActivityStarted{
				BaseEvent: journal.BaseEvent{ScheduledByID: task.ScheduledAt},
				WorkerID:  w.workerID,
				StartedAt: w.clock.Now(),
			},
		}, 0)
		if err != nil {
			return err
		}
	}

	resultEvent := w.executeActivity(ctx, fn, task)

	if failed, ok := resultEvent.(journal.ActivityFailed); ok && task.RetryPolicy != nil {
		errVal := failed.Error
		policy := PolicyFromPayload(task.RetryPolicy)

		if !errVal.NonRetryable && policy.ShouldRetry(errVal.Kind, attempt) {
			nextDelay := policy.NextDelay(attempt)
			if errVal.NextRetryDelay > 0 {
				nextDelay = errVal.NextRetryDelay
			}

			if tp := task.TimeoutPolicy; tp != nil && !task.ScheduleToCloseDeadline.IsZero() {
				remaining := task.ScheduleToCloseDeadline.Sub(w.clock.Now())
				if remaining <= 0 {
					// Budget exhausted - fail now rather than queue a doomed retry
					resultEvent = journal.ActivityFailed{
						Error: journal.NewError(journal.ErrorKindTimeout, "activity timeout: schedule_to_close"),
					}
				} else {
					if nextDelay > remaining {
						nextDelay = remaining
					}
					return w.store.RequeueForRetry(ctx, task.WorkflowID, task.RunID, task.ScheduledAt, journal.RequeueInfo{
						Attempt:     attempt + 1,
						NotBefore:   w.clock.Now().Add(nextDelay),
						LastFailure: errVal.Message,
						RetryPolicy: task.RetryPolicy,
					})
				}
			} else {
				return w.store.RequeueForRetry(ctx, task.WorkflowID, task.RunID, task.ScheduledAt, journal.RequeueInfo{
					Attempt:     attempt + 1,
					NotBefore:   w.clock.Now().Add(nextDelay),
					LastFailure: errVal.Message,
					RetryPolicy: task.RetryPolicy,
				})
			}
		}
	}

	resultEvent = resultEvent.WithScheduledByID(task.ScheduledAt)

	if _, err := w.store.Append(ctx, task.WorkflowID, task.RunID, []journal.Event{resultEvent}, 0); err != nil {
		return err
	}

	_, err := w.store.Append(ctx, task.WorkflowID, task.RunID, []journal.Event{
		journal.WorkflowTaskScheduled{},
	}, 0)
	return err
}

func (w *activityWorker) executeActivity(ctx context.Context, fn any, task journal.PendingActivityTask) journal.Event {
	attempt := task.Attempt
	if attempt == 0 {
		attempt = 1
	}

	ctx = context.WithValue(ctx, activityInfoKey{}, ActivityInfo{
		WorkflowID:    task.WorkflowID,
		RunID:         task.RunID,
		ActivityName:  task.ActivityName,
		ScheduledAt:   task.ScheduledAt,
		Attempt:       attempt,
		ScheduledTime: task.ScheduledTime,
	})

	if tp := task.TimeoutPolicy; tp != nil && tp.HeartbeatTimeout > 0 {
		hb := func(details any) error {
			encoded, err := w.codec.Encode(details)
			if err != nil {
				return err
			}
			return w.store.RecordHeartbeat(ctx, task.WorkflowID, task.RunID, task.ScheduledAt, encoded)
		}
		ctx = context.WithValue(ctx, heartbeatKey{}, heartbeatFunc(hb))
	}

	if tp := task.TimeoutPolicy; tp != nil && tp.StartToCloseTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, tp.StartToCloseTimeout)
		defer cancel()
	}

	fnVal := reflect.ValueOf(fn)
	fnType := fnVal.Type()

	inputType := fnType.In(1)
	inputPtr := reflect.New(inputType)

	if err := w.codec.Decode(task.Input, inputPtr.Interface()); err != nil {
		return journal.ActivityFailed{
			Error: journal.NewErrorf(journal.ErrorKindApplication, "decode input: %v", err),
		}
	}

	results := fnVal.Call([]reflect.Value{
		reflect.ValueOf(ctx),
		inputPtr.Elem(),
	})

	if !results[1].IsNil() {
		err := results[1].Interface().(error)

		if errors.Is(err, context.DeadlineExceeded) {
			return journal.ActivityFailed{
				Error: journal.NewError(journal.ErrorKindTimeout, "activity timeout: start_to_close"),
			}
		}

		journalErr := journal.ToError(err)

		var nonRetryable *NonRetryableError
		var retryable *RetryableError

		if errors.As(err, &nonRetryable) {
			journalErr.NonRetryable = true
		} else if errors.As(err, &retryable) && retryable.NextDelay > 0 {
			journalErr.NextRetryDelay = retryable.NextDelay
		}

		return journal.ActivityFailed{
			Error: journalErr,
		}
	}

	resultJSON, err := w.codec.Encode(results[0].Interface())
	if err != nil {
		return journal.ActivityFailed{
			Error: journal.NewErrorf(journal.ErrorKindApplication, "encode result: %v", err),
		}
	}

	return journal.ActivityCompleted{
		Result: resultJSON,
	}
}
