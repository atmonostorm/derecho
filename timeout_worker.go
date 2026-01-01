package derecho

import (
	"context"
	"fmt"
	"time"

	"github.com/atmonostorm/derecho/journal"
)

type timeoutWorker struct {
	store               journal.Store
	clock               Clock
	workflowTaskTimeout time.Duration
}

func (w *timeoutWorker) Process(ctx context.Context) error {
	now := w.clock.Now()
	timedOut, err := w.store.GetTimedOutActivities(ctx, now)
	if err != nil {
		return err
	}

	for _, to := range timedOut {
		if err := w.processTimeout(ctx, to); err != nil {
			return err
		}
	}

	if w.workflowTaskTimeout > 0 {
		if _, err := w.store.ReleaseExpiredWorkflowTasks(ctx, now, w.workflowTaskTimeout); err != nil {
			return err
		}
	}

	if len(timedOut) == 0 {
		select {
		case <-time.After(100 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (w *timeoutWorker) processTimeout(ctx context.Context, to journal.TimedOutActivity) error {
	_, err := w.store.Append(ctx, to.WorkflowID, to.RunID, []journal.Event{
		journal.ActivityFailed{
			BaseEvent: journal.BaseEvent{ScheduledByID: to.ScheduledAt},
			Error:     journal.NewError(journal.ErrorKindTimeout, fmt.Sprintf("activity timeout: %s", to.TimeoutKind)),
		},
		journal.WorkflowTaskScheduled{},
	}, 0)
	return err
}
