package derecho

import (
	"context"
	"sync"
	"time"

	"github.com/atmonostorm/derecho/journal"
)

// Clock abstracts time for deterministic testing.
type Clock interface {
	Now() time.Time
}

type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now() }

// FakeClock provides controllable time for tests.
type FakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func NewFakeClock(t time.Time) *FakeClock {
	return &FakeClock{now: t}
}

func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *FakeClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = t
}

func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

type timerWorker struct {
	store journal.Store
	clock Clock
}

func (w *timerWorker) Process(ctx context.Context) error {
	now := w.clock.Now()
	pending, err := w.store.GetTimersToFire(ctx, now)
	if err != nil {
		return err
	}

	for _, task := range pending {
		if err := w.processTimer(ctx, task, now); err != nil {
			return err
		}
	}

	if len(pending) == 0 {
		select {
		case <-time.After(100 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (w *timerWorker) processTimer(ctx context.Context, task journal.PendingTimerTask, firedAt time.Time) error {
	_, err := w.store.Append(ctx, task.WorkflowID, task.RunID, []journal.Event{
		journal.TimerFired{
			BaseEvent: journal.BaseEvent{ScheduledByID: task.ScheduledAt},
			FiredAt:   firedAt,
		},
		journal.WorkflowTaskScheduled{},
	}, 0)
	return err
}
