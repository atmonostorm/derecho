package derecho

import (
	"time"

	"github.com/atmonostorm/derecho/journal"
)

type eventQuerier interface {
	getByScheduledID(scheduledEventID int) []journal.Event
}

type Future[T any] interface {
	Get(ctx Context) (T, error)
}

type activityFuture[T any] struct {
	scheduledID int
	codec       Codec
	result      T
	err         error
	done        bool
}

func (f *activityFuture[T]) SetScheduledID(id int) {
	f.scheduledID = id
}

func newActivityFuture[T any](scheduledID int, codec Codec) Future[T] {
	return &activityFuture[T]{scheduledID: scheduledID, codec: codec}
}

func newFailedFuture[T any](err error) Future[T] {
	return &activityFuture[T]{err: err, done: true}
}

func (f *activityFuture[T]) Get(ctx Context) (T, error) {
	if f.done {
		return f.result, f.err
	}

	querier, ok := ctx.(eventQuerier)
	if !ok {
		panic(panicOutsideWorkflow)
	}

	Await(ctx, func() bool {
		events := querier.getByScheduledID(f.scheduledID)
		for _, ev := range events {
			switch e := ev.(type) {
			case journal.ActivityCompleted:
				if err := f.codec.Decode(e.Result, &f.result); err != nil {
					f.err = err
				}
				f.done = true
				return true
			case journal.ActivityFailed:
				f.err = e.Error
				f.done = true
				return true
			}
		}
		return false
	})

	return f.result, f.err
}

type timerFuture struct {
	scheduledID int
	result      time.Time
	err         error
	done        bool
}

func newTimerFuture(scheduledID int) *timerFuture {
	return &timerFuture{scheduledID: scheduledID}
}

func (f *timerFuture) SetScheduledID(id int) {
	f.scheduledID = id
}

func (f *timerFuture) Get(ctx Context) (time.Time, error) {
	if f.done {
		return f.result, f.err
	}

	querier, ok := ctx.(eventQuerier)
	if !ok {
		panic(panicOutsideWorkflow)
	}

	Await(ctx, func() bool {
		events := querier.getByScheduledID(f.scheduledID)
		for _, ev := range events {
			switch e := ev.(type) {
			case journal.TimerFired:
				f.result = e.FiredAt
				f.done = true
				return true
			case journal.TimerCancelled:
				f.err = ErrTimerCancelled
				f.done = true
				return true
			}
		}
		return false
	})

	return f.result, f.err
}
