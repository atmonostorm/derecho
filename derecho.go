package derecho

import (
	"errors"
	"fmt"
	"time"

	"github.com/atmonostorm/derecho/journal"
)

// WorkflowNotFoundError is returned when signaling a workflow that doesn't exist or isn't running.
type WorkflowNotFoundError struct {
	WorkflowID string
}

func (e *WorkflowNotFoundError) Error() string {
	return fmt.Sprintf("derecho: workflow %q not found or not running", e.WorkflowID)
}

// ContinueAsNewError restarts the workflow with fresh history.
// Use to avoid unbounded event accumulation in long-running workflows.
type ContinueAsNewError struct {
	Input any
}

func NewContinueAsNewError(input any) *ContinueAsNewError {
	return &ContinueAsNewError{Input: input}
}

func (e *ContinueAsNewError) Error() string {
	return "derecho: continue as new"
}

// Await blocks until checkFn returns true.
// Each call to checkFn is a yield point - the scheduler runs other fibers between checks.
func Await(ctx Context, checkFn func() bool) {
	y, ok := ctx.(yielder)
	if !ok {
		panic(panicOutsideWorkflow)
	}

	for {
		if checkFn() {
			return
		}
		y.Yield()
	}
}

// Go spawns a new fiber to run goFn concurrently within the workflow.
// Child fibers are cooperatively scheduled with the parent.
func Go(ctx Context, goFn func(Context)) {
	s, ok := ctx.(spawner)
	if !ok {
		panic(panicOutsideWorkflow)
	}

	s.registerFiber(goFn)
}

// Sleep pauses the workflow for duration d.
// The timer is durable - if the workflow replays, Sleep returns immediately
// once the original timer would have fired.
func Sleep(ctx Context, d time.Duration) {
	NewTimer(ctx, d).Get(ctx)
}

// NewTimer schedules a timer that fires after duration d.
// Returns a Future that resolves to the time when the timer fired.
func NewTimer(ctx Context, d time.Duration) Future[time.Time] {
	ts, ok := ctx.(timerScheduler)
	if !ok {
		panic(panicOutsideWorkflow)
	}
	fireAt := ts.workflowTime().Add(d)
	scheduledID := ts.emitTimerScheduled(d, fireAt)
	return newTimerFuture(scheduledID)
}

// ErrTimerCancelled is returned by Future.Get when the timer was cancelled.
var ErrTimerCancelled = errors.New("derecho: timer cancelled")

// CancelTimer cancels a pending timer. The timer's Future.Get will return ErrTimerCancelled.
// Returns an error if the timer has already fired or been cancelled.
func CancelTimer(ctx Context, timer Future[time.Time]) error {
	tc, ok := ctx.(timerCanceller)
	if !ok {
		panic(panicOutsideWorkflow)
	}

	tf, ok := timer.(*timerFuture)
	if !ok {
		return errors.New("derecho: CancelTimer requires timer from NewTimer")
	}

	if tf.done {
		return errors.New("derecho: timer already completed")
	}

	tc.emitTimerCancelled(tf.scheduledID)
	return nil
}

// Now returns the workflow's current logical time.
// This is deterministic during replay - it returns the time from when
// the workflow originally executed, not wall-clock time.
func Now(ctx Context) time.Time {
	tp, ok := ctx.(timeProvider)
	if !ok {
		panic(panicOutsideWorkflow)
	}
	return tp.workflowTime()
}

// GetInfo returns metadata about the currently executing workflow.
func GetInfo(ctx Context) WorkflowInfo {
	ip, ok := ctx.(infoProvider)
	if !ok {
		panic(panicOutsideWorkflow)
	}
	return ip.info()
}

// AwaitWithTimeout blocks until checkFn returns true or timeout elapses.
// Returns true if condition was met, false if timed out.
func AwaitWithTimeout(ctx Context, timeout time.Duration, checkFn func() bool) bool {
	timer := NewTimer(ctx, timeout)
	timerDone := false

	Go(ctx, func(ctx Context) {
		timer.Get(ctx)
		timerDone = true
	})

	Await(ctx, func() bool {
		return checkFn() || timerDone
	})

	if checkFn() {
		CancelTimer(ctx, timer)
		return true
	}
	return false
}

// SideEffect executes fn and records its result.
// On replay, fn is not called - the recorded result is returned instead.
// Use for non-deterministic operations like generating UUIDs or reading wall-clock time.
func SideEffect[T any](ctx Context, fn func() T) T {
	rcp, ok := ctx.(interface {
		recorderProvider
		codecProvider
		internalFailer
	})
	if !ok {
		panic(panicOutsideWorkflow)
	}

	rec := rcp.recorder()
	codec := rcp.codec()

	recorded, err := rec.Record(journal.TypeSideEffectRecorded, func() journal.Event {
		result := fn()
		encoded, encErr := codec.Encode(result)
		if encErr != nil {
			// Inside callback - can't use failInternal, let fiber's panic handler catch it
			panic(encErr)
		}
		return journal.SideEffectRecorded{Result: encoded}
	})
	if err != nil {
		rcp.failInternal(err)
	}

	ev := recorded.(journal.SideEffectRecorded)
	var result T
	if decErr := codec.Decode(ev.Result, &result); decErr != nil {
		rcp.failInternal(decErr)
	}
	return result
}
