package derecho

import (
	"context"
	"runtime"
)

// Fibers are cooperative coroutines within a workflow. The scheduler wakes one fiber
// at a time via the wake channel; the fiber runs until it yields (Await, Sleep, Future.Get)
// or terminates. Panics and infrastructure errors are captured for clean shutdown.
type fiber struct {
	id   string
	wake chan chan bool // scheduler sends acker, fiber replies done status; serializes execution
	fn   func(Context)

	currentAcker chan bool
	panicChan    chan any   // receives panic value if fiber panicked; buffered size 1
	internalErr  chan error // receives infrastructure errors (e.g., NondeterminismError); buffered size 1
}

func newFiber(id string, fn func(Context)) *fiber {
	return &fiber{
		id:          id,
		wake:        make(chan chan bool),
		fn:          fn,
		panicChan:   make(chan any, 1),
		internalErr: make(chan error, 1),
	}
}

func (f *fiber) yield() {
	if f.currentAcker != nil {
		f.currentAcker <- false
	}
	acker, ok := <-f.wake
	if !ok {
		runtime.Goexit()
	}
	f.currentAcker = acker
}

func (f *fiber) wakeup(ctx context.Context) (alive bool, panicVal any, internalErr error, err error) {
	acker := make(chan bool)

	select {
	case f.wake <- acker:
	case p := <-f.panicChan:
		return false, p, nil, nil
	case ie := <-f.internalErr:
		return false, nil, ie, nil
	case <-ctx.Done():
		return false, nil, nil, ctx.Err()
	}

	select {
	case done := <-acker:
		return !done, nil, nil, nil
	case p := <-f.panicChan:
		return false, p, nil, nil
	case ie := <-f.internalErr:
		return false, nil, ie, nil
	case <-ctx.Done():
		return false, nil, nil, ctx.Err()
	}
}

func (f *fiber) launch(ctx *workflowContext) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				f.panicChan <- r
			}
		}()
		acker, ok := <-f.wake
		if !ok {
			return
		}
		f.currentAcker = acker
		f.fn(ctx)
		f.currentAcker <- true
	}()
}
