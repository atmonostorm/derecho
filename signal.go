package derecho

import (
	"github.com/atmonostorm/derecho/journal"
)

// SignalChannel provides typed signal reception within a workflow.
// Signals arrive from external clients or other workflows.
type SignalChannel[T any] struct {
	name string
}

// GetSignalChannel returns a channel for receiving signals with the given name.
func GetSignalChannel[T any](ctx Context, name string) *SignalChannel[T] {
	_, ok := ctx.(yielder)
	if !ok {
		panic(panicOutsideWorkflow)
	}
	return &SignalChannel[T]{name: name}
}

// Receive blocks until a signal arrives, returning (value, true).
// During replay, returns signals in the same order they were originally received.
func (ch *SignalChannel[T]) Receive(ctx Context) (T, bool) {
	sq, ok := ctx.(signalQuerier)
	if !ok {
		panic(panicOutsideWorkflow)
	}

	var result T

	Await(ctx, func() bool {
		sig := sq.nextSignal(ch.name)
		if sig == nil {
			return false
		}

		codec := sq.codec()
		if err := codec.Decode(sig.Payload, &result); err != nil {
			sq.failInternal(err)
		}
		return true
	})

	return result, true
}

// TryReceive returns a signal if one is available, without blocking.
// Returns (value, true) if a signal was available, (zero, false) otherwise.
func (ch *SignalChannel[T]) TryReceive(ctx Context) (T, bool) {
	sq, ok := ctx.(signalQuerier)
	if !ok {
		panic(panicOutsideWorkflow)
	}

	var result T
	sig := sq.nextSignal(ch.name)
	if sig == nil {
		return result, false
	}

	codec := sq.codec()
	if err := codec.Decode(sig.Payload, &result); err != nil {
		sq.failInternal(err)
	}
	return result, true
}

// ReceiveFuture returns a Future for use with Selector.
func (ch *SignalChannel[T]) ReceiveFuture() Future[T] {
	return &signalFuture[T]{ch: ch}
}

type signalFuture[T any] struct {
	ch     *SignalChannel[T]
	result T
	done   bool
}

// Get implements Future[T].
func (f *signalFuture[T]) Get(ctx Context) (T, error) {
	if f.done {
		return f.result, nil
	}

	sq, ok := ctx.(signalQuerier)
	if !ok {
		panic(panicOutsideWorkflow)
	}

	Await(ctx, func() bool {
		sig := sq.nextSignal(f.ch.name)
		if sig == nil {
			return false
		}

		codec := sq.codec()
		if err := codec.Decode(sig.Payload, &f.result); err != nil {
			sq.failInternal(err)
		}
		f.done = true
		return true
	})

	return f.result, nil
}

// SignalExternalWorkflow sends a signal to another running workflow.
// This is fire-and-forget - no confirmation of delivery is provided.
func SignalExternalWorkflow(ctx Context, targetWorkflowID, signalName string, payload any) {
	ses, ok := ctx.(signalExternalScheduler)
	if !ok {
		panic(panicOutsideWorkflow)
	}
	ses.emitSignalExternalScheduled(targetWorkflowID, signalName, payload)
}

type signalQuerier interface {
	nextSignal(name string) *journal.SignalReceived
	codec() Codec
	failInternal(err error)
}

type signalExternalScheduler interface {
	emitSignalExternalScheduled(targetWorkflowID, signalName string, payload any)
}
