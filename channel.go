package derecho

import "errors"

// ErrChannelClosed is returned by ReceiveFuture.Get when the channel was closed.
var ErrChannelClosed = errors.New("derecho: channel closed")

// Channel provides typed, fiber-safe communication within a workflow.
type Channel[T any] struct {
	buffer      []T
	cap         int
	sendWaiters []*channelWaiter[T]
	recvWaiters []*channelWaiter[T]
	closed      bool
}

type channelWaiter[T any] struct {
	value T
	ready bool
	ok    bool
}

// NewChannel creates an unbuffered channel.
func NewChannel[T any](ctx Context) *Channel[T] {
	_, ok := ctx.(yielder)
	if !ok {
		panic(panicOutsideWorkflow)
	}
	return &Channel[T]{cap: 0}
}

// NewBufferedChannel creates a buffered channel with the given capacity.
func NewBufferedChannel[T any](ctx Context, capacity int) *Channel[T] {
	_, ok := ctx.(yielder)
	if !ok {
		panic(panicOutsideWorkflow)
	}
	if capacity < 0 {
		capacity = 0
	}
	return &Channel[T]{
		buffer: make([]T, 0, capacity),
		cap:    capacity,
	}
}

// Send sends a value on the channel. Panics if closed.
func (ch *Channel[T]) Send(ctx Context, value T) {
	ps, ok := ctx.(progressSignaler)
	if !ok {
		panic(panicOutsideWorkflow)
	}

	if ch.closed {
		panic("derecho: send on closed channel")
	}

	if len(ch.recvWaiters) > 0 {
		recv := ch.recvWaiters[0]
		ch.recvWaiters = ch.recvWaiters[1:]
		recv.value = value
		recv.ok = true
		recv.ready = true
		ps.signalProgress()
		return
	}

	if ch.cap > 0 && len(ch.buffer) < ch.cap {
		ch.buffer = append(ch.buffer, value)
		return
	}

	waiter := &channelWaiter[T]{value: value}
	ch.sendWaiters = append(ch.sendWaiters, waiter)

	Await(ctx, func() bool {
		return waiter.ready
	})
}

// Receive receives a value. Returns (zero, false) if closed.
func (ch *Channel[T]) Receive(ctx Context) (T, bool) {
	ps, ok := ctx.(progressSignaler)
	if !ok {
		panic(panicOutsideWorkflow)
	}

	if len(ch.sendWaiters) > 0 {
		sender := ch.sendWaiters[0]
		ch.sendWaiters = ch.sendWaiters[1:]
		value := sender.value
		sender.ready = true
		ps.signalProgress()
		return value, true
	}

	if len(ch.buffer) > 0 {
		value := ch.buffer[0]
		ch.buffer = ch.buffer[1:]
		return value, true
	}

	// Closed channels can still have buffered values - check after draining buffer.
	if ch.closed {
		var zero T
		return zero, false
	}

	waiter := &channelWaiter[T]{}
	ch.recvWaiters = append(ch.recvWaiters, waiter)

	Await(ctx, func() bool {
		return waiter.ready
	})

	return waiter.value, waiter.ok
}

// Close closes the channel.
func (ch *Channel[T]) Close(ctx Context) {
	ps, ok := ctx.(progressSignaler)
	if !ok {
		panic(panicOutsideWorkflow)
	}

	if ch.closed {
		panic("derecho: close of closed channel")
	}
	ch.closed = true

	hadWaiters := len(ch.recvWaiters) > 0
	for _, waiter := range ch.recvWaiters {
		waiter.ok = false
		waiter.ready = true
	}
	ch.recvWaiters = nil

	if hadWaiters {
		ps.signalProgress()
	}
}

// ReceiveFuture adapts a channel receive for Selector integration.
type ReceiveFuture[T any] struct {
	ch     *Channel[T]
	result T
	ok     bool
	done   bool
}

// ReceiveFuture returns a Future for use with Selector.
func (ch *Channel[T]) ReceiveFuture() Future[T] {
	return &ReceiveFuture[T]{ch: ch}
}

// Get implements Future[T].
func (f *ReceiveFuture[T]) Get(ctx Context) (T, error) {
	if f.done {
		if !f.ok {
			return f.result, ErrChannelClosed
		}
		return f.result, nil
	}

	ps, ok := ctx.(progressSignaler)
	if !ok {
		panic(panicOutsideWorkflow)
	}

	ch := f.ch

	Await(ctx, func() bool {
		if len(ch.sendWaiters) > 0 {
			sender := ch.sendWaiters[0]
			ch.sendWaiters = ch.sendWaiters[1:]
			f.result = sender.value
			f.ok = true
			f.done = true
			sender.ready = true
			ps.signalProgress()
			return true
		}

		if len(ch.buffer) > 0 {
			f.result = ch.buffer[0]
			ch.buffer = ch.buffer[1:]
			f.ok = true
			f.done = true
			return true
		}

		if ch.closed {
			f.ok = false
			f.done = true
			return true
		}

		return false
	})

	if !f.ok {
		return f.result, ErrChannelClosed
	}
	return f.result, nil
}
