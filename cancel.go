package derecho

import (
	"errors"
	"time"
)

var ErrCancelled = errors.New("derecho: workflow cancelled")

type CancelInfo struct {
	Reason      string
	RequestedAt time.Time
}

func Cancelled(ctx Context) Future[CancelInfo] {
	provider, ok := ctx.(cancelFutureProvider)
	if !ok {
		panic(panicOutsideWorkflow)
	}
	return provider.cancelFuture()
}

type cancelFutureProvider interface {
	cancelFuture() Future[CancelInfo]
}

type cancelFutureImpl struct {
	info     CancelInfo
	resolved bool
}

func (f *cancelFutureImpl) Get(ctx Context) (CancelInfo, error) {
	if f.resolved {
		return f.info, nil
	}

	yielder, ok := ctx.(yielder)
	if !ok {
		panic(panicOutsideWorkflow)
	}

	for !f.resolved {
		yielder.Yield()
	}

	return f.info, nil
}

func (f *cancelFutureImpl) resolve(info CancelInfo) {
	f.info = info
	f.resolved = true
}

func (f *cancelFutureImpl) isResolved() bool {
	return f.resolved
}
