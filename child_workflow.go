package derecho

import (
	"fmt"

	"github.com/atmonostorm/derecho/journal"
)

type childWorkflowEventEmitter interface {
	emitChildWorkflowScheduled(workflowType, workflowID string, input []byte, closePolicy journal.ParentClosePolicy) int
}

type childWorkflowOptions struct {
	closePolicy journal.ParentClosePolicy
}

// ChildWorkflowOption configures child workflow execution.
type ChildWorkflowOption func(*childWorkflowOptions)

// WithClosePolicy sets the parent close policy for the child workflow.
func WithClosePolicy(policy journal.ParentClosePolicy) ChildWorkflowOption {
	return func(o *childWorkflowOptions) { o.closePolicy = policy }
}

type ChildWorkflowRef[I, O any] struct {
	name string
}

func NewChildWorkflowRef[I, O any](name string) ChildWorkflowRef[I, O] {
	return ChildWorkflowRef[I, O]{name: name}
}

func (r ChildWorkflowRef[I, O]) Name() string { return r.name }

// Execute schedules a child workflow and returns a future for its result.
// The workflowID must be provided explicitly for deduplication.
func (r ChildWorkflowRef[I, O]) Execute(ctx Context, workflowID string, input I, opts ...ChildWorkflowOption) Future[O] {
	wctx, ok := ctx.(interface {
		childWorkflowEventEmitter
		codecProvider
	})
	if !ok {
		panic(panicOutsideWorkflow)
	}
	codec := wctx.codec()

	var options childWorkflowOptions
	for _, opt := range opts {
		opt(&options)
	}

	inputJSON, err := codec.Encode(input)
	if err != nil {
		return newFailedFuture[O](fmt.Errorf("encode child workflow input for %q: %w", r.name, err))
	}

	scheduledID := wctx.emitChildWorkflowScheduled(r.name, workflowID, inputJSON, options.closePolicy)
	return newChildWorkflowFuture[O](scheduledID, codec)
}

type childWorkflowFuture[T any] struct {
	scheduledID int
	codec       Codec
	result      T
	err         error
	done        bool
}

func newChildWorkflowFuture[T any](scheduledID int, codec Codec) Future[T] {
	return &childWorkflowFuture[T]{scheduledID: scheduledID, codec: codec}
}

func (f *childWorkflowFuture[T]) Get(ctx Context) (T, error) {
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
			case journal.ChildWorkflowCompleted:
				if err := f.codec.Decode(e.Result, &f.result); err != nil {
					f.err = err
				}
				f.done = true
				return true
			case journal.ChildWorkflowFailed:
				f.err = e.Error
				f.done = true
				return true
			}
		}
		return false
	})

	return f.result, f.err
}
