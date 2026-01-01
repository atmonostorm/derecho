package derecho

import (
	"context"
	"errors"
	"fmt"

	"github.com/atmonostorm/derecho/journal"
)

// ErrNotCompleted is returned by Get with NonBlocking option when the workflow hasn't finished.
var ErrNotCompleted = errors.New("derecho: workflow not completed")

type Client interface {
	StartWorkflow(ctx context.Context, workflowType, workflowID string, input any) (Run, error)
	GetWorkflow(ctx context.Context, workflowID, runID string) (Run, error)
	SignalWorkflow(ctx context.Context, workflowID, signalName string, payload any) error
	CancelWorkflow(ctx context.Context, workflowID, reason string) error
	ListWorkflows(ctx context.Context, opts ...journal.ListWorkflowsOption) (*journal.ListWorkflowsResult, error)
}

type Run interface {
	ID() string
	RunID() string
	Get(ctx context.Context, result any, opts ...GetOption) error
}

type getOptions struct {
	nonBlocking bool
}

// GetOption configures Get behavior.
type GetOption func(*getOptions)

// NonBlocking returns ErrNotCompleted instead of blocking if the workflow hasn't finished.
func NonBlocking() GetOption {
	return func(o *getOptions) { o.nonBlocking = true }
}

type client struct {
	store journal.Store
	codec Codec
	clock Clock
}

func (c *client) StartWorkflow(ctx context.Context, workflowType, workflowID string, input any) (Run, error) {
	inputJSON, err := c.codec.Encode(input)
	if err != nil {
		return nil, err
	}

	runID, err := c.store.CreateWorkflow(ctx, workflowID, workflowType, inputJSON, c.clock.Now())
	if err != nil {
		return nil, err
	}

	return &workflowRun{
		store:      c.store,
		codec:      c.codec,
		workflowID: workflowID,
		runID:      runID,
	}, nil
}

func (c *client) GetWorkflow(ctx context.Context, workflowID, runID string) (Run, error) {
	return &workflowRun{
		store:      c.store,
		codec:      c.codec,
		workflowID: workflowID,
		runID:      runID,
	}, nil
}

func (c *client) SignalWorkflow(ctx context.Context, workflowID, signalName string, payload any) error {
	encoded, err := c.codec.Encode(payload)
	if err != nil {
		return err
	}
	return c.store.SignalWorkflow(ctx, workflowID, signalName, encoded)
}

func (c *client) CancelWorkflow(ctx context.Context, workflowID, reason string) error {
	return c.store.CancelWorkflow(ctx, workflowID, reason)
}

func (c *client) ListWorkflows(ctx context.Context, opts ...journal.ListWorkflowsOption) (*journal.ListWorkflowsResult, error) {
	return c.store.ListWorkflows(ctx, opts...)
}

type workflowRun struct {
	store      journal.Store
	codec      Codec
	workflowID string
	runID      string
}

func (r *workflowRun) ID() string    { return r.workflowID }
func (r *workflowRun) RunID() string { return r.runID }

func (r *workflowRun) Get(ctx context.Context, result any, opts ...GetOption) error {
	var options getOptions
	for _, opt := range opts {
		opt(&options)
	}

	if options.nonBlocking {
		status, err := r.store.GetStatus(ctx, r.workflowID, r.runID)
		if err != nil {
			return err
		}
		if status == journal.WorkflowStatusRunning || status == journal.WorkflowStatusUnknown {
			return ErrNotCompleted
		}
	}

	ev, err := r.store.WaitForCompletion(ctx, r.workflowID, r.runID)
	if err != nil {
		return err
	}

	switch e := ev.(type) {
	case journal.WorkflowCompleted:
		return r.codec.Decode(e.Result, result)
	case journal.WorkflowFailed:
		return e.Error
	case journal.WorkflowCancelled:
		return ErrCancelled
	default:
		return fmt.Errorf("derecho: unexpected completion event type: %T", ev)
	}
}
