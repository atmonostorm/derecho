package derecho

import (
	"context"
	"errors"

	"github.com/atmonostorm/derecho/journal"
)

// Replay replays a workflow against recorded events.
// Returns NondeterminismError if workflow diverges from history.
// Works with complete and incomplete workflows.
func Replay(workflowFn any, events []journal.Event) error {
	if len(events) == 0 {
		return errors.New("derecho: events cannot be empty")
	}

	started, ok := events[0].(journal.WorkflowStarted)
	if !ok {
		return errors.New("derecho: events must start with WorkflowStarted")
	}

	info := WorkflowInfo{
		WorkflowID:   "replay",
		RunID:        "replay",
		WorkflowType: started.WorkflowType,
		StartTime:    started.StartedAt,
	}

	state := newExecutionState(info.WorkflowID, info.RunID, events, nil)
	wrapped := BindWorkflowInput(workflowFn, started.Args)
	sched := NewScheduler(state, wrapped, info)

	return sched.Advance(context.Background(), 0, started.StartedAt)
}

// ReplayFromStore loads events from a store and replays.
func ReplayFromStore(ctx context.Context, store journal.Store, workflowID, runID string, workflowFn any) error {
	events, err := store.Load(ctx, workflowID, runID)
	if err != nil {
		return err
	}
	return Replay(workflowFn, events)
}

// ReplayResult contains the result of a successful replay.
type ReplayResult struct {
	// NewEvents contains events generated beyond the recorded history.
	// Empty for complete workflows replayed deterministically.
	NewEvents []journal.Event

	// Complete is true if the workflow reached WorkflowCompleted or WorkflowFailed.
	Complete bool

	// Result is the JSON-encoded result if Complete is true and workflow succeeded.
	Result []byte

	// Error is the workflow error if Complete is true and workflow failed.
	Error error
}

// ReplayWithResult replays a workflow and returns detailed result information.
func ReplayWithResult(workflowFn any, events []journal.Event) (*ReplayResult, error) {
	if len(events) == 0 {
		return nil, errors.New("derecho: events cannot be empty")
	}

	started, ok := events[0].(journal.WorkflowStarted)
	if !ok {
		return nil, errors.New("derecho: events must start with WorkflowStarted")
	}

	info := WorkflowInfo{
		WorkflowID:   "replay",
		RunID:        "replay",
		WorkflowType: started.WorkflowType,
		StartTime:    started.StartedAt,
	}

	state := newExecutionState(info.WorkflowID, info.RunID, events, nil)
	wrapped := BindWorkflowInput(workflowFn, started.Args)
	sched := NewScheduler(state, wrapped, info)

	if err := sched.Advance(context.Background(), 0, started.StartedAt); err != nil {
		return nil, err
	}

	result := &ReplayResult{
		NewEvents: state.NewEvents(),
	}

	allEvents := append(events, state.NewEvents()...)
	for _, ev := range allEvents {
		switch e := ev.(type) {
		case journal.WorkflowCompleted:
			result.Complete = true
			result.Result = e.Result
		case journal.WorkflowFailed:
			result.Complete = true
			result.Error = e.Error
		}
	}

	return result, nil
}
