package journal

import (
	"context"
	"time"
)

// Inspector provides read-only access to workflow state for inspection/debugging.
// Store implementations can optionally satisfy this interface.
type Inspector interface {
	ListWorkflows(ctx context.Context, opts ...ListWorkflowsOption) (*ListWorkflowsResult, error)
	CountWorkflows(ctx context.Context, opts ...ListWorkflowsOption) (int, error)
	LoadEvents(ctx context.Context, workflowID, runID string) ([]Event, error)
	GetWorkflowExecution(ctx context.Context, workflowID, runID string) (*WorkflowExecution, error)
	GetPendingWorkflowTasks(ctx context.Context) ([]InspectorWorkflowTask, error)
	GetPendingActivities(ctx context.Context) ([]InspectorActivity, error)
	GetPendingTimers(ctx context.Context) ([]InspectorTimer, error)
	Close() error
}

// InspectorWorkflowTask is a pending workflow task for inspection.
type InspectorWorkflowTask struct {
	WorkflowID  string
	RunID       string
	ScheduledAt int
	CreatedAt   time.Time
	WorkerID    string // from affinity, empty if unassigned
}

// InspectorActivity is a pending activity for inspection.
type InspectorActivity struct {
	WorkflowID   string
	RunID        string
	ScheduledAt  int
	ActivityName string
	Attempt      int
	Claimed      bool
	ClaimedBy    string
	NotBefore    *time.Time
	CreatedAt    time.Time
}

// InspectorTimer is a pending timer for inspection.
type InspectorTimer struct {
	WorkflowID  string
	RunID       string
	ScheduledAt int
	FireAt      time.Time
}
