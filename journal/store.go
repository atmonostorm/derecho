package journal

import (
	"context"
	"errors"
	"time"
)

// Store is the durable backend for workflow executions.
// Handles event persistence and distributed task coordination.
// WorkflowStatus represents the current state of a workflow execution.
type WorkflowStatus int

const (
	WorkflowStatusUnknown WorkflowStatus = iota
	WorkflowStatusRunning
	WorkflowStatusCancelling
	WorkflowStatusCompleted
	WorkflowStatusFailed
	WorkflowStatusCancelled
	WorkflowStatusTerminated
	WorkflowStatusContinuedAsNew
)

func (s WorkflowStatus) String() string {
	switch s {
	case WorkflowStatusUnknown:
		return "unknown"
	case WorkflowStatusRunning:
		return "running"
	case WorkflowStatusCancelling:
		return "cancelling"
	case WorkflowStatusCompleted:
		return "completed"
	case WorkflowStatusFailed:
		return "failed"
	case WorkflowStatusCancelled:
		return "cancelled"
	case WorkflowStatusTerminated:
		return "terminated"
	case WorkflowStatusContinuedAsNew:
		return "continued_as_new"
	}
	return "unknown"
}

type Store interface {
	Load(ctx context.Context, workflowID, runID string) ([]Event, error)
	LoadFrom(ctx context.Context, workflowID, runID string, afterEventID int) ([]Event, error)

	Append(ctx context.Context, workflowID, runID string, events []Event, scheduledByEventID int) (eventIDs []int, err error)

	// WaitForWorkflowTasks returns pending workflow tasks for this worker.
	//
	// Affinity: The store tracks which worker "owns" each workflow. Workers cache
	// scheduler state in-memory, so routing tasks to the owning worker avoids
	// expensive full-history replays. Affinity is created when a worker first
	// processes a workflow and cleared when the workflow completes. When workers
	// restart with new IDs, their old affinities become orphaned; those workflows
	// are treated as unassigned and can be claimed by any worker.
	//
	// Task selection:
	//   1. All tasks for workflows with affinity to this worker (always returned)
	//   2. Up to maxNew tasks for unassigned workflows (creates new affinity)
	//   3. Tasks for workflows owned by other workers stay queued
	//
	// Blocking: If no tasks match (1) or (2), blocks until a matching task arrives
	// or ctx is cancelled. This provides backpressure when workers are at capacity.
	//
	// Atomicity: Appends WorkflowTaskStarted for each returned task. The event ID
	// is the next available (not necessarily ScheduledAt + 1, since completions
	// may have been appended after scheduling).
	WaitForWorkflowTasks(ctx context.Context, workerID string, maxNew int) ([]PendingWorkflowTask, error)
	// WaitForActivityTasks blocks until activity tasks are available, then claims
	// up to maxActivities. Low values (e.g., 1) prevent one worker from starving others.
	WaitForActivityTasks(ctx context.Context, workerID string, maxActivities int) ([]PendingActivityTask, error)
	GetTimersToFire(ctx context.Context, now time.Time) ([]PendingTimerTask, error)
	GetTimedOutActivities(ctx context.Context, now time.Time) ([]TimedOutActivity, error)
	WaitForCompletion(ctx context.Context, workflowID, runID string) (Event, error)

	// GetStatus returns the current workflow status without blocking.
	GetStatus(ctx context.Context, workflowID, runID string) (WorkflowStatus, error)

	RequeueForRetry(ctx context.Context, workflowID, runID string, scheduledAt int, info RequeueInfo) error

	// RecordHeartbeat updates heartbeat tracking for an in-progress activity.
	// Resets the heartbeat deadline and stores progress details for checkpointing.
	// No-op if the activity has already completed.
	RecordHeartbeat(ctx context.Context, workflowID, runID string, scheduledAt int, details []byte) error

	// SignalWorkflow delivers a signal to a running workflow.
	// Creates SignalReceived event and schedules a workflow task if needed.
	SignalWorkflow(ctx context.Context, workflowID, signalName string, payload []byte) error

	// CancelWorkflow requests cancellation of a running workflow.
	// Appends WorkflowCancelRequested event and schedules a workflow task.
	CancelWorkflow(ctx context.Context, workflowID, reason string) error

	// CreateWorkflow initializes a new workflow execution.
	// Generates run ID internally, appends WorkflowStarted and WorkflowTaskScheduled events.
	// Returns the generated run ID.
	// Returns ErrWorkflowAlreadyRunning if a workflow with this ID is already running.
	CreateWorkflow(ctx context.Context, workflowID, workflowType string, input []byte, startedAt time.Time) (runID string, err error)

	// ListWorkflows returns workflow executions matching the given options.
	// Results ordered by created_at DESC (newest first).
	ListWorkflows(ctx context.Context, opts ...ListWorkflowsOption) (*ListWorkflowsResult, error)
}

type RequeueInfo struct {
	Attempt     int
	NotBefore   time.Time
	LastFailure string
	RetryPolicy *RetryPolicyPayload
}

type PendingWorkflowTask struct {
	WorkflowID  string
	RunID       string
	ScheduledAt int
	StartedAt   time.Time // Set by Store when WorkflowTaskStarted is appended
}

type PendingActivityTask struct {
	WorkflowID   string
	RunID        string
	ActivityName string
	ScheduledAt  int
	Input        []byte
	Attempt      int
	RetryPolicy  *RetryPolicyPayload
	NotBefore    time.Time
	LastFailure  string

	// Timeout tracking
	TimeoutPolicy           *TimeoutPolicyPayload
	ScheduledTime           time.Time // Workflow time when ActivityScheduled was recorded
	StartedTime             time.Time // Zero until ActivityStarted; set by store
	ScheduleToStartDeadline time.Time // ScheduledTime + ScheduleToStartTimeout
	StartToCloseDeadline    time.Time // StartedTime + StartToCloseTimeout (computed on start)
	ScheduleToCloseDeadline time.Time // ScheduledTime + ScheduleToCloseTimeout

	// Heartbeat tracking
	HeartbeatTimeout  time.Duration // from TimeoutPolicy
	LastHeartbeatTime time.Time     // updated by RecordHeartbeat
	HeartbeatDeadline time.Time     // recomputed on each heartbeat
	HeartbeatDetails  []byte        // progress checkpoint for retry

	// Claimed is set when the activity is picked up by a worker.
	// Claimed activities remain tracked for timeout enforcement until completion.
	Claimed bool
}

type PendingTimerTask struct {
	WorkflowID  string
	RunID       string
	ScheduledAt int
	FireAt      time.Time
}

// TimeoutKind identifies which activity timeout was violated.
type TimeoutKind string

const (
	TimeoutKindScheduleToStart TimeoutKind = "schedule_to_start"
	TimeoutKindStartToClose    TimeoutKind = "start_to_close"
	TimeoutKindScheduleToClose TimeoutKind = "schedule_to_close"
	TimeoutKindHeartbeat       TimeoutKind = "heartbeat"
)

// TimedOutActivity represents an activity that exceeded its timeout.
type TimedOutActivity struct {
	WorkflowID  string
	RunID       string
	ScheduledAt int
	TimeoutKind TimeoutKind
}

func WorkflowKey(workflowID, runID string) string {
	return workflowID + "/" + runID
}

var (
	ErrAlreadyProcessed       = errors.New("derecho: workflow task already processed")
	ErrWorkflowAlreadyRunning = errors.New("derecho: workflow already running")
)

type WorkflowExecution struct {
	WorkflowID   string
	RunID        string
	WorkflowType string
	Status       WorkflowStatus
	CreatedAt    time.Time
	CompletedAt  *time.Time
	Input        []byte
	Error        *Error // populated for failed workflows
}

type ListWorkflowsResult struct {
	Executions []WorkflowExecution
	NextCursor string // empty if no more results
}

type ListWorkflowsOption func(*ListWorkflowsOptions)

type ListWorkflowsOptions struct {
	WorkflowType string
	Status       WorkflowStatus
	WorkflowID   string
	Limit        int
	Cursor       string
}

func (o *ListWorkflowsOptions) ApplyDefaults() {
	if o.Limit <= 0 {
		o.Limit = 100
	}
	if o.Limit > 1000 {
		o.Limit = 1000
	}
}

func WithWorkflowType(t string) ListWorkflowsOption {
	return func(o *ListWorkflowsOptions) { o.WorkflowType = t }
}

func WithStatus(s WorkflowStatus) ListWorkflowsOption {
	return func(o *ListWorkflowsOptions) { o.Status = s }
}

func WithWorkflowID(id string) ListWorkflowsOption {
	return func(o *ListWorkflowsOptions) { o.WorkflowID = id }
}

func WithLimit(n int) ListWorkflowsOption {
	return func(o *ListWorkflowsOptions) { o.Limit = n }
}

func WithCursor(c string) ListWorkflowsOption {
	return func(o *ListWorkflowsOptions) { o.Cursor = c }
}

type ListCursor struct {
	CreatedAt  time.Time
	WorkflowID string
	RunID      string
}

func EncodeCursor(createdAt time.Time, workflowID, runID string) string {
	return createdAt.Format(time.RFC3339Nano) + "|" + workflowID + "|" + runID
}

func DecodeCursor(cursor string) (ListCursor, error) {
	if cursor == "" {
		return ListCursor{}, nil
	}
	parts := splitCursor(cursor)
	if len(parts) != 3 {
		return ListCursor{}, errors.New("invalid cursor format")
	}
	t, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return ListCursor{}, errors.New("invalid cursor timestamp")
	}
	return ListCursor{
		CreatedAt:  t,
		WorkflowID: parts[1],
		RunID:      parts[2],
	}, nil
}

func splitCursor(s string) []string {
	var parts []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '|' {
			parts = append(parts, s[start:i])
			start = i + 1
		}
	}
	parts = append(parts, s[start:])
	return parts
}
