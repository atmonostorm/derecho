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
	WorkflowStatusCompleted
	WorkflowStatusFailed
	WorkflowStatusTerminated
	WorkflowStatusContinuedAsNew
)

func (s WorkflowStatus) String() string {
	switch s {
	case WorkflowStatusUnknown:
		return "unknown"
	case WorkflowStatusRunning:
		return "running"
	case WorkflowStatusCompleted:
		return "completed"
	case WorkflowStatusFailed:
		return "failed"
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
	WaitForActivityTasks(ctx context.Context, workerID string) ([]PendingActivityTask, error)
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

	// CreateWorkflow initializes a new workflow execution.
	// Generates run ID internally, appends WorkflowStarted and WorkflowTaskScheduled events.
	// Returns the generated run ID.
	CreateWorkflow(ctx context.Context, workflowID, workflowType string, input []byte, startedAt time.Time) (runID string, err error)
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

var ErrAlreadyProcessed = errors.New("derecho: workflow task already processed")
