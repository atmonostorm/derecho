package journal

import (
	"fmt"
	"time"
)

// Event type constants.
const (
	TypeWorkflowStarted   = "workflow_started"
	TypeWorkflowFailed    = "workflow_failed"
	TypeWorkflowCompleted = "workflow_completed"

	TypeWorkflowTaskScheduled = "workflow_task_scheduled"
	TypeWorkflowTaskStarted   = "workflow_task_started"
	TypeWorkflowTaskCompleted = "workflow_task_completed"

	TypeActivityScheduled = "activity_scheduled"
	TypeActivityStarted   = "activity_started"
	TypeActivityCompleted = "activity_completed"
	TypeActivityFailed    = "activity_failed"

	TypeTimerScheduled = "timer_scheduled"
	TypeTimerFired     = "timer_fired"
	TypeTimerCancelled = "timer_cancelled"

	TypeSideEffectRecorded = "side_effect_recorded"

	TypeChildWorkflowScheduled = "child_workflow_scheduled"
	TypeChildWorkflowStarted   = "child_workflow_started"
	TypeChildWorkflowCompleted = "child_workflow_completed"
	TypeChildWorkflowFailed    = "child_workflow_failed"

	TypeSignalReceived          = "signal_received"
	TypeSignalExternalScheduled = "signal_external_scheduled"

	TypeWorkflowContinuedAsNew = "workflow_continued_as_new"
)

// Event is the interface implemented by all journal events.
type Event interface {
	Base() BaseEvent
	EventType() string
	WithID(id int) Event
	WithScheduledByID(id int) Event
}

// BaseEvent contains fields common to all events.
type BaseEvent struct {
	ID            int `json:"id"`
	ScheduledByID int `json:"scheduled_by_id,omitempty"`
}

// Base returns the base event (itself).
func (e BaseEvent) Base() BaseEvent { return e }

// WorkflowStarted marks the beginning of a workflow execution.
type WorkflowStarted struct {
	BaseEvent
	WorkflowType string    `json:"workflow_type"`
	Args         []byte    `json:"args"`
	StartedAt    time.Time `json:"started_at"`
}

func (WorkflowStarted) EventType() string { return TypeWorkflowStarted }

func (e WorkflowStarted) WithID(id int) Event {
	e.ID = id
	return e
}

func (e WorkflowStarted) WithScheduledByID(id int) Event {
	e.ScheduledByID = id
	return e
}

// WorkflowCompleted marks successful workflow completion.
type WorkflowCompleted struct {
	BaseEvent
	Result []byte `json:"result"`
}

func (WorkflowCompleted) EventType() string { return TypeWorkflowCompleted }

func (e WorkflowCompleted) WithID(id int) Event {
	e.ID = id
	return e
}

func (e WorkflowCompleted) WithScheduledByID(id int) Event {
	e.ScheduledByID = id
	return e
}

// WorkflowFailed marks workflow failure.
type WorkflowFailed struct {
	BaseEvent
	Error *Error `json:"error"`
}

func (WorkflowFailed) EventType() string { return TypeWorkflowFailed }

func (e WorkflowFailed) WithID(id int) Event {
	e.ID = id
	return e
}

func (e WorkflowFailed) WithScheduledByID(id int) Event {
	e.ScheduledByID = id
	return e
}

// RetryPolicyPayload is the serializable form of retry configuration.
type RetryPolicyPayload struct {
	InitialInterval    time.Duration `json:"initial_interval"`
	BackoffCoefficient float64       `json:"backoff_coefficient"`
	MaxInterval        time.Duration `json:"max_interval"`
	MaxAttempts        int           `json:"max_attempts"`
	NonRetryableErrors []ErrorKind   `json:"non_retryable_errors,omitempty"`
}

// TimeoutPolicyPayload is the serializable form of activity timeout configuration.
type TimeoutPolicyPayload struct {
	ScheduleToStartTimeout time.Duration `json:"schedule_to_start_timeout,omitempty"`
	StartToCloseTimeout    time.Duration `json:"start_to_close_timeout,omitempty"`
	ScheduleToCloseTimeout time.Duration `json:"schedule_to_close_timeout,omitempty"`
	HeartbeatTimeout       time.Duration `json:"heartbeat_timeout,omitempty"`
}

// ActivityScheduled marks an activity being scheduled.
type ActivityScheduled struct {
	BaseEvent
	Name          string                `json:"name"`
	Input         []byte                `json:"input"`
	RetryPolicy   *RetryPolicyPayload   `json:"retry_policy,omitempty"`
	TimeoutPolicy *TimeoutPolicyPayload `json:"timeout_policy,omitempty"`
	ScheduledAt   time.Time             `json:"scheduled_at"`
}

func (ActivityScheduled) EventType() string { return TypeActivityScheduled }

func (e ActivityScheduled) WithID(id int) Event {
	e.ID = id
	return e
}

func (e ActivityScheduled) WithScheduledByID(id int) Event {
	e.ScheduledByID = id
	return e
}

// ActivityStarted marks an activity beginning execution.
type ActivityStarted struct {
	BaseEvent
	WorkerID  string    `json:"worker_id"`
	StartedAt time.Time `json:"started_at"`
}

func (ActivityStarted) EventType() string { return TypeActivityStarted }

func (e ActivityStarted) WithID(id int) Event {
	e.ID = id
	return e
}

func (e ActivityStarted) WithScheduledByID(id int) Event {
	e.ScheduledByID = id
	return e
}

// ActivityCompleted marks successful activity completion.
type ActivityCompleted struct {
	BaseEvent
	Result []byte `json:"result"`
}

func (ActivityCompleted) EventType() string { return TypeActivityCompleted }

func (e ActivityCompleted) WithID(id int) Event {
	e.ID = id
	return e
}

func (e ActivityCompleted) WithScheduledByID(id int) Event {
	e.ScheduledByID = id
	return e
}

// ActivityFailed marks activity failure.
type ActivityFailed struct {
	BaseEvent
	Error *Error `json:"error"`
}

func (ActivityFailed) EventType() string { return TypeActivityFailed }

func (e ActivityFailed) WithID(id int) Event {
	e.ID = id
	return e
}

func (e ActivityFailed) WithScheduledByID(id int) Event {
	e.ScheduledByID = id
	return e
}

// WorkflowTaskScheduled marks a workflow task being queued.
type WorkflowTaskScheduled struct {
	BaseEvent
}

func (WorkflowTaskScheduled) EventType() string { return TypeWorkflowTaskScheduled }

func (e WorkflowTaskScheduled) WithID(id int) Event {
	e.ID = id
	return e
}

func (e WorkflowTaskScheduled) WithScheduledByID(id int) Event {
	e.ScheduledByID = id
	return e
}

// WorkflowTaskStarted marks a workflow task beginning execution.
type WorkflowTaskStarted struct {
	BaseEvent
	WorkerID  string    `json:"worker_id"`
	StartedAt time.Time `json:"started_at"`
}

func (WorkflowTaskStarted) EventType() string { return TypeWorkflowTaskStarted }

func (e WorkflowTaskStarted) WithID(id int) Event {
	e.ID = id
	return e
}

func (e WorkflowTaskStarted) WithScheduledByID(id int) Event {
	e.ScheduledByID = id
	return e
}

// WorkflowTaskCompleted marks a workflow task finishing.
type WorkflowTaskCompleted struct {
	BaseEvent
}

func (WorkflowTaskCompleted) EventType() string { return TypeWorkflowTaskCompleted }

func (e WorkflowTaskCompleted) WithID(id int) Event {
	e.ID = id
	return e
}

func (e WorkflowTaskCompleted) WithScheduledByID(id int) Event {
	e.ScheduledByID = id
	return e
}

// TimerScheduled marks a timer being set.
type TimerScheduled struct {
	BaseEvent
	Duration time.Duration `json:"duration"`
	FireAt   time.Time     `json:"fire_at"`
}

func (TimerScheduled) EventType() string { return TypeTimerScheduled }

func (e TimerScheduled) WithID(id int) Event {
	e.ID = id
	return e
}

func (e TimerScheduled) WithScheduledByID(id int) Event {
	e.ScheduledByID = id
	return e
}

// TimerFired marks a timer expiring.
type TimerFired struct {
	BaseEvent
	FiredAt time.Time `json:"fired_at"`
}

func (TimerFired) EventType() string { return TypeTimerFired }

func (e TimerFired) WithID(id int) Event {
	e.ID = id
	return e
}

func (e TimerFired) WithScheduledByID(id int) Event {
	e.ScheduledByID = id
	return e
}

// TimerCancelled marks a timer being cancelled before firing.
type TimerCancelled struct {
	BaseEvent
}

func (TimerCancelled) EventType() string { return TypeTimerCancelled }

func (e TimerCancelled) WithID(id int) Event {
	e.ID = id
	return e
}

func (e TimerCancelled) WithScheduledByID(id int) Event {
	e.ScheduledByID = id
	return e
}

// SideEffectRecorded marks a recorded side effect result.
type SideEffectRecorded struct {
	BaseEvent
	Result []byte `json:"result"`
}

func (SideEffectRecorded) EventType() string { return TypeSideEffectRecorded }

func (e SideEffectRecorded) WithID(id int) Event {
	e.ID = id
	return e
}

func (e SideEffectRecorded) WithScheduledByID(id int) Event {
	e.ScheduledByID = id
	return e
}

// ParentClosePolicy controls what happens to a child workflow when its parent terminates.
type ParentClosePolicy int

const (
	// ParentClosePolicyTerminate terminates the child when the parent completes or fails.
	ParentClosePolicyTerminate ParentClosePolicy = iota
	// ParentClosePolicyAbandon allows the child to continue running independently.
	ParentClosePolicyAbandon
)

// ChildWorkflowScheduled marks a child workflow being spawned.
type ChildWorkflowScheduled struct {
	BaseEvent
	WorkflowType      string            `json:"workflow_type"`
	WorkflowID        string            `json:"workflow_id"`
	Input             []byte            `json:"input"`
	ParentWorkflowID  string            `json:"parent_workflow_id"`
	ParentRunID       string            `json:"parent_run_id"`
	ParentClosePolicy ParentClosePolicy `json:"parent_close_policy"`
}

func (ChildWorkflowScheduled) EventType() string { return TypeChildWorkflowScheduled }

func (e ChildWorkflowScheduled) WithID(id int) Event {
	e.ID = id
	return e
}

func (e ChildWorkflowScheduled) WithScheduledByID(id int) Event {
	e.ScheduledByID = id
	return e
}

// ChildWorkflowStarted marks a child workflow beginning execution.
type ChildWorkflowStarted struct {
	BaseEvent
	ChildRunID string    `json:"child_run_id"`
	StartedAt  time.Time `json:"started_at"`
}

func (ChildWorkflowStarted) EventType() string { return TypeChildWorkflowStarted }

func (e ChildWorkflowStarted) WithID(id int) Event {
	e.ID = id
	return e
}

func (e ChildWorkflowStarted) WithScheduledByID(id int) Event {
	e.ScheduledByID = id
	return e
}

// ChildWorkflowCompleted marks successful child workflow completion.
type ChildWorkflowCompleted struct {
	BaseEvent
	ChildWorkflowID string `json:"child_workflow_id"`
	ChildRunID      string `json:"child_run_id"`
	Result          []byte `json:"result"`
}

func (ChildWorkflowCompleted) EventType() string { return TypeChildWorkflowCompleted }

func (e ChildWorkflowCompleted) WithID(id int) Event {
	e.ID = id
	return e
}

func (e ChildWorkflowCompleted) WithScheduledByID(id int) Event {
	e.ScheduledByID = id
	return e
}

// ChildWorkflowFailed marks child workflow failure.
type ChildWorkflowFailed struct {
	BaseEvent
	ChildWorkflowID string `json:"child_workflow_id"`
	ChildRunID      string `json:"child_run_id"`
	Error           *Error `json:"error"`
}

func (ChildWorkflowFailed) EventType() string { return TypeChildWorkflowFailed }

func (e ChildWorkflowFailed) WithID(id int) Event {
	e.ID = id
	return e
}

func (e ChildWorkflowFailed) WithScheduledByID(id int) Event {
	e.ScheduledByID = id
	return e
}

// ErrorKind categorizes errors for retry decisions.
type ErrorKind string

const (
	ErrorKindApplication    ErrorKind = "application"
	ErrorKindTimeout        ErrorKind = "timeout"
	ErrorKindCancelled      ErrorKind = "cancelled"
	ErrorKindNondeterminism ErrorKind = "nondeterminism"
	ErrorKindPanic          ErrorKind = "panic"
)

// Error represents a serializable workflow or activity error.
type Error struct {
	Kind           ErrorKind         `json:"kind"`
	Message        string            `json:"message"`
	Details        map[string]string `json:"details,omitempty"`
	NonRetryable   bool              `json:"non_retryable,omitempty"`
	NextRetryDelay time.Duration     `json:"next_retry_delay,omitempty"`
}

func (e *Error) Error() string {
	return e.Message
}

func NewError(kind ErrorKind, message string) *Error {
	return &Error{Kind: kind, Message: message}
}

func NewErrorf(kind ErrorKind, format string, args ...any) *Error {
	return &Error{Kind: kind, Message: fmt.Sprintf(format, args...)}
}

func (e *Error) WithDetail(key, value string) *Error {
	if e.Details == nil {
		e.Details = make(map[string]string)
	}
	e.Details[key] = value
	return e
}

func ToError(err error) *Error {
	if err == nil {
		return nil
	}
	if wErr, ok := err.(*Error); ok {
		return wErr
	}
	return NewError(ErrorKindApplication, err.Error())
}

// SignalReceived marks a signal arriving at a workflow.
type SignalReceived struct {
	BaseEvent
	SignalName string    `json:"signal_name"`
	Payload    []byte    `json:"payload"`
	SentAt     time.Time `json:"sent_at"`
}

func (SignalReceived) EventType() string { return TypeSignalReceived }

func (e SignalReceived) WithID(id int) Event {
	e.ID = id
	return e
}

func (e SignalReceived) WithScheduledByID(id int) Event {
	e.ScheduledByID = id
	return e
}

// SignalExternalScheduled marks a workflow sending a signal to another workflow.
type SignalExternalScheduled struct {
	BaseEvent
	TargetWorkflowID string `json:"target_workflow_id"`
	SignalName       string `json:"signal_name"`
	Payload          []byte `json:"payload"`
}

func (SignalExternalScheduled) EventType() string { return TypeSignalExternalScheduled }

func (e SignalExternalScheduled) WithID(id int) Event {
	e.ID = id
	return e
}

func (e SignalExternalScheduled) WithScheduledByID(id int) Event {
	e.ScheduledByID = id
	return e
}

// WorkflowContinuedAsNew marks a workflow restarting with new input.
type WorkflowContinuedAsNew struct {
	BaseEvent
	NewInput []byte `json:"new_input"`
}

func (WorkflowContinuedAsNew) EventType() string { return TypeWorkflowContinuedAsNew }

func (e WorkflowContinuedAsNew) WithID(id int) Event {
	e.ID = id
	return e
}

func (e WorkflowContinuedAsNew) WithScheduledByID(id int) Event {
	e.ScheduledByID = id
	return e
}
