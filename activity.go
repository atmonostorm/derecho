package derecho

import (
	"context"
	"fmt"
	"time"

	"github.com/atmonostorm/derecho/journal"
)

type activityEventEmitter interface {
	emitActivityScheduled(name string, input []byte, retryPolicy *journal.RetryPolicyPayload, timeoutPolicy *journal.TimeoutPolicyPayload) (id int, newEventIndex int)
}

type activityOptions struct {
	retryPolicy   *RetryPolicy
	timeoutPolicy *TimeoutPolicy
}

// ActivityOption configures activity execution.
type ActivityOption func(*activityOptions)

// WithRetry sets the retry policy for the activity.
func WithRetry(policy RetryPolicy) ActivityOption {
	return func(o *activityOptions) { o.retryPolicy = &policy }
}

// WithNoRetry disables retries for the activity.
func WithNoRetry() ActivityOption {
	p := NoRetryPolicy()
	return func(o *activityOptions) { o.retryPolicy = &p }
}

// TimeoutPolicy configures activity timeout behavior.
type TimeoutPolicy struct {
	ScheduleToStartTimeout time.Duration
	StartToCloseTimeout    time.Duration
	ScheduleToCloseTimeout time.Duration
	HeartbeatTimeout       time.Duration
}

// ToPayload converts the policy to a serializable payload.
func (p *TimeoutPolicy) ToPayload() *journal.TimeoutPolicyPayload {
	if p == nil {
		return nil
	}
	return &journal.TimeoutPolicyPayload{
		ScheduleToStartTimeout: p.ScheduleToStartTimeout,
		StartToCloseTimeout:    p.StartToCloseTimeout,
		ScheduleToCloseTimeout: p.ScheduleToCloseTimeout,
		HeartbeatTimeout:       p.HeartbeatTimeout,
	}
}

// WithScheduleToStartTimeout sets the maximum time from scheduling to worker pickup.
func WithScheduleToStartTimeout(d time.Duration) ActivityOption {
	return func(o *activityOptions) {
		if o.timeoutPolicy == nil {
			o.timeoutPolicy = &TimeoutPolicy{}
		}
		o.timeoutPolicy.ScheduleToStartTimeout = d
	}
}

// WithStartToCloseTimeout sets the maximum time from worker start to completion.
func WithStartToCloseTimeout(d time.Duration) ActivityOption {
	return func(o *activityOptions) {
		if o.timeoutPolicy == nil {
			o.timeoutPolicy = &TimeoutPolicy{}
		}
		o.timeoutPolicy.StartToCloseTimeout = d
	}
}

// WithScheduleToCloseTimeout sets the maximum total time from scheduling to completion.
func WithScheduleToCloseTimeout(d time.Duration) ActivityOption {
	return func(o *activityOptions) {
		if o.timeoutPolicy == nil {
			o.timeoutPolicy = &TimeoutPolicy{}
		}
		o.timeoutPolicy.ScheduleToCloseTimeout = d
	}
}

// WithHeartbeatTimeout sets the maximum time between heartbeats.
// Activities must call Heartbeat periodically to reset this deadline.
func WithHeartbeatTimeout(d time.Duration) ActivityOption {
	return func(o *activityOptions) {
		if o.timeoutPolicy == nil {
			o.timeoutPolicy = &TimeoutPolicy{}
		}
		o.timeoutPolicy.HeartbeatTimeout = d
	}
}

type heartbeatKey struct{}

type heartbeatFunc func(details any) error

// Heartbeat signals activity liveness and optionally records progress.
// The details parameter is encoded and stored for checkpointing on retry.
// No-op if the activity was not configured with HeartbeatTimeout.
func Heartbeat(ctx context.Context, details any) error {
	hb, ok := ctx.Value(heartbeatKey{}).(heartbeatFunc)
	if !ok {
		return nil
	}
	return hb(details)
}

type activityInfoKey struct{}

// ActivityInfo provides execution context to running activities.
// Use this to build idempotency keys for external calls.
type ActivityInfo struct {
	WorkflowID    string
	RunID         string
	ActivityName  string
	ScheduledAt   int       // Event ID - stable across retries
	Attempt       int       // 1-indexed; use with ScheduledAt for retry-aware idempotency
	ScheduledTime time.Time // When the activity was first scheduled
}

// IdempotencyKey returns a stable key for this activity execution.
// Format: {workflowID}/{scheduledAt}/{attempt}
func (i ActivityInfo) IdempotencyKey() string {
	return fmt.Sprintf("%s/%d/%d", i.WorkflowID, i.ScheduledAt, i.Attempt)
}

// GetActivityInfo retrieves execution context from an activity's context.
// Returns zero value if called outside an activity.
func GetActivityInfo(ctx context.Context) ActivityInfo {
	info, _ := ctx.Value(activityInfoKey{}).(ActivityInfo)
	return info
}

// WithActivityInfo returns a context with ActivityInfo set.
// Intended for testing; production code uses automatic injection.
func WithActivityInfo(ctx context.Context, info ActivityInfo) context.Context {
	return context.WithValue(ctx, activityInfoKey{}, info)
}

// WithHeartbeatRecorder returns a context with a custom heartbeat handler.
// Intended for testing; production code uses automatic injection.
func WithHeartbeatRecorder(ctx context.Context, recorder func(details any) error) context.Context {
	return context.WithValue(ctx, heartbeatKey{}, heartbeatFunc(recorder))
}

type Activity[I, O any] func(context.Context, I) (O, error)

type ActivityRef[I, O any] struct {
	name string
}

func NewActivityRef[I, O any](name string) ActivityRef[I, O] {
	return ActivityRef[I, O]{name: name}
}

func (r ActivityRef[I, O]) Name() string { return r.name }

type defaultRetryPolicyProvider interface {
	defaultRetryPolicy() *RetryPolicy
}

func (r ActivityRef[I, O]) Execute(ctx Context, input I, opts ...ActivityOption) Future[O] {
	wctx, ok := ctx.(interface {
		activityEventEmitter
		codecProvider
		futureRegistrar
	})
	if !ok {
		panic(panicOutsideWorkflow)
	}
	codec := wctx.codec()

	var options activityOptions
	for _, opt := range opts {
		opt(&options)
	}

	var retryPayload *journal.RetryPolicyPayload
	if options.retryPolicy != nil {
		retryPayload = options.retryPolicy.ToPayload()
	} else if provider, ok := ctx.(defaultRetryPolicyProvider); ok {
		if dp := provider.defaultRetryPolicy(); dp != nil {
			retryPayload = dp.ToPayload()
		}
	}

	var timeoutPayload *journal.TimeoutPolicyPayload
	if options.timeoutPolicy != nil {
		timeoutPayload = options.timeoutPolicy.ToPayload()
	}

	inputJSON, err := codec.Encode(input)
	if err != nil {
		return newFailedFuture[O](fmt.Errorf("encode activity input for %q: %w", r.name, err))
	}

	scheduledID, pendingIndex := wctx.emitActivityScheduled(r.name, inputJSON, retryPayload, timeoutPayload)
	future := &activityFuture[O]{scheduledID: scheduledID, codec: codec}
	wctx.registerPendingFuture(pendingIndex, future)
	return future
}
