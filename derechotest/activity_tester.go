package derechotest

import (
	"context"
	"testing"
	"time"

	"github.com/atmonostorm/derecho"
)

// ActivityTester provides a test harness for testing activities in isolation.
type ActivityTester struct {
	t          testing.TB
	info       derecho.ActivityInfo
	heartbeats []any
}

// NewActivityTester creates a tester for activity unit tests.
func NewActivityTester(t testing.TB) *ActivityTester {
	return &ActivityTester{
		t: t,
		info: derecho.ActivityInfo{
			WorkflowID:    "test-workflow",
			RunID:         "test-run",
			ActivityName:  "test-activity",
			ScheduledAt:   1,
			Attempt:       1,
			ScheduledTime: time.Now(),
		},
	}
}

// WithWorkflowID sets the workflow ID for the activity context.
func (at *ActivityTester) WithWorkflowID(id string) *ActivityTester {
	at.info.WorkflowID = id
	return at
}

// WithActivityName sets the activity name.
func (at *ActivityTester) WithActivityName(name string) *ActivityTester {
	at.info.ActivityName = name
	return at
}

// WithAttempt sets the retry attempt number (1-indexed).
func (at *ActivityTester) WithAttempt(attempt int) *ActivityTester {
	at.info.Attempt = attempt
	return at
}

// WithScheduledAt sets the scheduled event ID.
func (at *ActivityTester) WithScheduledAt(id int) *ActivityTester {
	at.info.ScheduledAt = id
	return at
}

// Context returns a context configured for activity execution.
// The context includes ActivityInfo and captures heartbeats.
func (at *ActivityTester) Context(parent context.Context) context.Context {
	ctx := derecho.WithActivityInfo(parent, at.info)
	ctx = derecho.WithHeartbeatRecorder(ctx, func(details any) error {
		at.heartbeats = append(at.heartbeats, details)
		return nil
	})
	return ctx
}

// Heartbeats returns all heartbeat details recorded during execution.
func (at *ActivityTester) Heartbeats() []any {
	return at.heartbeats
}

// HeartbeatCount returns the number of heartbeats recorded.
func (at *ActivityTester) HeartbeatCount() int {
	return len(at.heartbeats)
}

// RunActivity executes an activity function with the test context and returns the result.
func RunActivity[I, O any](at *ActivityTester, activity derecho.Activity[I, O], input I) (O, error) {
	ctx := at.Context(context.Background())
	return activity(ctx, input)
}
