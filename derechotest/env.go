package derechotest

import (
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/atmonostorm/derecho"
)

// TestLogger returns a logger that discards all output, for use in tests.
func TestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// activityAttemptState tracks retry state for a scheduled activity.
type activityAttemptState struct {
	attempt int
}

// TestEnv is the primary test harness for workflow testing.
type TestEnv struct {
	t          testing.TB
	stubs      map[string]*activityStub
	childStubs map[string]*childWorkflowStub
	calls      map[string][]ActivityCall
	childCalls map[string][]ChildWorkflowCall

	state       *derecho.StubExecutionState
	currentTime time.Time
	attempts    map[int]*activityAttemptState
}

// Option configures a TestEnv.
type Option func(*TestEnv)

// New creates a test environment.
func New(t testing.TB, opts ...Option) *TestEnv {
	env := &TestEnv{
		t:           t,
		stubs:       make(map[string]*activityStub),
		childStubs:  make(map[string]*childWorkflowStub),
		calls:       make(map[string][]ActivityCall),
		childCalls:  make(map[string][]ChildWorkflowCall),
		currentTime: time.Now(),
	}
	for _, opt := range opts {
		opt(env)
	}
	return env
}

// WithStartTime sets the initial workflow time.
func WithStartTime(t time.Time) Option {
	return func(e *TestEnv) {
		e.currentTime = t
	}
}

// ActivityCall records a call to a stubbed activity.
type ActivityCall struct {
	Input   json.RawMessage
	Output  json.RawMessage
	Error   error
	Attempt int
}

type activityStub struct {
	result           any
	err              error
	fn               func(json.RawMessage) (any, error)
	failUntilAttempt int
	failWith         error
}

func (s *activityStub) execute(input json.RawMessage, attempt int) (any, error) {
	if s.failUntilAttempt > 0 && attempt < s.failUntilAttempt {
		if s.failWith != nil {
			return nil, s.failWith
		}
		return nil, s.err
	}
	if s.fn != nil {
		return s.fn(input)
	}
	if s.err != nil {
		return nil, s.err
	}
	return s.result, nil
}

// ChildWorkflowCall records a call to a stubbed child workflow.
type ChildWorkflowCall struct {
	WorkflowID string
	Input      json.RawMessage
	Output     json.RawMessage
	Error      error
}

type childWorkflowStub struct {
	result any
	err    error
	fn     func(workflowID string, input json.RawMessage) (any, error)
}

func (s *childWorkflowStub) execute(workflowID string, input json.RawMessage) (any, error) {
	if s.fn != nil {
		return s.fn(workflowID, input)
	}
	if s.err != nil {
		return nil, s.err
	}
	return s.result, nil
}
