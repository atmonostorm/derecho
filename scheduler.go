package derecho

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/atmonostorm/derecho/journal"
)

const defaultAdvanceTimeout = time.Second

type WorkflowInfo struct {
	WorkflowID   string
	RunID        string
	WorkflowType string
	StartTime    time.Time
}

type Scheduler struct {
	state    ExecutionState
	recorder Recorder
	codec    Codec
	fibers   []*fiber
	fiberID  int

	fiberSpawnedThisRound    bool
	fiberTerminatedThisRound bool
	progressThisRound        bool

	workflowTime       time.Time
	workflowInfo       WorkflowInfo
	defaultRetryPolicy *RetryPolicy

	signalConsumed map[string]int
}

type SchedulerOption func(*Scheduler)

func WithCodec(c Codec) SchedulerOption {
	return func(s *Scheduler) {
		s.codec = c
	}
}

func WithSchedulerRetryPolicy(p RetryPolicy) SchedulerOption {
	return func(s *Scheduler) {
		s.defaultRetryPolicy = &p
	}
}

func NewScheduler(state ExecutionState, workflowFn func(Context), info WorkflowInfo, opts ...SchedulerOption) *Scheduler {
	s := &Scheduler{
		state:          state,
		codec:          DefaultCodec,
		workflowInfo:   info,
		signalConsumed: make(map[string]int),
	}
	for _, opt := range opts {
		opt(s)
	}
	s.registerFiber(workflowFn)
	return s
}

func (s *Scheduler) registerFiber(fn func(Context)) {
	id := fmt.Sprintf("fiber-%d", s.fiberID)
	s.fiberID++

	f := newFiber(id, fn)
	ctx := &workflowContext{f: f, s: s}
	f.launch(ctx)
	s.fibers = append(s.fibers, f)
	s.fiberSpawnedThisRound = true
}

func (s *Scheduler) Close() {
	for _, f := range s.fibers {
		if f != nil {
			close(f.wake)
		}
	}
	s.fibers = nil
}

func (s *Scheduler) handlePanic(ctx context.Context, panicVal any, scheduledAtTask int) error {
	// Create fresh recorder - discard any previous events from this round
	r := s.state.NewRecorder(scheduledAtTask + 1)

	_, err := r.Record(journal.TypeWorkflowFailed, func() journal.Event {
		return journal.WorkflowFailed{
			Error: journal.NewErrorf(journal.ErrorKindPanic, "panic: %v", panicVal),
		}
	})
	if err != nil {
		return err
	}

	if err := r.Commit(ctx, scheduledAtTask); err != nil {
		return err
	}

	s.Close()
	return nil
}

func (s *Scheduler) handleInternalError(ctx context.Context, internalErr error, scheduledAtTask int) error {
	// Create fresh recorder - discard any previous events from this round
	r := s.state.NewRecorder(scheduledAtTask + 1)

	// Determine error kind based on error type
	errKind := journal.ErrorKindApplication
	if _, ok := internalErr.(*NondeterminismError); ok {
		errKind = journal.ErrorKindNondeterminism
	}

	_, err := r.Record(journal.TypeWorkflowFailed, func() journal.Event {
		return journal.WorkflowFailed{
			Error: &journal.Error{
				Kind:    errKind,
				Message: internalErr.Error(),
			},
		}
	})
	if err != nil {
		return err
	}

	if err := r.Commit(ctx, scheduledAtTask); err != nil {
		return err
	}

	s.Close()
	return nil
}

// Advance runs the scheduler until quiescence or completion.
// scheduledAtTask is the ID of the WorkflowTaskScheduled event that triggered this advance.
// Invariant: WorkflowTaskStarted is always scheduledAtTask+1 (appended immediately before Advance).
func (s *Scheduler) Advance(ctx context.Context, scheduledAtTask int, workflowTime time.Time) error {
	s.workflowTime = workflowTime
	s.recorder = s.state.NewRecorder(scheduledAtTask + 1)

	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultAdvanceTimeout)
		defer cancel()
	}

	for {
		s.fiberSpawnedThisRound = false
		s.fiberTerminatedThisRound = false
		s.progressThisRound = false
		pendingBefore := s.recorder.PendingCount()

		for i := 0; i < len(s.fibers); i++ {
			alive, panicVal, internalErr, err := s.fibers[i].wakeup(ctx)
			if err != nil {
				return fmt.Errorf("derecho: workflow deadlock detected (>%s elapsed during workflow execution): %w", defaultAdvanceTimeout, err)
			}
			if internalErr != nil {
				return s.handleInternalError(ctx, internalErr, scheduledAtTask)
			}
			if panicVal != nil {
				return s.handlePanic(ctx, panicVal, scheduledAtTask)
			}
			if !alive {
				s.fibers[i] = nil
				s.fiberTerminatedThisRound = true
			}
		}

		s.fibers = slices.DeleteFunc(s.fibers, func(f *fiber) bool {
			return f == nil
		})

		eventsThisRound := s.recorder.PendingCount() - pendingBefore

		allFibersDone := len(s.fibers) == 0
		// Quiescent: no activity this round - fibers cannot make progress
		quiescent := eventsThisRound == 0 && !s.fiberSpawnedThisRound && !s.fiberTerminatedThisRound && !s.progressThisRound

		if allFibersDone || quiescent {
			break
		}
	}

	return s.recorder.Commit(ctx, scheduledAtTask)
}
