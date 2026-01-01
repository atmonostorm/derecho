package derecho

import (
	"runtime"
	"time"

	"github.com/atmonostorm/derecho/journal"
)

const panicOutsideWorkflow = "derecho: called outside workflow context"

// Context is a sealed interface for workflow execution. The unexported method
// prevents external implementation, so internal type assertions to capability
// interfaces (yielder, spawner, etc) are safe - only workflowContext exists.
type Context interface {
	derechoContext()

	Err() error
}

type workflowContext struct {
	f *fiber
	s *Scheduler
}

func (*workflowContext) derechoContext() {}

func (ctx *workflowContext) Err() error {
	if ctx.s.cancelRequested {
		return ErrCancelled
	}
	return nil
}

func (ctx *workflowContext) cancelFuture() Future[CancelInfo] {
	return ctx.s.cancelFut
}

// failInternal terminates the workflow fiber with an infrastructure error.
// Uses runtime.Goexit so user code cannot intercept via recover().
func (ctx *workflowContext) failInternal(err error) {
	ctx.f.internalErr <- err
	runtime.Goexit()
}

func (ctx *workflowContext) emitWorkflowCompleted(result []byte) {
	_, _, err := ctx.s.recorder.Record(journal.TypeWorkflowCompleted, func() journal.Event {
		return journal.WorkflowCompleted{Result: result}
	})
	if err != nil {
		ctx.failInternal(err)
	}
}

func (ctx *workflowContext) emitWorkflowFailed(err error) {
	_, _, recordErr := ctx.s.recorder.Record(journal.TypeWorkflowFailed, func() journal.Event {
		return journal.WorkflowFailed{Error: journal.ToError(err)}
	})
	if recordErr != nil {
		ctx.failInternal(recordErr)
	}
}

func (ctx *workflowContext) emitWorkflowCancelled() {
	_, _, recordErr := ctx.s.recorder.Record(journal.TypeWorkflowCancelled, func() journal.Event {
		return journal.WorkflowCancelled{Forced: false}
	})
	if recordErr != nil {
		ctx.failInternal(recordErr)
	}
}

func (ctx *workflowContext) emitContinueAsNew(input []byte) {
	_, _, recordErr := ctx.s.recorder.Record(journal.TypeWorkflowContinuedAsNew, func() journal.Event {
		return journal.WorkflowContinuedAsNew{NewInput: input}
	})
	if recordErr != nil {
		ctx.failInternal(recordErr)
	}
}

func (ctx *workflowContext) emitActivityScheduled(name string, input []byte, retryPolicy *journal.RetryPolicyPayload, timeoutPolicy *journal.TimeoutPolicyPayload) (id int, newEventIndex int) {
	scheduledAt := ctx.s.workflowTime
	recorded, idx, err := ctx.s.recorder.Record(journal.TypeActivityScheduled, func() journal.Event {
		return journal.ActivityScheduled{
			Name:          name,
			Input:         input,
			RetryPolicy:   retryPolicy,
			TimeoutPolicy: timeoutPolicy,
			ScheduledAt:   scheduledAt,
		}
	})
	if err != nil {
		ctx.failInternal(err)
	}
	return recorded.Base().ID, idx
}

func (ctx *workflowContext) defaultRetryPolicy() *RetryPolicy {
	return ctx.s.defaultRetryPolicy
}

func (ctx *workflowContext) emitTimerScheduled(duration time.Duration, fireAt time.Time) (id int, newEventIndex int) {
	recorded, idx, err := ctx.s.recorder.Record(journal.TypeTimerScheduled, func() journal.Event {
		return journal.TimerScheduled{
			Duration: duration,
			FireAt:   fireAt,
		}
	})
	if err != nil {
		ctx.failInternal(err)
	}
	return recorded.Base().ID, idx
}

func (ctx *workflowContext) emitTimerCancelled(scheduledID int) {
	_, _, err := ctx.s.recorder.Record(journal.TypeTimerCancelled, func() journal.Event {
		return journal.TimerCancelled{
			BaseEvent: journal.BaseEvent{ScheduledByID: scheduledID},
		}
	})
	if err != nil {
		ctx.failInternal(err)
	}
}

func (ctx *workflowContext) emitChildWorkflowScheduled(workflowType, workflowID string, input []byte, closePolicy journal.ParentClosePolicy) (id int, newEventIndex int) {
	info := ctx.s.workflowInfo
	recorded, idx, err := ctx.s.recorder.Record(journal.TypeChildWorkflowScheduled, func() journal.Event {
		return journal.ChildWorkflowScheduled{
			WorkflowType:      workflowType,
			WorkflowID:        workflowID,
			Input:             input,
			ParentWorkflowID:  info.WorkflowID,
			ParentRunID:       info.RunID,
			ParentClosePolicy: closePolicy,
		}
	})
	if err != nil {
		ctx.failInternal(err)
	}
	return recorded.Base().ID, idx
}

func (ctx *workflowContext) getByScheduledID(scheduledEventID int) []journal.Event {
	return ctx.s.state.GetByScheduledID(scheduledEventID)
}

func (ctx *workflowContext) codec() Codec {
	return ctx.s.codec
}

func (ctx *workflowContext) Yield() {
	ctx.f.yield()
}

func (ctx *workflowContext) registerFiber(fn func(Context)) {
	ctx.s.registerFiber(fn)
}

func (ctx *workflowContext) workflowTime() time.Time {
	return ctx.s.workflowTime
}

func (ctx *workflowContext) info() WorkflowInfo {
	return ctx.s.workflowInfo
}

func (ctx *workflowContext) recorder() Recorder {
	return ctx.s.recorder
}

func (ctx *workflowContext) nextSignal(name string) *journal.SignalReceived {
	signals := ctx.s.state.GetSignals(name)
	consumed := ctx.s.signalConsumed[name]
	if consumed >= len(signals) {
		return nil
	}
	ctx.s.signalConsumed[name] = consumed + 1
	return &signals[consumed]
}

func (ctx *workflowContext) emitSignalExternalScheduled(targetWorkflowID, signalName string, payload any) {
	codec := ctx.s.codec
	encoded, err := codec.Encode(payload)
	if err != nil {
		ctx.failInternal(err)
	}

	_, _, err = ctx.s.recorder.Record(journal.TypeSignalExternalScheduled, func() journal.Event {
		return journal.SignalExternalScheduled{
			TargetWorkflowID: targetWorkflowID,
			SignalName:       signalName,
			Payload:          encoded,
		}
	})
	if err != nil {
		ctx.failInternal(err)
	}
}

// Internal interfaces for workflow API functions.
// Each interface exposes a narrow capability that workflow functions need.

type yielder interface {
	Yield()
}

type spawner interface {
	registerFiber(fn func(Context))
}

type timerScheduler interface {
	emitTimerScheduled(duration time.Duration, fireAt time.Time) (id int, newEventIndex int)
	workflowTime() time.Time
}

type timerCanceller interface {
	emitTimerCancelled(scheduledID int)
}

type timeProvider interface {
	workflowTime() time.Time
}

type infoProvider interface {
	info() WorkflowInfo
}

type recorderProvider interface {
	recorder() Recorder
}

type codecProvider interface {
	codec() Codec
}

type progressSignaler interface {
	signalProgress()
}

func (ctx *workflowContext) signalProgress() {
	ctx.s.progressThisRound = true
}

type internalFailer interface {
	failInternal(err error)
}

type futureRegistrar interface {
	registerPendingFuture(pendingIndex int, f ScheduledIDReceiver)
}

func (ctx *workflowContext) registerPendingFuture(pendingIndex int, f ScheduledIDReceiver) {
	ctx.s.recorder.RegisterPendingFuture(pendingIndex, f)
}
