package derecho

import (
	"strings"
	"testing"
	"time"

	"github.com/atmonostorm/derecho/journal"
)

func TestChannel_UnbufferedSendReceive(t *testing.T) {
	state := NewStubExecutionState()
	var received int
	var ok bool

	wf := func(ctx Context) {
		ch := NewChannel[int](ctx)

		Go(ctx, func(ctx Context) {
			ch.Send(ctx, 42)
		})

		received, ok = ch.Receive(ctx)
	}

	s := NewScheduler(state, wf, testWorkflowInfo())
	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	if !ok {
		t.Error("expected ok=true")
	}
	if received != 42 {
		t.Errorf("expected 42, got %d", received)
	}
}

func TestChannel_UnbufferedRendezvous(t *testing.T) {
	state := NewStubExecutionState()
	var order []string

	wf := func(ctx Context) {
		ch := NewChannel[int](ctx)

		Go(ctx, func(ctx Context) {
			order = append(order, "sender-before")
			ch.Send(ctx, 1)
			order = append(order, "sender-after")
		})

		order = append(order, "receiver-before")
		ch.Receive(ctx)
		order = append(order, "receiver-after")
	}

	s := NewScheduler(state, wf, testWorkflowInfo())
	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	if len(order) != 4 {
		t.Fatalf("expected 4 entries, got %d: %v", len(order), order)
	}
}

func TestChannel_BufferedSendReceive(t *testing.T) {
	state := NewStubExecutionState()
	var results []int

	wf := func(ctx Context) {
		ch := NewBufferedChannel[int](ctx, 3)

		ch.Send(ctx, 1)
		ch.Send(ctx, 2)
		ch.Send(ctx, 3)

		for i := 0; i < 3; i++ {
			v, ok := ch.Receive(ctx)
			if !ok {
				break
			}
			results = append(results, v)
		}
	}

	s := NewScheduler(state, wf, testWorkflowInfo())
	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	for i, v := range results {
		if v != i+1 {
			t.Errorf("results[%d] = %d, want %d", i, v, i+1)
		}
	}
}

func TestChannel_BufferedBlocksWhenFull(t *testing.T) {
	state := NewStubExecutionState()
	var order []string

	wf := func(ctx Context) {
		ch := NewBufferedChannel[int](ctx, 1)

		Go(ctx, func(ctx Context) {
			ch.Send(ctx, 1) // fits in buffer
			order = append(order, "first-send")
			ch.Send(ctx, 2) // blocks - buffer full
			order = append(order, "second-send")
		})

		// Let sender run first
		Await(ctx, func() bool { return len(order) > 0 })

		order = append(order, "receive-1")
		ch.Receive(ctx) // unblocks second send
		order = append(order, "receive-2")
		ch.Receive(ctx)
	}

	s := NewScheduler(state, wf, testWorkflowInfo())
	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	// first-send, receive-1, second-send, receive-2
	if len(order) != 4 {
		t.Fatalf("expected 4 entries, got %d: %v", len(order), order)
	}
}

func TestChannel_CloseUnblocksReceivers(t *testing.T) {
	state := NewStubExecutionState()
	var receiverOK bool
	var receiverDone bool

	wf := func(ctx Context) {
		ch := NewChannel[int](ctx)

		Go(ctx, func(ctx Context) {
			_, receiverOK = ch.Receive(ctx)
			receiverDone = true
		})

		// Let receiver block first
		Await(ctx, func() bool { return len(ch.recvWaiters) > 0 })

		ch.Close(ctx)

		Await(ctx, func() bool { return receiverDone })
	}

	s := NewScheduler(state, wf, testWorkflowInfo())
	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	if receiverOK {
		t.Error("expected ok=false after close")
	}
	if !receiverDone {
		t.Error("receiver did not complete")
	}
}

func TestChannel_ReceiveFromClosedReturnsZero(t *testing.T) {
	state := NewStubExecutionState()
	var received int
	var ok bool

	wf := func(ctx Context) {
		ch := NewChannel[int](ctx)
		ch.Close(ctx)
		received, ok = ch.Receive(ctx)
	}

	s := NewScheduler(state, wf, testWorkflowInfo())
	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	if ok {
		t.Error("expected ok=false")
	}
	if received != 0 {
		t.Errorf("expected zero value, got %d", received)
	}
}

func TestChannel_ClosedBufferedDrainsFirst(t *testing.T) {
	state := NewStubExecutionState()
	var results []int
	var oks []bool

	wf := func(ctx Context) {
		ch := NewBufferedChannel[int](ctx, 3)
		ch.Send(ctx, 1)
		ch.Send(ctx, 2)
		ch.Close(ctx)

		// Should drain buffer before returning closed
		for i := 0; i < 3; i++ {
			v, ok := ch.Receive(ctx)
			results = append(results, v)
			oks = append(oks, ok)
		}
	}

	s := NewScheduler(state, wf, testWorkflowInfo())
	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	// First two should have values
	if results[0] != 1 || !oks[0] {
		t.Errorf("first receive: got (%d, %v), want (1, true)", results[0], oks[0])
	}
	if results[1] != 2 || !oks[1] {
		t.Errorf("second receive: got (%d, %v), want (2, true)", results[1], oks[1])
	}
	// Third should be closed
	if results[2] != 0 || oks[2] {
		t.Errorf("third receive: got (%d, %v), want (0, false)", results[2], oks[2])
	}
}

func TestChannel_SendOnClosedPanics(t *testing.T) {
	state := NewStubExecutionState()

	wf := func(ctx Context) {
		ch := NewChannel[int](ctx)
		ch.Close(ctx)
		ch.Send(ctx, 42)
	}

	s := NewScheduler(state, wf, testWorkflowInfo())
	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(state.Events()) != 1 {
		t.Fatalf("expected 1 event, got %d", len(state.Events()))
	}
	ev, ok := state.Events()[0].(journal.WorkflowFailed)
	if !ok {
		t.Fatalf("expected WorkflowFailed, got %T", state.Events()[0])
	}
	if !strings.Contains(ev.Error.Message, "send on closed channel") {
		t.Errorf("expected panic message about send on closed channel, got: %s", ev.Error.Message)
	}
}

func TestChannel_DoubleClosePanics(t *testing.T) {
	state := NewStubExecutionState()

	wf := func(ctx Context) {
		ch := NewChannel[int](ctx)
		ch.Close(ctx)
		ch.Close(ctx)
	}

	s := NewScheduler(state, wf, testWorkflowInfo())
	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(state.Events()) != 1 {
		t.Fatalf("expected 1 event, got %d", len(state.Events()))
	}
	ev, ok := state.Events()[0].(journal.WorkflowFailed)
	if !ok {
		t.Fatalf("expected WorkflowFailed, got %T", state.Events()[0])
	}
	if !strings.Contains(ev.Error.Message, "close of closed channel") {
		t.Errorf("expected panic message about close of closed channel, got: %s", ev.Error.Message)
	}
}

func TestChannel_SelectorSingleChannel(t *testing.T) {
	state := NewStubExecutionState()
	var received int
	var callbackCalled bool

	wf := func(ctx Context) {
		ch := NewChannel[int](ctx)

		Go(ctx, func(ctx Context) {
			ch.Send(ctx, 99)
		})

		selector := NewSelector()
		AddFuture(selector, ch.ReceiveFuture(), func(val int, err error) {
			received = val
			callbackCalled = true
		})
		selector.Select(ctx)
	}

	s := NewScheduler(state, wf, testWorkflowInfo())
	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	if !callbackCalled {
		t.Error("callback not called")
	}
	if received != 99 {
		t.Errorf("expected 99, got %d", received)
	}
}

func TestChannel_SelectorMultipleChannels(t *testing.T) {
	state := NewStubExecutionState()
	var winner string

	wf := func(ctx Context) {
		ch1 := NewChannel[int](ctx)
		ch2 := NewChannel[string](ctx)

		// ch2 gets value first
		Go(ctx, func(ctx Context) {
			ch2.Send(ctx, "hello")
		})

		selector := NewSelector()
		AddFuture(selector, ch1.ReceiveFuture(), func(val int, err error) {
			winner = "ch1"
		})
		AddFuture(selector, ch2.ReceiveFuture(), func(val string, err error) {
			winner = "ch2"
		})
		selector.Select(ctx)
	}

	s := NewScheduler(state, wf, testWorkflowInfo())
	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	if winner != "ch2" {
		t.Errorf("expected ch2 to win, got %s", winner)
	}
}

func TestChannel_SelectorChannelAndTimer(t *testing.T) {
	state := NewStubExecutionState()
	var winner string

	wf := func(ctx Context) {
		ch := NewChannel[int](ctx)
		timer := NewTimer(ctx, time.Hour)

		// Channel gets value, timer not fired
		Go(ctx, func(ctx Context) {
			ch.Send(ctx, 42)
		})

		selector := NewSelector()
		AddFuture(selector, ch.ReceiveFuture(), func(val int, err error) {
			winner = "channel"
		})
		AddFuture(selector, timer, func(t time.Time, err error) {
			winner = "timer"
		})
		selector.Select(ctx)
	}

	s := NewScheduler(state, wf, testWorkflowInfo())
	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	if winner != "channel" {
		t.Errorf("expected channel to win, got %s", winner)
	}
}

func TestChannel_SelectorClosedChannel(t *testing.T) {
	state := NewStubExecutionState()
	var gotErr error
	var callbackCalled bool

	wf := func(ctx Context) {
		ch := NewChannel[int](ctx)
		ch.Close(ctx)

		selector := NewSelector()
		AddFuture(selector, ch.ReceiveFuture(), func(val int, err error) {
			gotErr = err
			callbackCalled = true
		})
		selector.Select(ctx)
	}

	s := NewScheduler(state, wf, testWorkflowInfo())
	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	if !callbackCalled {
		t.Error("callback not called")
	}
	if gotErr != ErrChannelClosed {
		t.Errorf("expected ErrChannelClosed, got %v", gotErr)
	}
}

func TestChannel_MultipleProducersConsumers(t *testing.T) {
	state := NewStubExecutionState()
	var received []int

	wf := func(ctx Context) {
		ch := NewChannel[int](ctx)

		for i := 1; i <= 3; i++ {
			val := i
			Go(ctx, func(ctx Context) {
				ch.Send(ctx, val)
			})
		}

		for i := 0; i < 3; i++ {
			v, _ := ch.Receive(ctx)
			received = append(received, v)
		}
	}

	s := NewScheduler(state, wf, testWorkflowInfo())
	if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	if len(received) != 3 {
		t.Fatalf("expected 3 values, got %d", len(received))
	}

	sum := 0
	for _, v := range received {
		sum += v
	}
	if sum != 6 { // 1+2+3
		t.Errorf("expected sum=6, got %d", sum)
	}
}

func TestChannel_SchedulerDeterminism(t *testing.T) {
	runWorkflow := func() []int {
		state := NewStubExecutionState()
		var results []int

		wf := func(ctx Context) {
			ch := NewChannel[int](ctx)

			Go(ctx, func(ctx Context) {
				ch.Send(ctx, 1)
				ch.Send(ctx, 2)
			})

			Go(ctx, func(ctx Context) {
				ch.Send(ctx, 3)
			})

			for i := 0; i < 3; i++ {
				v, _ := ch.Receive(ctx)
				results = append(results, v)
			}
		}

		s := NewScheduler(state, wf, testWorkflowInfo())
		if err := s.Advance(t.Context(), 0, time.Now()); err != nil {
			t.Fatal(err)
		}
		return results
	}

	results1 := runWorkflow()
	results2 := runWorkflow()

	if len(results1) != len(results2) {
		t.Fatalf("different result lengths: %d vs %d", len(results1), len(results2))
	}

	for i := range results1 {
		if results1[i] != results2[i] {
			t.Errorf("results differ at index %d: %d vs %d", i, results1[i], results2[i])
		}
	}
}

func TestChannel_HighConcurrencyReplay(t *testing.T) {
	store := NewMemoryStore()
	engine := mustEngine(t, store)

	channelWorkflow := func(ctx Context, _ struct{}) ([]int, error) {
		const numProducers = 5
		const numItems = 3

		unbuffered := NewChannel[int](ctx)
		pipeline := NewBufferedChannel[int](ctx, numProducers*numItems)

		for i := 0; i < numProducers; i++ {
			producerID := i
			Go(ctx, func(ctx Context) {
				for j := 0; j < numItems; j++ {
					unbuffered.Send(ctx, producerID*100+j)
				}
			})
		}

		Go(ctx, func(ctx Context) {
			for i := 0; i < numProducers*numItems; i++ {
				val, _ := unbuffered.Receive(ctx)
				pipeline.Send(ctx, val*10)
			}
			pipeline.Close(ctx)
		})

		var collected []int
		for i := 0; i < numProducers*numItems; i++ {
			selector := NewSelector()
			AddFuture(selector, pipeline.ReceiveFuture(), func(val int, err error) {
				if err == nil {
					collected = append(collected, val)
				}
			})
			selector.Select(ctx)
		}

		return collected, nil
	}

	RegisterWorkflow(engine, "channel-stress", channelWorkflow)

	client := engine.Client()
	worker := engine.WorkflowWorker()

	run, err := client.StartWorkflow(t.Context(), "channel-stress", "stress-1", struct{}{})
	if err != nil {
		t.Fatal(err)
	}

	if err := worker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	var result []int
	if err := run.Get(t.Context(), &result, NonBlocking()); err != nil {
		t.Fatalf("workflow failed: %v", err)
	}

	if len(result) != 15 {
		t.Errorf("expected 15 results, got %d: %v", len(result), result)
	}

	events, err := store.Load(t.Context(), "stress-1", run.RunID())
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("workflow completed with %d events, result has %d items", len(events), len(result))

	if err := Replay(channelWorkflow, events); err != nil {
		t.Fatalf("replay failed: %v", err)
	}

	if err := Replay(channelWorkflow, events); err != nil {
		t.Fatalf("double replay failed: %v", err)
	}

	replayResult, err := ReplayWithResult(channelWorkflow, events)
	if err != nil {
		t.Fatalf("ReplayWithResult failed: %v", err)
	}

	if !replayResult.Complete {
		t.Error("replay did not complete")
	}

	var replayedResult []int
	if err := DefaultCodec.Decode(replayResult.Result, &replayedResult); err != nil {
		t.Fatalf("failed to decode replay result: %v", err)
	}

	if len(replayedResult) != len(result) {
		t.Fatalf("replay result length mismatch: original=%d, replay=%d", len(result), len(replayedResult))
	}

	for i := range result {
		if result[i] != replayedResult[i] {
			t.Errorf("result mismatch at index %d: original=%d, replay=%d", i, result[i], replayedResult[i])
		}
	}

	t.Logf("replay verified: %d items match", len(result))
}
