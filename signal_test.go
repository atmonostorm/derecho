package derecho_test

import (
	"testing"

	"github.com/atmonostorm/derecho"
	"github.com/atmonostorm/derecho/journal"
)

func TestSignalChannel_ReceiveSingle(t *testing.T) {
	store := derecho.NewMemoryStore()
	engine := mustEngine(t, store)

	derecho.RegisterWorkflow(engine, "signal-test", func(ctx derecho.Context, _ struct{}) (string, error) {
		ch := derecho.GetSignalChannel[string](ctx, "greeting")
		val, ok := ch.Receive(ctx)
		if !ok {
			return "", nil
		}
		return val, nil
	})

	client := engine.Client()
	workflowWorker := engine.WorkflowWorker()

	run, err := client.StartWorkflow(t.Context(), "signal-test", "wf-1", struct{}{})
	if err != nil {
		t.Fatal(err)
	}

	if err := workflowWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	if err := store.SignalWorkflow(t.Context(), "wf-1", "greeting", []byte(`"hello"`)); err != nil {
		t.Fatal(err)
	}

	if err := workflowWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	var result string
	if err := run.Get(t.Context(), &result, derecho.NonBlocking()); err != nil {
		t.Fatal(err)
	}

	if result != "hello" {
		t.Errorf("got %q, want %q", result, "hello")
	}
}

func TestSignalChannel_ReceiveMultiple(t *testing.T) {
	store := derecho.NewMemoryStore()
	engine := mustEngine(t, store)

	derecho.RegisterWorkflow(engine, "multi-signal", func(ctx derecho.Context, _ struct{}) ([]int, error) {
		ch := derecho.GetSignalChannel[int](ctx, "numbers")
		var collected []int
		for i := 0; i < 3; i++ {
			val, _ := ch.Receive(ctx)
			collected = append(collected, val)
		}
		return collected, nil
	})

	client := engine.Client()
	workflowWorker := engine.WorkflowWorker()

	run, err := client.StartWorkflow(t.Context(), "multi-signal", "wf-1", struct{}{})
	if err != nil {
		t.Fatal(err)
	}

	if err := workflowWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 3; i++ {
		val := i + 1
		encoded, _ := derecho.DefaultCodec.Encode(val)
		if err := store.SignalWorkflow(t.Context(), "wf-1", "numbers", encoded); err != nil {
			t.Fatal(err)
		}
	}

	if err := workflowWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	var result []int
	if err := run.Get(t.Context(), &result, derecho.NonBlocking()); err != nil {
		t.Fatal(err)
	}

	if len(result) != 3 || result[0] != 1 || result[1] != 2 || result[2] != 3 {
		t.Errorf("got %v, want [1 2 3]", result)
	}
}

func TestSignalChannel_TryReceive(t *testing.T) {
	store := derecho.NewMemoryStore()
	engine := mustEngine(t, store)

	derecho.RegisterWorkflow(engine, "try-recv", func(ctx derecho.Context, _ struct{}) (string, error) {
		ch := derecho.GetSignalChannel[string](ctx, "maybe")

		val, ok := ch.TryReceive(ctx)
		if ok {
			return val, nil
		}
		return "no signal yet", nil
	})

	client := engine.Client()
	workflowWorker := engine.WorkflowWorker()

	if err := store.SignalWorkflow(t.Context(), "wf-1", "maybe", []byte(`"early signal"`)); err == nil {
		t.Fatal("expected error for non-existent workflow")
	}

	run, err := client.StartWorkflow(t.Context(), "try-recv", "wf-1", struct{}{})
	if err != nil {
		t.Fatal(err)
	}

	if err := workflowWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	var result string
	if err := run.Get(t.Context(), &result, derecho.NonBlocking()); err != nil {
		t.Fatal(err)
	}

	if result != "no signal yet" {
		t.Errorf("got %q, want %q", result, "no signal yet")
	}
}

func TestSignalChannel_Selector(t *testing.T) {
	store := derecho.NewMemoryStore()
	engine := mustEngine(t, store)

	derecho.RegisterWorkflow(engine, "selector-signal", func(ctx derecho.Context, _ struct{}) (string, error) {
		ch := derecho.GetSignalChannel[string](ctx, "select-me")

		var received string
		selector := derecho.NewSelector()
		derecho.AddFuture(selector, ch.ReceiveFuture(), func(val string, err error) {
			received = val
		})
		selector.Select(ctx)

		return received, nil
	})

	client := engine.Client()
	workflowWorker := engine.WorkflowWorker()

	run, err := client.StartWorkflow(t.Context(), "selector-signal", "wf-1", struct{}{})
	if err != nil {
		t.Fatal(err)
	}

	if err := workflowWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	if err := store.SignalWorkflow(t.Context(), "wf-1", "select-me", []byte(`"selected"`)); err != nil {
		t.Fatal(err)
	}

	if err := workflowWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	var result string
	if err := run.Get(t.Context(), &result, derecho.NonBlocking()); err != nil {
		t.Fatal(err)
	}

	if result != "selected" {
		t.Errorf("got %q, want %q", result, "selected")
	}
}

func TestSignalChannel_Replay(t *testing.T) {
	store := derecho.NewMemoryStore()
	engine := mustEngine(t, store)

	signalWorkflow := func(ctx derecho.Context, _ struct{}) ([]string, error) {
		ch := derecho.GetSignalChannel[string](ctx, "replay-test")
		var received []string
		for i := 0; i < 2; i++ {
			val, _ := ch.Receive(ctx)
			received = append(received, val)
		}
		return received, nil
	}

	derecho.RegisterWorkflow(engine, "replay-signal", signalWorkflow)

	client := engine.Client()
	workflowWorker := engine.WorkflowWorker()

	run, err := client.StartWorkflow(t.Context(), "replay-signal", "wf-1", struct{}{})
	if err != nil {
		t.Fatal(err)
	}

	if err := workflowWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	if err := store.SignalWorkflow(t.Context(), "wf-1", "replay-test", []byte(`"first"`)); err != nil {
		t.Fatal(err)
	}
	if err := store.SignalWorkflow(t.Context(), "wf-1", "replay-test", []byte(`"second"`)); err != nil {
		t.Fatal(err)
	}

	if err := workflowWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	var result []string
	if err := run.Get(t.Context(), &result, derecho.NonBlocking()); err != nil {
		t.Fatal(err)
	}

	events, _ := store.Load(t.Context(), "wf-1", run.RunID())

	replayRes, err := derecho.ReplayWithResult(signalWorkflow, events)
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}
	if !replayRes.Complete {
		t.Fatal("replay should complete")
	}

	var replayResult []string
	if err := derecho.DefaultCodec.Decode(replayRes.Result, &replayResult); err != nil {
		t.Fatalf("decode replay result: %v", err)
	}

	if len(replayResult) != len(result) {
		t.Errorf("replay result length %d != original %d", len(replayResult), len(result))
	}
	for i := range result {
		if replayResult[i] != result[i] {
			t.Errorf("replay result[%d] = %q, original = %q", i, replayResult[i], result[i])
		}
	}
}

func TestSignalExternalWorkflow(t *testing.T) {
	store := derecho.NewMemoryStore()
	engine := mustEngine(t, store)

	receiverWorkflow := func(ctx derecho.Context, _ struct{}) (string, error) {
		ch := derecho.GetSignalChannel[string](ctx, "cross-workflow")
		val, _ := ch.Receive(ctx)
		return val, nil
	}

	senderWorkflow := func(ctx derecho.Context, _ struct{}) (string, error) {
		derecho.SignalExternalWorkflow(ctx, "receiver-wf", "cross-workflow", "from sender")
		return "sent", nil
	}

	derecho.RegisterWorkflow(engine, "receiver", receiverWorkflow)
	derecho.RegisterWorkflow(engine, "sender", senderWorkflow)

	client := engine.Client()
	workflowWorker := engine.WorkflowWorker()

	receiverRun, err := client.StartWorkflow(t.Context(), "receiver", "receiver-wf", struct{}{})
	if err != nil {
		t.Fatal(err)
	}

	if err := workflowWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	_, err = client.StartWorkflow(t.Context(), "sender", "sender-wf", struct{}{})
	if err != nil {
		t.Fatal(err)
	}

	if err := workflowWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	if err := workflowWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	var result string
	if err := receiverRun.Get(t.Context(), &result, derecho.NonBlocking()); err != nil {
		t.Fatal(err)
	}

	if result != "from sender" {
		t.Errorf("got %q, want %q", result, "from sender")
	}
}

func TestClient_SignalWorkflow(t *testing.T) {
	store := derecho.NewMemoryStore()
	engine := mustEngine(t, store)

	derecho.RegisterWorkflow(engine, "client-signal-test", func(ctx derecho.Context, _ struct{}) (int, error) {
		ch := derecho.GetSignalChannel[int](ctx, "client-signal")
		val, _ := ch.Receive(ctx)
		return val, nil
	})

	client := engine.Client()
	workflowWorker := engine.WorkflowWorker()

	run, err := client.StartWorkflow(t.Context(), "client-signal-test", "wf-1", struct{}{})
	if err != nil {
		t.Fatal(err)
	}

	if err := workflowWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	if err := client.SignalWorkflow(t.Context(), "wf-1", "client-signal", 42); err != nil {
		t.Fatal(err)
	}

	if err := workflowWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	var result int
	if err := run.Get(t.Context(), &result, derecho.NonBlocking()); err != nil {
		t.Fatal(err)
	}

	if result != 42 {
		t.Errorf("got %d, want %d", result, 42)
	}
}

func TestSignal_WorkflowNotFound(t *testing.T) {
	store := derecho.NewMemoryStore()

	err := store.SignalWorkflow(t.Context(), "nonexistent", "signal", []byte(`"payload"`))
	if err == nil {
		t.Fatal("expected error for nonexistent workflow")
	}
}

func TestSignal_CoalescesWakeup(t *testing.T) {
	store := derecho.NewMemoryStore()
	engine := mustEngine(t, store)

	derecho.RegisterWorkflow(engine, "coalesce-test", func(ctx derecho.Context, _ struct{}) (int, error) {
		ch := derecho.GetSignalChannel[int](ctx, "coalesce")
		sum := 0
		for i := 0; i < 3; i++ {
			val, _ := ch.Receive(ctx)
			sum += val
		}
		return sum, nil
	})

	client := engine.Client()
	workflowWorker := engine.WorkflowWorker()

	run, err := client.StartWorkflow(t.Context(), "coalesce-test", "wf-1", struct{}{})
	if err != nil {
		t.Fatal(err)
	}

	if err := workflowWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= 3; i++ {
		encoded, _ := derecho.DefaultCodec.Encode(i)
		if err := store.SignalWorkflow(t.Context(), "wf-1", "coalesce", encoded); err != nil {
			t.Fatal(err)
		}
	}

	events, _ := store.Load(t.Context(), "wf-1", run.RunID())
	taskCount := 0
	for _, ev := range events {
		if ev.EventType() == journal.TypeWorkflowTaskScheduled {
			taskCount++
		}
	}
	if taskCount > 2 {
		t.Errorf("expected coalesced wakeups: got %d WorkflowTaskScheduled events", taskCount)
	}

	if err := workflowWorker.Process(t.Context()); err != nil {
		t.Fatal(err)
	}

	var result int
	if err := run.Get(t.Context(), &result, derecho.NonBlocking()); err != nil {
		t.Fatal(err)
	}

	if result != 6 {
		t.Errorf("got %d, want %d", result, 6)
	}
}
