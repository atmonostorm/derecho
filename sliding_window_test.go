package derecho_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/atmonostorm/derecho"
	"github.com/atmonostorm/derecho/store/derechosqlite"
)

// runWorkers runs workflow and activity workers concurrently until ctx is cancelled.
func runWorkers(ctx context.Context, workflowWorker, activityWorker interface{ Process(context.Context) error }) {
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for ctx.Err() == nil {
			if err := workflowWorker.Process(ctx); err != nil {
				if ctx.Err() != nil {
					return
				}
			}
		}
	}()

	if activityWorker != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				if err := activityWorker.Process(ctx); err != nil {
					if ctx.Err() != nil {
						return
					}
				}
			}
		}()
	}

	<-ctx.Done()
	wg.Wait()
}

// TestSlidingWindowChildWorkflows mimics the filesystem analyzer pattern:
// parent spawns children with bounded concurrency using Selector, then ContinueAsNew.
func TestSlidingWindowChildWorkflows(t *testing.T) {
	store := derecho.NewMemoryStore()
	engine := mustEngine(t, store)

	var activityCalls atomic.Int32

	derecho.RegisterActivity(engine, "DoWork", func(ctx context.Context, id int) (int, error) {
		activityCalls.Add(1)
		return id * 2, nil
	})

	childRef := derecho.NewChildWorkflowRef[int, int]("worker-child")
	activityRef := derecho.NewActivityRef[int, int]("DoWork")

	derecho.RegisterWorkflow(engine, "worker-child", func(ctx derecho.Context, id int) (int, error) {
		return activityRef.Execute(ctx, id, derecho.WithStartToCloseTimeout(time.Minute)).Get(ctx)
	})

	type ParentInput struct {
		TotalItems     int
		MaxConcurrent  int
		ProcessedSoFar int
	}

	derecho.RegisterWorkflow(engine, "sliding-parent", func(ctx derecho.Context, input ParentInput) (int, error) {
		const batchSize = 5

		type activeChild struct {
			id     int
			future derecho.Future[int]
		}

		var active []activeChild
		nextIdx := input.ProcessedSoFar
		var completed int

		startChild := func(id int) activeChild {
			workflowID := fmt.Sprintf("child-%d", id)
			return activeChild{
				id:     id,
				future: childRef.Execute(ctx, workflowID, id),
			}
		}

		batchEnd := input.ProcessedSoFar + batchSize
		if batchEnd > input.TotalItems {
			batchEnd = input.TotalItems
		}

		for nextIdx < batchEnd && len(active) < input.MaxConcurrent {
			active = append(active, startChild(nextIdx))
			nextIdx++
		}

		for len(active) > 0 {
			sel := derecho.NewSelector()
			completedIdx := -1

			for i := range active {
				idx := i
				derecho.AddFuture(sel, active[idx].future, func(_ int, _ error) {
					completedIdx = idx
				})
			}

			sel.Select(ctx)

			if completedIdx >= 0 && completedIdx < len(active) {
				active = append(active[:completedIdx], active[completedIdx+1:]...)
				completed++

				if nextIdx < batchEnd {
					active = append(active, startChild(nextIdx))
					nextIdx++
				}
			}
		}

		if batchEnd < input.TotalItems {
			return 0, derecho.NewContinueAsNewError(ParentInput{
				TotalItems:     input.TotalItems,
				MaxConcurrent:  input.MaxConcurrent,
				ProcessedSoFar: batchEnd,
			})
		}

		return completed, nil
	})

	client := engine.Client()
	workflowWorker := engine.WorkflowWorker()
	activityWorker := engine.ActivityWorker()

	_, err := client.StartWorkflow(t.Context(), "sliding-parent", "parent-1", ParentInput{
		TotalItems:    15,
		MaxConcurrent: 3,
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	go runWorkers(ctx, workflowWorker, activityWorker)

	// Wait for all activities to complete (workflow uses ContinueAsNew, so run.Get won't work)
	for activityCalls.Load() < 15 {
		select {
		case <-ctx.Done():
			t.Fatalf("timeout waiting for activities, got %d", activityCalls.Load())
		case <-time.After(10 * time.Millisecond):
		}
	}

	cancel()

	if activityCalls.Load() != 15 {
		t.Errorf("expected 15 activity calls, got %d", activityCalls.Load())
	}
}

// TestSlidingWindowManyChildren tests with more children to stress the system.
func TestSlidingWindowManyChildren(t *testing.T) {
	store := derecho.NewMemoryStore()
	engine := mustEngine(t, store)

	var childCompletions atomic.Int32

	childRef := derecho.NewChildWorkflowRef[int, int]("counter-child")

	derecho.RegisterWorkflow(engine, "counter-child", func(ctx derecho.Context, id int) (int, error) {
		childCompletions.Add(1)
		return id, nil
	})

	derecho.RegisterWorkflow(engine, "many-children-parent", func(ctx derecho.Context, totalChildren int) (int, error) {
		const maxConcurrent = 10

		type activeChild struct {
			id     int
			future derecho.Future[int]
		}

		var active []activeChild
		nextIdx := 0
		var completed int

		for nextIdx < totalChildren && len(active) < maxConcurrent {
			workflowID := fmt.Sprintf("child-%d", nextIdx)
			active = append(active, activeChild{
				id:     nextIdx,
				future: childRef.Execute(ctx, workflowID, nextIdx),
			})
			nextIdx++
		}

		for len(active) > 0 {
			sel := derecho.NewSelector()
			completedIdx := -1

			for i := range active {
				idx := i
				derecho.AddFuture(sel, active[idx].future, func(_ int, _ error) {
					completedIdx = idx
				})
			}

			sel.Select(ctx)

			if completedIdx >= 0 && completedIdx < len(active) {
				active = append(active[:completedIdx], active[completedIdx+1:]...)
				completed++

				if nextIdx < totalChildren {
					workflowID := fmt.Sprintf("child-%d", nextIdx)
					active = append(active, activeChild{
						id:     nextIdx,
						future: childRef.Execute(ctx, workflowID, nextIdx),
					})
					nextIdx++
				}
			}
		}

		return completed, nil
	})

	client := engine.Client()
	worker := engine.WorkflowWorker()

	run, err := client.StartWorkflow(t.Context(), "many-children-parent", "parent-1", 50)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	go runWorkers(ctx, worker, nil)

	var result int
	for {
		err = run.Get(ctx, &result, derecho.NonBlocking())
		if err == derecho.ErrNotCompleted {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if err != nil {
			t.Fatalf("workflow failed: %v", err)
		}
		break
	}

	cancel()

	if result != 50 {
		t.Errorf("expected 50 completed, got %d", result)
	}
	if childCompletions.Load() != 50 {
		t.Errorf("expected 50 child completions, got %d", childCompletions.Load())
	}
}

// TestSlidingWindowWithActivitiesInChildren exercises the full pattern:
// parent with sliding window, children with multiple activities.
func TestSlidingWindowWithActivitiesInChildren(t *testing.T) {
	store := derecho.NewMemoryStore()
	engine := mustEngine(t, store)

	var step1Calls, step2Calls, step3Calls atomic.Int32

	derecho.RegisterActivity(engine, "Step1", func(ctx context.Context, id int) (int, error) {
		step1Calls.Add(1)
		return id, nil
	})
	derecho.RegisterActivity(engine, "Step2", func(ctx context.Context, id int) (int, error) {
		step2Calls.Add(1)
		return id * 2, nil
	})
	derecho.RegisterActivity(engine, "Step3", func(ctx context.Context, id int) (int, error) {
		step3Calls.Add(1)
		return id, nil
	})

	childRef := derecho.NewChildWorkflowRef[int, int]("multi-step-child")
	step1Ref := derecho.NewActivityRef[int, int]("Step1")
	step2Ref := derecho.NewActivityRef[int, int]("Step2")
	step3Ref := derecho.NewActivityRef[int, int]("Step3")

	opts := []derecho.ActivityOption{derecho.WithStartToCloseTimeout(time.Minute)}

	derecho.RegisterWorkflow(engine, "multi-step-child", func(ctx derecho.Context, id int) (int, error) {
		v1, err := step1Ref.Execute(ctx, id, opts...).Get(ctx)
		if err != nil {
			return 0, err
		}
		v2, err := step2Ref.Execute(ctx, v1, opts...).Get(ctx)
		if err != nil {
			return 0, err
		}
		v3, err := step3Ref.Execute(ctx, v2, opts...).Get(ctx)
		if err != nil {
			return 0, err
		}
		return v3, nil
	})

	derecho.RegisterWorkflow(engine, "orchestrator", func(ctx derecho.Context, totalChildren int) (int, error) {
		const maxConcurrent = 5

		type activeChild struct {
			id     int
			future derecho.Future[int]
		}

		var active []activeChild
		nextIdx := 0
		var completed int

		for nextIdx < totalChildren && len(active) < maxConcurrent {
			workflowID := fmt.Sprintf("child-%d", nextIdx)
			active = append(active, activeChild{
				id:     nextIdx,
				future: childRef.Execute(ctx, workflowID, nextIdx),
			})
			nextIdx++
		}

		for len(active) > 0 {
			sel := derecho.NewSelector()
			completedIdx := -1

			for i := range active {
				idx := i
				derecho.AddFuture(sel, active[idx].future, func(_ int, _ error) {
					completedIdx = idx
				})
			}

			sel.Select(ctx)

			if completedIdx >= 0 && completedIdx < len(active) {
				active = append(active[:completedIdx], active[completedIdx+1:]...)
				completed++

				if nextIdx < totalChildren {
					workflowID := fmt.Sprintf("child-%d", nextIdx)
					active = append(active, activeChild{
						id:     nextIdx,
						future: childRef.Execute(ctx, workflowID, nextIdx),
					})
					nextIdx++
				}
			}
		}

		return completed, nil
	})

	client := engine.Client()
	workflowWorker := engine.WorkflowWorker()
	activityWorker := engine.ActivityWorker()

	const totalChildren = 20

	run, err := client.StartWorkflow(t.Context(), "orchestrator", "parent-1", totalChildren)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	go runWorkers(ctx, workflowWorker, activityWorker)

	var result int
	for {
		err = run.Get(ctx, &result, derecho.NonBlocking())
		if err == derecho.ErrNotCompleted {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if err != nil {
			t.Fatalf("workflow failed: %v", err)
		}
		break
	}

	cancel()

	if result != totalChildren {
		t.Errorf("expected %d completed, got %d", totalChildren, result)
	}
	if step1Calls.Load() != int32(totalChildren) {
		t.Errorf("expected %d step1 calls, got %d", totalChildren, step1Calls.Load())
	}
	if step2Calls.Load() != int32(totalChildren) {
		t.Errorf("expected %d step2 calls, got %d", totalChildren, step2Calls.Load())
	}
	if step3Calls.Load() != int32(totalChildren) {
		t.Errorf("expected %d step3 calls, got %d", totalChildren, step3Calls.Load())
	}
}

// TestSlidingWindowContinueAsNewWithChildren tests the combination of
// sliding window children and ContinueAsNew (exactly like filesystem analyzer).
func TestSlidingWindowContinueAsNewWithChildren(t *testing.T) {
	store := derecho.NewMemoryStore()
	engine := mustEngine(t, store)

	var childCompletions atomic.Int32
	var continueAsNewCount atomic.Int32

	childRef := derecho.NewChildWorkflowRef[int, int]("batch-child")

	derecho.RegisterWorkflow(engine, "batch-child", func(ctx derecho.Context, id int) (int, error) {
		childCompletions.Add(1)
		return id, nil
	})

	type BatchInput struct {
		TotalItems     int
		BatchSize      int
		MaxConcurrent  int
		ProcessedSoFar int
	}

	derecho.RegisterWorkflow(engine, "batch-orchestrator", func(ctx derecho.Context, input BatchInput) (int, error) {
		type activeChild struct {
			id     int
			future derecho.Future[int]
		}

		var active []activeChild
		nextIdx := input.ProcessedSoFar
		var batchCompleted int

		batchEnd := input.ProcessedSoFar + input.BatchSize
		if batchEnd > input.TotalItems {
			batchEnd = input.TotalItems
		}

		for nextIdx < batchEnd && len(active) < input.MaxConcurrent {
			workflowID := fmt.Sprintf("child-%d", nextIdx)
			active = append(active, activeChild{
				id:     nextIdx,
				future: childRef.Execute(ctx, workflowID, nextIdx),
			})
			nextIdx++
		}

		for len(active) > 0 {
			sel := derecho.NewSelector()
			completedIdx := -1

			for i := range active {
				idx := i
				derecho.AddFuture(sel, active[idx].future, func(_ int, _ error) {
					completedIdx = idx
				})
			}

			sel.Select(ctx)

			if completedIdx >= 0 && completedIdx < len(active) {
				active = append(active[:completedIdx], active[completedIdx+1:]...)
				batchCompleted++

				if nextIdx < batchEnd {
					workflowID := fmt.Sprintf("child-%d", nextIdx)
					active = append(active, activeChild{
						id:     nextIdx,
						future: childRef.Execute(ctx, workflowID, nextIdx),
					})
					nextIdx++
				}
			}
		}

		if batchEnd < input.TotalItems {
			continueAsNewCount.Add(1)
			return 0, derecho.NewContinueAsNewError(BatchInput{
				TotalItems:     input.TotalItems,
				BatchSize:      input.BatchSize,
				MaxConcurrent:  input.MaxConcurrent,
				ProcessedSoFar: batchEnd,
			})
		}

		return batchCompleted, nil
	})

	client := engine.Client()
	worker := engine.WorkflowWorker()

	_, err := client.StartWorkflow(t.Context(), "batch-orchestrator", "parent-1", BatchInput{
		TotalItems:    30,
		BatchSize:     10,
		MaxConcurrent: 3,
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	go runWorkers(ctx, worker, nil)

	// Wait for all children to complete (workflow uses ContinueAsNew, so run.Get won't work)
	for childCompletions.Load() < 30 {
		select {
		case <-ctx.Done():
			t.Fatalf("timeout waiting for children, got %d", childCompletions.Load())
		case <-time.After(10 * time.Millisecond):
		}
	}

	cancel()

	if childCompletions.Load() != 30 {
		t.Errorf("expected 30 child completions, got %d", childCompletions.Load())
	}
	if continueAsNewCount.Load() != 2 {
		t.Errorf("expected 2 ContinueAsNew calls (3 batches), got %d", continueAsNewCount.Load())
	}
}

// TestSQLiteMultipleChildWorkflowsComplete verifies that futures get correct
// scheduled IDs when using SQLiteStore. The bug: provisional IDs diverge from
// real IDs because ChildWorkflowStarted events are interleaved during Append.
// Without the fix, futures check for wrong IDs and never find their completions.
func TestSQLiteMultipleChildWorkflowsComplete(t *testing.T) {
	store, err := derechosqlite.New(":memory:")
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	engine := mustEngine(t, store)

	var childCompletions atomic.Int32

	childRef := derecho.NewChildWorkflowRef[int, int]("simple-child")

	derecho.RegisterWorkflow(engine, "simple-child", func(ctx derecho.Context, id int) (int, error) {
		childCompletions.Add(1)
		return id * 2, nil
	})

	derecho.RegisterWorkflow(engine, "multi-child-parent", func(ctx derecho.Context, childCount int) (int, error) {
		var futures []derecho.Future[int]
		for i := 0; i < childCount; i++ {
			workflowID := fmt.Sprintf("child-%d", i)
			futures = append(futures, childRef.Execute(ctx, workflowID, i))
		}

		var total int
		for _, f := range futures {
			result, err := f.Get(ctx)
			if err != nil {
				return 0, err
			}
			total += result
		}
		return total, nil
	})

	client := engine.Client()
	worker := engine.WorkflowWorker()

	const childCount = 5
	run, err := client.StartWorkflow(t.Context(), "multi-child-parent", "parent-1", childCount)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	go runWorkers(ctx, worker, nil)

	var result int
	for {
		err = run.Get(ctx, &result, derecho.NonBlocking())
		if err == derecho.ErrNotCompleted {
			continue
		}
		if err != nil {
			t.Fatalf("workflow failed: %v", err)
		}
		break
	}

	cancel()

	// 0*2 + 1*2 + 2*2 + 3*2 + 4*2 = 20
	expectedTotal := 0
	for i := 0; i < childCount; i++ {
		expectedTotal += i * 2
	}
	if result != expectedTotal {
		t.Errorf("expected total %d, got %d", expectedTotal, result)
	}
	if childCompletions.Load() != int32(childCount) {
		t.Errorf("expected %d child completions, got %d", childCount, childCompletions.Load())
	}
}
