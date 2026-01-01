package derecho

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// TestChildWorkflowHang reproduces a hang when parent workflow uses
// sliding window concurrency with selectors to process many child workflows.
func TestChildWorkflowHang(t *testing.T) {
	store := NewMemoryStore()
	engine := mustEngine(t, store,
		WithWorkerConcurrency(3),
		WithCacheSize(100),
	)

	var childCompletions atomic.Int32

	// Register a simple child workflow that completes immediately
	err := RegisterWorkflow(engine, "ChildWorkflow", func(ctx Context, id int) (struct{}, error) {
		t.Logf("Child %d executing", id)
		childCompletions.Add(1)
		return struct{}{}, nil
	})
	if err != nil {
		t.Fatalf("failed to register child workflow: %v", err)
	}

	// Register parent workflow that spawns 10 children with selector
	err = RegisterWorkflow(engine, "ParentWorkflow", func(ctx Context, count int) (int, error) {
		childRef := NewChildWorkflowRef[int, struct{}]("ChildWorkflow")
		const maxConcurrent = 10

		type activeChild struct {
			id     int
			future Future[struct{}]
		}
		var active []activeChild
		nextIdx := 0
		completed := 0

		// Start initial batch
		for nextIdx < count && len(active) < maxConcurrent {
			workflowID := fmt.Sprintf("child-%d", nextIdx)
			t.Logf("Parent: scheduling child %d", nextIdx)
			active = append(active, activeChild{
				id:     nextIdx,
				future: childRef.Execute(ctx, workflowID, nextIdx),
			})
			nextIdx++
		}

		// Process completions with selector
		for len(active) > 0 {
			t.Logf("Parent: entering selector with %d active children", len(active))
			sel := NewSelector()
			completedIdx := -1

			for i := range active {
				idx := i
				AddFuture(sel, active[idx].future, func(_ struct{}, err error) {
					completedIdx = idx
					t.Logf("Parent: child %d completed", active[idx].id)
				})
			}

			sel.Select(ctx)

			if completedIdx >= 0 && completedIdx < len(active) {
				active = append(active[:completedIdx], active[completedIdx+1:]...)
				completed++
				if nextIdx < count {
					workflowID := fmt.Sprintf("child-%d", nextIdx)
					t.Logf("Parent: scheduling child %d (after completion)", nextIdx)
					active = append(active, activeChild{
						id:     nextIdx,
						future: childRef.Execute(ctx, workflowID, nextIdx),
					})
					nextIdx++
				}
			}
		}

		t.Logf("Parent: all %d children completed", completed)
		return completed, nil
	})
	if err != nil {
		t.Fatalf("failed to register parent workflow: %v", err)
	}

	// Start the parent workflow
	client := engine.Client()
	run, err := client.StartWorkflow(context.Background(), "ParentWorkflow", "test-parent", 10)
	if err != nil {
		t.Fatalf("failed to start workflow: %v", err)
	}

	// Run engine with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	go engine.Run(ctx)

	// Wait for completion with periodic status
	done := make(chan error, 1)
	go func() {
		var result int
		err := run.Get(ctx, &result)
		if err == nil && result != 10 {
			err = fmt.Errorf("expected 10 completions, got %d", result)
		}
		done <- err
	}()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("workflow failed: %v (child completions: %d)", err, childCompletions.Load())
			}
			t.Logf("SUCCESS: all children completed (count: %d)", childCompletions.Load())
			return
		case <-ticker.C:
			t.Logf("STATUS: child completions so far: %d", childCompletions.Load())
		case <-ctx.Done():
			t.Fatalf("timeout: child completions: %d", childCompletions.Load())
		}
	}
}
