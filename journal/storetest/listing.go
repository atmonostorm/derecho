package storetest

import (
	"context"
	"testing"
	"time"

	"github.com/atmonostorm/derecho/journal"
)

func testListing(t *testing.T, factory Factory) {
	t.Run("Empty", testListWorkflowsEmpty(factory))
	t.Run("Basic", testListWorkflowsBasic(factory))
	t.Run("FilterByStatus", testListWorkflowsFilterByStatus(factory))
	t.Run("FilterByType", testListWorkflowsFilterByType(factory))
	t.Run("FilterByWorkflowID", testListWorkflowsFilterByWorkflowID(factory))
	t.Run("Pagination", testListWorkflowsPagination(factory))
	t.Run("Ordering", testListWorkflowsOrdering(factory))
	t.Run("InputAndCompletedAt", testListWorkflowsInputAndCompletedAt(factory))
}

func testListWorkflowsEmpty(factory Factory) func(t *testing.T) {
	return func(t *testing.T) {
		store, _ := newTestStore(t, factory)
		ctx := context.Background()

		result, err := store.ListWorkflows(ctx)
		if err != nil {
			t.Fatalf("ListWorkflows failed: %v", err)
		}

		if len(result.Executions) != 0 {
			t.Errorf("expected 0 executions, got %d", len(result.Executions))
		}
		if result.NextCursor != "" {
			t.Errorf("expected empty cursor, got %q", result.NextCursor)
		}
	}
}

func testListWorkflowsBasic(factory Factory) func(t *testing.T) {
	return func(t *testing.T) {
		store, clock := newTestStore(t, factory)
		ctx := context.Background()

		runID1, err := store.CreateWorkflow(ctx, "wf-1", "TypeA", nil, clock.Now())
		if err != nil {
			t.Fatalf("CreateWorkflow 1 failed: %v", err)
		}

		clock.Advance(time.Second)
		runID2, err := store.CreateWorkflow(ctx, "wf-2", "TypeB", nil, clock.Now())
		if err != nil {
			t.Fatalf("CreateWorkflow 2 failed: %v", err)
		}

		result, err := store.ListWorkflows(ctx)
		if err != nil {
			t.Fatalf("ListWorkflows failed: %v", err)
		}

		if len(result.Executions) != 2 {
			t.Fatalf("expected 2 executions, got %d", len(result.Executions))
		}

		// Newest first
		if result.Executions[0].WorkflowID != "wf-2" || result.Executions[0].RunID != runID2 {
			t.Errorf("first execution = %s/%s, want wf-2/%s", result.Executions[0].WorkflowID, result.Executions[0].RunID, runID2)
		}
		if result.Executions[1].WorkflowID != "wf-1" || result.Executions[1].RunID != runID1 {
			t.Errorf("second execution = %s/%s, want wf-1/%s", result.Executions[1].WorkflowID, result.Executions[1].RunID, runID1)
		}

		// Check fields
		if result.Executions[0].WorkflowType != "TypeB" {
			t.Errorf("workflow type = %s, want TypeB", result.Executions[0].WorkflowType)
		}
		if result.Executions[0].Status != journal.WorkflowStatusRunning {
			t.Errorf("status = %v, want Running", result.Executions[0].Status)
		}
	}
}

func testListWorkflowsFilterByStatus(factory Factory) func(t *testing.T) {
	return func(t *testing.T) {
		store, clock := newTestStore(t, factory)
		ctx := context.Background()

		runID1, _ := store.CreateWorkflow(ctx, "wf-1", "Type", nil, clock.Now())
		clock.Advance(time.Second)
		store.CreateWorkflow(ctx, "wf-2", "Type", nil, clock.Now())

		// Complete wf-1
		store.Append(ctx, "wf-1", runID1, []journal.Event{
			journal.WorkflowCompleted{BaseEvent: journal.BaseEvent{ID: 3}},
		}, 2)

		// Filter for running only
		result, err := store.ListWorkflows(ctx, journal.WithStatus(journal.WorkflowStatusRunning))
		if err != nil {
			t.Fatalf("ListWorkflows failed: %v", err)
		}

		if len(result.Executions) != 1 {
			t.Fatalf("expected 1 running execution, got %d", len(result.Executions))
		}
		if result.Executions[0].WorkflowID != "wf-2" {
			t.Errorf("workflow = %s, want wf-2", result.Executions[0].WorkflowID)
		}

		// Filter for completed only
		result, err = store.ListWorkflows(ctx, journal.WithStatus(journal.WorkflowStatusCompleted))
		if err != nil {
			t.Fatalf("ListWorkflows failed: %v", err)
		}

		if len(result.Executions) != 1 {
			t.Fatalf("expected 1 completed execution, got %d", len(result.Executions))
		}
		if result.Executions[0].WorkflowID != "wf-1" {
			t.Errorf("workflow = %s, want wf-1", result.Executions[0].WorkflowID)
		}
	}
}

func testListWorkflowsFilterByType(factory Factory) func(t *testing.T) {
	return func(t *testing.T) {
		store, clock := newTestStore(t, factory)
		ctx := context.Background()

		store.CreateWorkflow(ctx, "wf-1", "TypeA", nil, clock.Now())
		clock.Advance(time.Second)
		store.CreateWorkflow(ctx, "wf-2", "TypeB", nil, clock.Now())
		clock.Advance(time.Second)
		store.CreateWorkflow(ctx, "wf-3", "TypeA", nil, clock.Now())

		result, err := store.ListWorkflows(ctx, journal.WithWorkflowType("TypeA"))
		if err != nil {
			t.Fatalf("ListWorkflows failed: %v", err)
		}

		if len(result.Executions) != 2 {
			t.Fatalf("expected 2 TypeA executions, got %d", len(result.Executions))
		}

		for _, ex := range result.Executions {
			if ex.WorkflowType != "TypeA" {
				t.Errorf("got type %s, want TypeA", ex.WorkflowType)
			}
		}
	}
}

func testListWorkflowsFilterByWorkflowID(factory Factory) func(t *testing.T) {
	return func(t *testing.T) {
		store, clock := newTestStore(t, factory)
		ctx := context.Background()

		store.CreateWorkflow(ctx, "wf-1", "Type", nil, clock.Now())
		clock.Advance(time.Second)
		store.CreateWorkflow(ctx, "wf-2", "Type", nil, clock.Now())

		result, err := store.ListWorkflows(ctx, journal.WithWorkflowID("wf-1"))
		if err != nil {
			t.Fatalf("ListWorkflows failed: %v", err)
		}

		if len(result.Executions) != 1 {
			t.Fatalf("expected 1 execution, got %d", len(result.Executions))
		}
		if result.Executions[0].WorkflowID != "wf-1" {
			t.Errorf("workflow = %s, want wf-1", result.Executions[0].WorkflowID)
		}
	}
}

func testListWorkflowsPagination(factory Factory) func(t *testing.T) {
	return func(t *testing.T) {
		store, clock := newTestStore(t, factory)
		ctx := context.Background()

		// Create 5 workflows
		for i := 0; i < 5; i++ {
			store.CreateWorkflow(ctx, "wf-"+string(rune('a'+i)), "Type", nil, clock.Now())
			clock.Advance(time.Second)
		}

		// Page 1: limit 2
		result, err := store.ListWorkflows(ctx, journal.WithLimit(2))
		if err != nil {
			t.Fatalf("ListWorkflows page 1 failed: %v", err)
		}

		if len(result.Executions) != 2 {
			t.Fatalf("page 1: expected 2 executions, got %d", len(result.Executions))
		}
		if result.NextCursor == "" {
			t.Fatal("page 1: expected cursor, got empty")
		}
		// Newest first: wf-e, wf-d
		if result.Executions[0].WorkflowID != "wf-e" {
			t.Errorf("page 1 first = %s, want wf-e", result.Executions[0].WorkflowID)
		}
		if result.Executions[1].WorkflowID != "wf-d" {
			t.Errorf("page 1 second = %s, want wf-d", result.Executions[1].WorkflowID)
		}

		// Page 2
		result, err = store.ListWorkflows(ctx, journal.WithLimit(2), journal.WithCursor(result.NextCursor))
		if err != nil {
			t.Fatalf("ListWorkflows page 2 failed: %v", err)
		}

		if len(result.Executions) != 2 {
			t.Fatalf("page 2: expected 2 executions, got %d", len(result.Executions))
		}
		if result.Executions[0].WorkflowID != "wf-c" {
			t.Errorf("page 2 first = %s, want wf-c", result.Executions[0].WorkflowID)
		}
		if result.Executions[1].WorkflowID != "wf-b" {
			t.Errorf("page 2 second = %s, want wf-b", result.Executions[1].WorkflowID)
		}

		// Page 3: last page
		result, err = store.ListWorkflows(ctx, journal.WithLimit(2), journal.WithCursor(result.NextCursor))
		if err != nil {
			t.Fatalf("ListWorkflows page 3 failed: %v", err)
		}

		if len(result.Executions) != 1 {
			t.Fatalf("page 3: expected 1 execution, got %d", len(result.Executions))
		}
		if result.Executions[0].WorkflowID != "wf-a" {
			t.Errorf("page 3 first = %s, want wf-a", result.Executions[0].WorkflowID)
		}
		if result.NextCursor != "" {
			t.Errorf("page 3: expected empty cursor, got %q", result.NextCursor)
		}
	}
}

func testListWorkflowsOrdering(factory Factory) func(t *testing.T) {
	return func(t *testing.T) {
		store, clock := newTestStore(t, factory)
		ctx := context.Background()

		// Create workflows with same timestamp but different IDs
		store.CreateWorkflow(ctx, "wf-c", "Type", nil, clock.Now())
		store.CreateWorkflow(ctx, "wf-a", "Type", nil, clock.Now())
		store.CreateWorkflow(ctx, "wf-b", "Type", nil, clock.Now())

		result, err := store.ListWorkflows(ctx)
		if err != nil {
			t.Fatalf("ListWorkflows failed: %v", err)
		}

		if len(result.Executions) != 3 {
			t.Fatalf("expected 3 executions, got %d", len(result.Executions))
		}

		// Same timestamp, so sorted by workflow_id DESC
		expected := []string{"wf-c", "wf-b", "wf-a"}
		for i, ex := range result.Executions {
			if ex.WorkflowID != expected[i] {
				t.Errorf("position %d: got %s, want %s", i, ex.WorkflowID, expected[i])
			}
		}
	}
}

func testListWorkflowsInputAndCompletedAt(factory Factory) func(t *testing.T) {
	return func(t *testing.T) {
		store, clock := newTestStore(t, factory)
		ctx := context.Background()

		// Create workflow with input data
		inputData := []byte(`{"file_id":"abc-123"}`)
		runID, err := store.CreateWorkflow(ctx, "wf-with-input", "TestWorkflow", inputData, clock.Now())
		if err != nil {
			t.Fatalf("CreateWorkflow failed: %v", err)
		}

		// Verify input before completion
		result, err := store.ListWorkflows(ctx)
		if err != nil {
			t.Fatalf("ListWorkflows failed: %v", err)
		}

		if len(result.Executions) != 1 {
			t.Fatalf("expected 1 execution, got %d", len(result.Executions))
		}

		ex := result.Executions[0]
		if string(ex.Input) != string(inputData) {
			t.Errorf("Input = %q, want %q", ex.Input, inputData)
		}
		if ex.CompletedAt != nil {
			t.Errorf("CompletedAt should be nil for running workflow, got %v", ex.CompletedAt)
		}

		// Advance clock and complete the workflow
		clock.Advance(5 * time.Minute)
		completionTime := clock.Now()

		_, err = store.Append(ctx, "wf-with-input", runID, []journal.Event{
			journal.WorkflowCompleted{BaseEvent: journal.BaseEvent{ID: 3}},
		}, 2)
		if err != nil {
			t.Fatalf("Append WorkflowCompleted failed: %v", err)
		}

		// Verify CompletedAt is set after completion
		result, err = store.ListWorkflows(ctx)
		if err != nil {
			t.Fatalf("ListWorkflows after completion failed: %v", err)
		}

		if len(result.Executions) != 1 {
			t.Fatalf("expected 1 execution, got %d", len(result.Executions))
		}

		ex = result.Executions[0]
		if ex.Status != journal.WorkflowStatusCompleted {
			t.Errorf("Status = %v, want Completed", ex.Status)
		}
		if ex.CompletedAt == nil {
			t.Error("CompletedAt should be set for completed workflow")
		} else if !ex.CompletedAt.Equal(completionTime) {
			t.Errorf("CompletedAt = %v, want %v", *ex.CompletedAt, completionTime)
		}
		// Input should still be preserved after completion
		if string(ex.Input) != string(inputData) {
			t.Errorf("Input after completion = %q, want %q", ex.Input, inputData)
		}
	}
}
