package derecho

import (
	"testing"
	"time"

	"github.com/atmonostorm/derecho/journal"
)

func testCachedScheduler(events ...journal.Event) *cachedScheduler {
	state := newExecutionState("wf", "run", events, nil)
	return &cachedScheduler{state: state}
}

func TestSchedulerCache_GetPut(t *testing.T) {
	c := newSchedulerCache(10)

	cs := testCachedScheduler()
	if !c.Put("wf-1", "run-1", cs) {
		t.Fatal("Put should succeed")
	}

	got := c.Get("wf-1", "run-1")
	if got != cs {
		t.Error("expected to get cached scheduler")
	}

	got = c.Get("wf-2", "run-2")
	if got != nil {
		t.Error("expected nil for missing key")
	}
}

func TestSchedulerCache_LRUEviction(t *testing.T) {
	c := newSchedulerCache(2)

	cs1 := testCachedScheduler()
	cs2 := testCachedScheduler()
	cs3 := testCachedScheduler()

	c.Put("wf-1", "run-1", cs1)
	c.Put("wf-2", "run-2", cs2)
	c.Put("wf-3", "run-3", cs3)

	if c.Get("wf-1", "run-1") != nil {
		t.Error("wf-1 should have been evicted")
	}
	if c.Get("wf-2", "run-2") != cs2 {
		t.Error("wf-2 should still be cached")
	}
	if c.Get("wf-3", "run-3") != cs3 {
		t.Error("wf-3 should be cached")
	}
}

func TestSchedulerCache_LRUReorder(t *testing.T) {
	c := newSchedulerCache(2)

	cs1 := testCachedScheduler()
	cs2 := testCachedScheduler()
	cs3 := testCachedScheduler()

	c.Put("wf-1", "run-1", cs1)
	c.Put("wf-2", "run-2", cs2)

	c.Get("wf-1", "run-1")

	c.Put("wf-3", "run-3", cs3)

	if c.Get("wf-1", "run-1") != cs1 {
		t.Error("wf-1 should still be cached (recently accessed)")
	}
	if c.Get("wf-2", "run-2") != nil {
		t.Error("wf-2 should have been evicted")
	}
	if c.Get("wf-3", "run-3") != cs3 {
		t.Error("wf-3 should be cached")
	}
}

func TestSchedulerCache_Remove(t *testing.T) {
	c := newSchedulerCache(10)

	cs := testCachedScheduler()
	c.Put("wf-1", "run-1", cs)

	c.Remove("wf-1", "run-1")

	if c.Get("wf-1", "run-1") != nil {
		t.Error("expected nil after Remove")
	}
}

func TestSchedulerCache_UpdateExisting(t *testing.T) {
	c := newSchedulerCache(10)

	cs1 := testCachedScheduler()
	cs2 := testCachedScheduler()

	c.Put("wf-1", "run-1", cs1)
	c.Put("wf-1", "run-1", cs2)

	got := c.Get("wf-1", "run-1")
	if got != cs2 {
		t.Error("expected updated scheduler")
	}
}

func TestSchedulerCache_EvictionClosesScheduler(t *testing.T) {
	c := newSchedulerCache(1)

	state := NewStubExecutionState()
	info := WorkflowInfo{WorkflowID: "wf-1", RunID: "run-1", WorkflowType: "Test", StartTime: time.Now()}
	s := NewScheduler(state, func(ctx Context) {}, info)

	execState := newExecutionState("wf-1", "run-1", nil, nil)
	cs := &cachedScheduler{sched: s, state: execState}
	c.Put("wf-1", "run-1", cs)

	c.Put("wf-2", "run-2", testCachedScheduler())

	if s.fibers != nil {
		t.Error("expected scheduler to be closed on eviction")
	}
}

func TestSchedulerCache_RemoveClosesScheduler(t *testing.T) {
	c := newSchedulerCache(10)

	state := NewStubExecutionState()
	info := WorkflowInfo{WorkflowID: "wf-1", RunID: "run-1", WorkflowType: "Test", StartTime: time.Now()}
	s := NewScheduler(state, func(ctx Context) {}, info)

	execState := newExecutionState("wf-1", "run-1", nil, nil)
	cs := &cachedScheduler{sched: s, state: execState}
	c.Put("wf-1", "run-1", cs)
	c.Remove("wf-1", "run-1")

	if s.fibers != nil {
		t.Error("expected scheduler to be closed on Remove")
	}
}

func TestSchedulerCache_HotWorkflowProtected(t *testing.T) {
	c := newSchedulerCache(2)

	// Create a workflow with pending activity (hot)
	hotEvents := []journal.Event{
		journal.ActivityScheduled{BaseEvent: journal.BaseEvent{ID: 1}},
	}
	hotCS := testCachedScheduler(hotEvents...)

	// Create a cold workflow (no pending work)
	coldCS := testCachedScheduler()

	c.Put("hot", "run", hotCS)
	c.Put("cold", "run", coldCS)

	// Add third workflow - should evict cold, not hot
	c.Put("new", "run", testCachedScheduler())

	if c.Get("hot", "run") != hotCS {
		t.Error("hot workflow should be protected from eviction")
	}
	if c.Get("cold", "run") != nil {
		t.Error("cold workflow should have been evicted")
	}
}

func TestSchedulerCache_BackpressureWhenAllHot(t *testing.T) {
	c := newSchedulerCache(2)

	// Fill cache with hot workflows
	hot1 := testCachedScheduler(journal.ActivityScheduled{BaseEvent: journal.BaseEvent{ID: 1}})
	hot2 := testCachedScheduler(journal.TimerScheduled{BaseEvent: journal.BaseEvent{ID: 1}})

	c.Put("hot1", "run", hot1)
	c.Put("hot2", "run", hot2)

	// Try to add another - should fail (backpressure)
	newCS := testCachedScheduler()
	if c.Put("new", "run", newCS) {
		t.Error("Put should return false when cache full of hot workflows")
	}

	// Original entries should still be there
	if c.Get("hot1", "run") != hot1 {
		t.Error("hot1 should still be cached")
	}
	if c.Get("hot2", "run") != hot2 {
		t.Error("hot2 should still be cached")
	}
}

func TestSchedulerCache_CompletedActivityNotHot(t *testing.T) {
	c := newSchedulerCache(2)

	// Workflow with completed activity (not hot)
	events := []journal.Event{
		journal.ActivityScheduled{BaseEvent: journal.BaseEvent{ID: 1}},
		journal.ActivityCompleted{BaseEvent: journal.BaseEvent{ID: 2, ScheduledByID: 1}},
	}
	cs := testCachedScheduler(events...)

	c.Put("wf1", "run", cs)
	c.Put("wf2", "run", testCachedScheduler())

	// Third should evict wf1 (not hot despite having activity)
	c.Put("wf3", "run", testCachedScheduler())

	if c.Get("wf1", "run") != nil {
		t.Error("completed workflow should be evictable")
	}
}
