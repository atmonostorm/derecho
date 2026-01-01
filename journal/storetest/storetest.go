// Package storetest provides conformance tests for journal.Store implementations.
//
// Usage:
//
//	func TestMyStore_Conformance(t *testing.T) {
//	    storetest.Run(t, func(t *testing.T, clock storetest.Clock) journal.Store {
//	        store := NewMyStore(WithClock(clock))
//	        t.Cleanup(func() { store.Close() })
//	        return store
//	    })
//	}
package storetest

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/atmonostorm/derecho/journal"
)

type Clock interface {
	Now() time.Time
}

type FakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func NewFakeClock(t time.Time) *FakeClock {
	return &FakeClock{now: t}
}

func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func (c *FakeClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = t
}

// Clock must be wired into the store's time source. Each call must return an
// independent store with no shared state.
type Factory func(t *testing.T, clock Clock) journal.Store

func Run(t *testing.T, factory Factory) {
	t.Run("Persistence", func(t *testing.T) { testPersistence(t, factory) })
	t.Run("Lifecycle", func(t *testing.T) { testLifecycle(t, factory) })
	t.Run("WorkflowTasks", func(t *testing.T) { testWorkflowTasks(t, factory) })
	t.Run("ActivityTasks", func(t *testing.T) { testActivityTasks(t, factory) })
	t.Run("Timers", func(t *testing.T) { testTimers(t, factory) })
	t.Run("Timeouts", func(t *testing.T) { testTimeouts(t, factory) })
	t.Run("Signals", func(t *testing.T) { testSignals(t, factory) })
	t.Run("Cancel", func(t *testing.T) { testCancel(t, factory) })
	t.Run("ChildWorkflows", func(t *testing.T) { testChildWorkflows(t, factory) })
	t.Run("Listing", func(t *testing.T) { testListing(t, factory) })
}

var baseTime = time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)

func newTestStore(t *testing.T, factory Factory) (journal.Store, *FakeClock) {
	clock := NewFakeClock(baseTime)
	store := factory(t, clock)
	return store, clock
}

func assertWakesOnWork[T any](t *testing.T, wait func(context.Context) (T, error), createWork func()) T {
	t.Helper()

	resultCh := make(chan T, 1)
	errCh := make(chan error, 1)

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		result, err := wait(ctx)
		if err != nil {
			errCh <- err
			return
		}
		resultCh <- result
	}()

	time.Sleep(50 * time.Millisecond)
	createWork()

	select {
	case result := <-resultCh:
		return result
	case err := <-errCh:
		t.Fatalf("wait failed: %v", err)
	case <-time.After(1 * time.Second):
		t.Fatal("wait did not wake up after work created")
	}
	panic("unreachable")
}
