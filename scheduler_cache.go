package derecho

import (
	"container/list"
	"sync"
)

const defaultCacheSize = 1000

type cachedScheduler struct {
	sched *Scheduler
	state *executionState
}

func (cs *cachedScheduler) hasPendingWork() bool {
	return cs.state.HasPendingWork()
}

type schedulerCache struct {
	mu      sync.Mutex
	entries map[string]*cacheEntry
	lru     *list.List
	maxSize int
}

type cacheEntry struct {
	key     string
	cs      *cachedScheduler
	element *list.Element
}

func newSchedulerCache(maxSize int) *schedulerCache {
	if maxSize <= 0 {
		maxSize = defaultCacheSize
	}
	return &schedulerCache{
		entries: make(map[string]*cacheEntry),
		lru:     list.New(),
		maxSize: maxSize,
	}
}

func (c *schedulerCache) Get(workflowID, runID string) *cachedScheduler {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := workflowID + "/" + runID
	entry, ok := c.entries[key]
	if !ok {
		return nil
	}
	c.lru.MoveToFront(entry.element)
	return entry.cs
}

// Put adds or updates a cached scheduler. Returns false if the cache is full
// and all entries have pending work (backpressure).
func (c *schedulerCache) Put(workflowID, runID string, cs *cachedScheduler) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := workflowID + "/" + runID

	if entry, ok := c.entries[key]; ok {
		entry.cs = cs
		c.lru.MoveToFront(entry.element)
		return true
	}

	if len(c.entries) >= c.maxSize {
		if !c.evictColdest() {
			return false
		}
	}

	entry := &cacheEntry{key: key, cs: cs}
	entry.element = c.lru.PushFront(entry)
	c.entries[key] = entry
	return true
}

func (c *schedulerCache) Remove(workflowID, runID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := workflowID + "/" + runID
	entry, ok := c.entries[key]
	if !ok {
		return
	}

	if entry.cs.sched != nil {
		entry.cs.sched.Close()
	}
	c.lru.Remove(entry.element)
	delete(c.entries, key)
}

// AvailableSlots returns how many new workflows can be cached.
// This is free slots plus cold (evictable) entries.
func (c *schedulerCache) AvailableSlots() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	free := c.maxSize - len(c.entries)
	if free > 0 {
		return free
	}

	// Count cold entries we could evict
	cold := 0
	elem := c.lru.Back()
	for i := 0; i < min(16, c.lru.Len()) && elem != nil; i++ {
		if !elem.Value.(*cacheEntry).cs.hasPendingWork() {
			cold++
		}
		elem = elem.Prev()
	}
	return cold
}

// evictColdest scans up to 16 entries from LRU tail for one without pending work.
// Returns true if eviction succeeded, false if all scanned entries are hot.
func (c *schedulerCache) evictColdest() bool {
	scanLimit := min(16, c.lru.Len())
	elem := c.lru.Back()

	for i := 0; i < scanLimit && elem != nil; i++ {
		entry := elem.Value.(*cacheEntry)
		if !entry.cs.hasPendingWork() {
			if entry.cs.sched != nil {
				entry.cs.sched.Close()
			}
			c.lru.Remove(elem)
			delete(c.entries, entry.key)
			return true
		}
		elem = elem.Prev()
	}
	return false
}
