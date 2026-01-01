package derecho

import (
	"testing"

	"github.com/atmonostorm/derecho/journal"
	"github.com/atmonostorm/derecho/journal/storetest"
)

func TestMemoryStore_Conformance(t *testing.T) {
	storetest.Run(t, func(t *testing.T, clock storetest.Clock) journal.Store {
		store := NewMemoryStore(WithStoreClock(clockAdapter{clock}))
		return store
	})
}

type clockAdapter struct {
	storetest.Clock
}
