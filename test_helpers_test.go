package derecho

import (
	"io"
	"log/slog"
	"testing"

	"github.com/atmonostorm/derecho/journal"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func mustEngine(t *testing.T, store journal.Store, opts ...EngineOption) *Engine {
	opts = append(opts, WithEngineLogger(testLogger()))
	engine, err := NewEngine(store, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return engine
}
