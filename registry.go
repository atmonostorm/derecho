package derecho

import (
	"fmt"
	"sync"
)

// Registrar allows registration of workflows and activities. Sealed via unexported
// methods - use RegisterWorkflow and RegisterActivity with an Engine instance.
type Registrar interface {
	registerWorkflow(name string, fn any) error
	registerActivity(name string, fn any) error
}

// Registry is the default WorkflowResolver and ActivityResolver implementation.
// It provides simple name-based lookup. Users can substitute custom resolvers
// for dynamic loading, versioning, multi-tenancy, or other routing strategies.
type Registry struct {
	mu      sync.RWMutex
	entries map[string]any
	kind    string
}

func newRegistry(kind string) *Registry {
	return &Registry{
		entries: make(map[string]any),
		kind:    kind,
	}
}

func (r *Registry) register(name string, fn any) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.entries[name]; exists {
		return fmt.Errorf("derecho: %s %q already registered", r.kind, name)
	}
	r.entries[name] = fn
	return nil
}

func (r *Registry) resolve(name string) (any, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	fn, ok := r.entries[name]
	return fn, ok
}

// ResolveWorkflow implements WorkflowResolver.
func (r *Registry) ResolveWorkflow(name string) (any, bool) { return r.resolve(name) }

// ResolveActivity implements ActivityResolver.
func (r *Registry) ResolveActivity(name string) (any, bool) { return r.resolve(name) }

// RegisterWorkflow registers a workflow function with compile-time type safety.
// The function must have signature: func(Context, I) (O, error)
func RegisterWorkflow[I, O any](r Registrar, name string, wf Workflow[I, O]) error {
	return r.registerWorkflow(name, wf)
}

// RegisterActivity registers an activity function with compile-time type safety.
// The function must have signature: func(context.Context, I) (O, error)
func RegisterActivity[I, O any](r Registrar, name string, fn Activity[I, O]) error {
	return r.registerActivity(name, fn)
}
