package derecho

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/atmonostorm/derecho/journal"
)

type Engine struct {
	store      journal.Store
	workflows  WorkflowResolver
	activities ActivityResolver

	// Default registries for Registrar implementation.
	// Nil if custom resolvers were provided.
	workflowRegistry *Registry
	activityRegistry *Registry

	cache              *schedulerCache
	workerID           string
	codec              Codec
	clock              Clock
	defaultRetryPolicy *RetryPolicy
}

type engineConfig struct {
	workerID           string
	cacheSize          int
	codec              Codec
	clock              Clock
	workflowResolver   WorkflowResolver
	activityResolver   ActivityResolver
	defaultRetryPolicy *RetryPolicy
}

type EngineOption func(*engineConfig)

func WithWorkerID(id string) EngineOption {
	return func(c *engineConfig) {
		c.workerID = id
	}
}

func WithCacheSize(size int) EngineOption {
	return func(c *engineConfig) {
		c.cacheSize = size
	}
}

func WithEngineCodec(codec Codec) EngineOption {
	return func(c *engineConfig) {
		c.codec = codec
	}
}

func WithClock(clock Clock) EngineOption {
	return func(c *engineConfig) {
		c.clock = clock
	}
}

// WithWorkflowResolver sets a custom workflow resolver.
// When set, RegisterWorkflow cannot be used on this engine.
func WithWorkflowResolver(r WorkflowResolver) EngineOption {
	return func(c *engineConfig) {
		c.workflowResolver = r
	}
}

// WithActivityResolver sets a custom activity resolver.
// When set, RegisterActivity cannot be used on this engine.
func WithActivityResolver(r ActivityResolver) EngineOption {
	return func(c *engineConfig) {
		c.activityResolver = r
	}
}

// WithDefaultRetryPolicy sets the default retry policy for activities.
func WithDefaultRetryPolicy(p RetryPolicy) EngineOption {
	return func(c *engineConfig) {
		c.defaultRetryPolicy = &p
	}
}

func NewEngine(store journal.Store, opts ...EngineOption) *Engine {
	cfg := engineConfig{
		workerID:  randomID(),
		cacheSize: defaultCacheSize,
		codec:     DefaultCodec,
		clock:     RealClock{},
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	e := &Engine{
		store:              store,
		cache:              newSchedulerCache(cfg.cacheSize),
		workerID:           cfg.workerID,
		codec:              cfg.codec,
		clock:              cfg.clock,
		defaultRetryPolicy: cfg.defaultRetryPolicy,
	}

	if cfg.workflowResolver != nil {
		e.workflows = cfg.workflowResolver
	} else {
		reg := newRegistry("workflow")
		e.workflows = reg
		e.workflowRegistry = reg
	}

	if cfg.activityResolver != nil {
		e.activities = cfg.activityResolver
	} else {
		reg := newRegistry("activity")
		e.activities = reg
		e.activityRegistry = reg
	}

	return e
}

// registerWorkflow implements Registrar.
func (e *Engine) registerWorkflow(name string, fn any) error {
	if e.workflowRegistry == nil {
		return errors.New("derecho: cannot register workflow with custom resolver")
	}
	if err := validateSignature(fn, derechoContextType, "derecho.Context"); err != nil {
		return fmt.Errorf("derecho: workflow %q: %w", name, err)
	}
	return e.workflowRegistry.register(name, fn)
}

// registerActivity implements Registrar.
func (e *Engine) registerActivity(name string, fn any) error {
	if e.activityRegistry == nil {
		return errors.New("derecho: cannot register activity with custom resolver")
	}
	if err := validateSignature(fn, stdContextType, "context.Context"); err != nil {
		return fmt.Errorf("derecho: activity %q: %w", name, err)
	}
	return e.activityRegistry.register(name, fn)
}

var (
	derechoContextType = reflect.TypeOf((*Context)(nil)).Elem()
	stdContextType     = reflect.TypeOf((*context.Context)(nil)).Elem()
	errorType          = reflect.TypeOf((*error)(nil)).Elem()
)

func validateSignature(fn any, ctxType reflect.Type, ctxName string) error {
	t := reflect.TypeOf(fn)
	if t == nil || t.Kind() != reflect.Func {
		return errors.New("derecho: must be a function")
	}
	if t.NumIn() != 2 {
		return fmt.Errorf("derecho: must have 2 parameters, got %d", t.NumIn())
	}
	if !t.In(0).Implements(ctxType) {
		return fmt.Errorf("derecho: first parameter must be %s, got %s", ctxName, t.In(0))
	}
	if t.NumOut() != 2 {
		return fmt.Errorf("derecho: must have 2 return values, got %d", t.NumOut())
	}
	if !t.Out(1).Implements(errorType) {
		return fmt.Errorf("derecho: second return value must be error, got %s", t.Out(1))
	}
	return nil
}

func (e *Engine) WorkerID() string {
	return e.workerID
}

func (e *Engine) Client() Client {
	return &client{store: e.store, codec: e.codec, clock: e.clock}
}

func (e *Engine) WorkflowWorker() *workflowWorker {
	return &workflowWorker{
		store:              e.store,
		cache:              e.cache,
		resolver:           e.workflows,
		workerID:           e.workerID,
		codec:              e.codec,
		defaultRetryPolicy: e.defaultRetryPolicy,
	}
}

func (e *Engine) ActivityWorker() *activityWorker {
	return &activityWorker{
		store:    e.store,
		resolver: e.activities,
		workerID: e.workerID,
		codec:    e.codec,
		clock:    e.clock,
	}
}

func (e *Engine) TimerWorker() *timerWorker {
	return &timerWorker{store: e.store, clock: e.clock}
}

func (e *Engine) TimeoutWorker() *timeoutWorker {
	return &timeoutWorker{store: e.store, clock: e.clock}
}

type Worker interface {
	Process(ctx context.Context) error
}

func RunWorker(ctx context.Context, w Worker) error {
	for {
		if err := w.Process(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
	}
}

// Run starts all workers and blocks until ctx is cancelled or a worker fails.
func (e *Engine) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errc := make(chan error, 4)

	go func() { errc <- RunWorker(ctx, e.WorkflowWorker()) }()
	go func() { errc <- RunWorker(ctx, e.ActivityWorker()) }()
	go func() { errc <- RunWorker(ctx, e.TimerWorker()) }()
	go func() { errc <- RunWorker(ctx, e.TimeoutWorker()) }()

	err := <-errc
	cancel()

	// Drain remaining errors
	<-errc
	<-errc
	<-errc

	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

// Actor returns execute and interrupt functions for use with run.Group.
func (e *Engine) Actor() (execute func() error, interrupt func(error)) {
	ctx, cancel := context.WithCancel(context.Background())
	return func() error {
			return e.Run(ctx)
		}, func(error) {
			cancel()
		}
}
