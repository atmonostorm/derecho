# Derecho

Durable workflow execution through event sourcing. About 10k lines of Go.

## Notice

This is an export from our internal monorepo. We use it in production and wanted to share it with the community.

**No support.** We will not respond to issues, questions, or feature requests.

**No contributions.** We will not accept pull requests. Fork it if you want to take it somewhere.

**No stability guarantees.** The API may change without warning. We version for ourselves.

Use it, learn from it, build on it. Just know you're on your own.

## Introduction

Your function crashes. The machine dies. The process restarts.

Derecho replays your workflow from its event log and picks up exactly where it left off. Same variables, same state, same execution path.

```go
func OrderWorkflow(ctx derecho.Context, order Order) (Receipt, error) {
    // Crash here? On restart, skip this. Result already in log.
    reserved := inventory.Execute(ctx, order.Items).Get(ctx)

    // Crash here? Payment result replays from log
    payment := charge.Execute(ctx, order.Payment).Get(ctx)

    // Weeks of downtime? Execution resumes at this exact point
    return ship.Execute(ctx, reserved, payment).Get(ctx)
}
```

## Why Derecho

**Durable execution, one database.** Temporal wants multiple services, Cassandra clusters, UI servers, someone to keep it all running. Derecho embeds into your app. One binary. One database. Done.

**Determinism is structural.** Derecho runs one fiber at a time through cooperative yields. Can't call `time.Now()` and break replay. Scheduler controls execution.

**Small enough to understand.** 10k lines. Something always breaks. When it does, you read the code and figure out why. Takes an afternoon.

### When Derecho Fits

- Behind-the-firewall apps where you own the infrastructure
- Single-region deploys (or active-passive failover)
- Teams that want to own their workflow engine
- Workflow counts in the millions

### When It Doesn't

- Multi-datacenter active-active (we don't coordinate across regions)
- You need a workflow UI yesterday (we don't have one)
- You need workflow versioning (planned, still cooking)
- You'd rather pay Temporal (reasonable, they're good)

## How It Works

**Event sourcing.** Every scheduling decision becomes an immutable event. Replay the log, get identical execution.

**Cooperative fibers.** Write straight-line code. `Await`, `Sleep`, `Future.Get` are yield points. Scheduler switches fibers there.

**Determinism by construction.** One scheduler loop. One fiber running at a time. Same events in, same events out. Always.

## Storage

Derecho talks to a `journal.Store` interface. Pick your poison:

```go
// Development and single-process: everything in memory
store := derecho.NewMemoryStore()

// Single node: SQLite (planned)
// store := derechosqlite.New(db)

// Multiple nodes: PostgreSQL (planned)
// store := derechopgx.New(pool)
```

### MemoryStore

Ships with Derecho. Good for development, testing, and single-process deployments where you don't need persistence across restarts.

### SQLite (Planned)

One server. WAL mode. Thousands of concurrent workflows on a laptop.

```go
db, _ := sql.Open("sqlite", "workflows.db?_journal=WAL&_synchronous=NORMAL")
store := derechosqlite.New(db)
```

Good for edge deployments, single-server apps, anywhere "just run Postgres" is annoying.

### PostgreSQL (Planned)

Multiple engine instances. Coordination through `SELECT ... FOR UPDATE SKIP LOCKED`. Worker affinity so workflows stick to nodes that have them cached.

```go
pool, _ := pgxpool.New(ctx, "postgres://localhost/workflows")
store := derechopgx.New(pool,
    derechopgx.WithWorkerAffinity(true),
)
```

Good for anything that needs to survive a server dying.

## Codecs

Derecho serializes workflow inputs, activity results, and signals. Interface is obvious:

```go
type Codec interface {
    Encode(v any) ([]byte, error)
    Decode(data []byte, v any) error
}
```

Default is JSON. JSON is fine. Strong feelings about protobuf? Wire it up:

```go
engine := derecho.NewEngine(store,
    derecho.WithEngineCodec(protoCodec{}),
)
```

### Protobuf

```go
type protoCodec struct{}

func (protoCodec) Encode(v any) ([]byte, error) {
    msg, ok := v.(proto.Message)
    if !ok {
        return nil, fmt.Errorf("not a proto.Message: %T", v)
    }
    return proto.Marshal(msg)
}

func (protoCodec) Decode(data []byte, v any) error {
    msg, ok := v.(proto.Message)
    if !ok {
        return fmt.Errorf("not a proto.Message: %T", v)
    }
    return proto.Unmarshal(data, msg)
}
```

### Versioning

Codecs don't version. Your types do. Two ways:

**Envelope with version tag:**

```go
type Envelope struct {
    Version int             `json:"v"`
    Payload json.RawMessage `json:"p"`
}

// Decode, switch on version, unmarshal the right type
```

**Additive fields:**

```go
type Order struct {
    Items    []string `json:"items"`
    Priority int      `json:"priority,omitempty"` // New field, zero = default
}
```

Old events decode with `Priority: 0`. New code treats zero as "normal priority." Works.

Changing codecs mid-deployment means migrating stored events. Pick a codec early. Save yourself pain.

## Activities

Activities are where side effects live. HTTP calls. Database writes. Emails. Anything touching the outside world.

```go
// Typed reference. Use this in workflows.
var sendEmail = derecho.NewActivityRef[EmailReq, EmailResp]("send-email")

// Implementation. Runs on activity workers.
derecho.RegisterActivity(engine, "send-email", func(ctx context.Context, req EmailReq) (EmailResp, error) {
    return emailClient.Send(req)
})

// In your workflow
resp, err := sendEmail.Execute(ctx, req).Get(ctx)
```

Activities run outside the deterministic bubble. Results get recorded. On replay, recorded result comes back. Activity doesn't run twice.

### Retries and Timeouts

```go
resp, err := sendEmail.Execute(ctx, req,
    derecho.WithRetry(derecho.RetryPolicy{
        InitialInterval:    time.Second,
        BackoffCoefficient: 2.0,
        MaxInterval:        time.Minute,
        MaxAttempts:        5,
    }),
    derecho.WithScheduleToCloseTimeout(5*time.Minute),  // Total budget
    derecho.WithStartToCloseTimeout(30*time.Second),    // Per attempt
    derecho.WithHeartbeatTimeout(10*time.Second),       // Liveness check
).Get(ctx)
```

**Timeouts:**
- `ScheduleToClose`: Total time from scheduling to completion, across all retries
- `StartToClose`: Time for one attempt
- `ScheduleToStart`: Time waiting for a worker to pick it up
- `Heartbeat`: Silence before we assume the worker died

### Heartbeats

Long-running activities should heartbeat. Otherwise can't tell if working or dead.

```go
func processLargeFile(ctx context.Context, req FileReq) (Result, error) {
    for i, chunk := range chunks {
        process(chunk)
        derecho.Heartbeat(ctx, Progress{Chunk: i, Total: len(chunks)})
    }
    return Result{}, nil
}
```

Heartbeat progress gets saved. Activity crashes, next attempt resumes from checkpoint.

### Controlling Retries

Activities can override retry behavior:

```go
func myActivity(ctx context.Context, req Request) (Response, error) {
    err := doWork()
    if isFatal(err) {
        return Response{}, derecho.NonRetryable(err)  // Stop retrying
    }
    if needsBackoff(err) {
        return Response{}, derecho.RetryAfter(30*time.Second, err)  // Custom delay
    }
    return Response{}, err  // Normal retry with policy backoff
}
```

## Parallelism

Spawn fibers with `Go`. Wait for conditions with `Await`. Fibers are cooperative. Safe to share memory because only one runs at a time.

```go
func ProcessBatch(ctx derecho.Context, items []Item) ([]Result, error) {
    results := make([]Result, len(items))

    for i, item := range items {
        i, item := i, item
        derecho.Go(ctx, func(ctx derecho.Context) {
            results[i], _ = process.Execute(ctx, item).Get(ctx)
        })
    }

    derecho.Await(ctx, func() bool {
        for _, r := range results {
            if r == (Result{}) { return false }
        }
        return true
    })

    return results, nil
}
```

### AwaitWithTimeout

Wait for a condition with a deadline:

```go
approved := derecho.AwaitWithTimeout(ctx, 24*time.Hour, func() bool {
    return approvalReceived
})
if !approved {
    return errors.New("approval timed out")
}
```

## Timers

Sleep survives restarts. Set timer for a week. Kill process. Restart three days later. Timer fires on schedule.

```go
derecho.Sleep(ctx, 7 * 24 * time.Hour)

// Cancellable version
timer := derecho.NewTimer(ctx, 30 * time.Minute)
// ... later ...
derecho.CancelTimer(ctx, timer)
```

## Workflow Context

Access workflow metadata and deterministic time:

```go
// Deterministic time (replays return original execution time)
now := derecho.Now(ctx)

// Workflow metadata
info := derecho.GetInfo(ctx)
// Fields: WorkflowID, RunID, WorkflowType, StartTime
```

`derecho.Now(ctx)` returns logical time, not wall clock. Safe for replay. Use `SideEffect` if you need actual wall time recorded.

## Signals

External events into workflows. User approves something. Webhook fires. Whatever.

```go
func ApprovalWorkflow(ctx derecho.Context, req ApprovalReq) (bool, error) {
    approvals := derecho.GetSignalChannel[Approval](ctx, "approval")
    deadline := derecho.NewTimer(ctx, 72 * time.Hour)

    sel := derecho.NewSelector()
    derecho.AddFuture(sel, approvals.ReceiveFuture(), func(a Approval, _ error) {
        // Handle approval
    })
    derecho.AddFuture(sel, deadline, func(_ time.Time, _ error) {
        // Handle timeout
    })
    sel.Select(ctx)
    // ...
}

// From outside
client.SignalWorkflow(ctx, "workflow-123", "approval", Approval{Approved: true})
```

## Child Workflows

Workflow section generates tons of events? Loops, fan-outs? Split it into child workflows. Each gets own event log. Parent stays small. Replays stay fast.

```go
var processChunk = derecho.NewChildWorkflowRef[Chunk, ChunkResult]("process-chunk")

func BigWorkflow(ctx derecho.Context, data LargeDataset) (Summary, error) {
    var futures []derecho.Future[ChunkResult]

    for i, chunk := range data.Split(1000) {
        f := processChunk.Execute(ctx, fmt.Sprintf("chunk-%d", i), chunk,
            derecho.WithClosePolicy(journal.ParentClosePolicyAbandon),
        )
        futures = append(futures, f)
    }
    // ...
}
```

## SideEffect

Non-deterministic operations too small for an activity. UUIDs. Wall-clock reads. Random numbers.

```go
id := derecho.SideEffect(ctx, func() string {
    return uuid.New().String()
})
// On replay, returns same UUID
```

## Continue-As-New

Long-running workflows accumulate events. Eventually replay gets slow. `ContinueAsNew` restarts with fresh history, carrying forward state.

```go
func InfinitePoller(ctx derecho.Context, cursor string) error {
    for i := 0; i < 1000; i++ {
        result, _ := poll.Execute(ctx, cursor).Get(ctx)
        cursor = result.NextCursor
        derecho.Sleep(ctx, time.Minute)
    }
    return derecho.NewContinueAsNewError(cursor)
}
```

## Running It

```go
store := derecho.NewMemoryStore()
engine := derecho.NewEngine(store,
    derecho.WithWorkerID("worker-1"),
    derecho.WithDefaultRetryPolicy(defaultPolicy),
)

derecho.RegisterWorkflow(engine, "order", OrderWorkflow)
derecho.RegisterActivity(engine, "charge", chargeActivity)

client := engine.Client()
wf, _ := client.StartWorkflow(ctx, "order", "order-123", orderData)

go engine.Run(ctx)

var receipt Receipt
wf.Get(ctx, &receipt)
```

### Graceful Shutdown

`engine.Actor()` returns `(execute, interrupt)` functions compatible with oklog/run or similar:

```go
ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
defer cancel()

if err := engine.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
    log.Fatal(err)
}
```

## Observability

Derecho logs through `slog`. Workflow ID, run ID, activity name in context. Something goes wrong at 3am, grep for the workflow.

For metrics, instrument the Store interface or wrap activity functions. No built-in metrics interface yet.

## Testing

### Unit Tests

`derechotest` stubs activities. Test workflow logic without I/O.

```go
func TestOrderWorkflow(t *testing.T) {
    env := derechotest.New(t)

    env.StubActivity("reserve-inventory", InventoryResp{Reserved: true})
    env.StubActivity("charge-payment", PaymentResp{TransactionID: "txn-123"})
    env.StubActivity("ship-order", ShipResp{TrackingNumber: "track-456"})

    result, err := derechotest.Run(env, OrderWorkflow, Order{Items: []Item{{SKU: "ABC"}}})

    env.AssertActivityCalled("reserve-inventory")
    env.AssertActivityCalled("charge-payment")
}
```

### Dynamic Stubs

```go
env.StubActivityFunc("charge-payment", func(input json.RawMessage) (any, error) {
    var req PaymentReq
    json.Unmarshal(input, &req)
    if req.Amount > 10000 {
        return nil, errors.New("amount exceeds limit")
    }
    return PaymentResp{TransactionID: "txn-123"}, nil
})
```

### Replay Testing

Catch non-determinism before production. Save event logs from real executions, replay against new code.

```go
func TestOrderWorkflow_Replay(t *testing.T) {
    events := loadEvents(t, "testdata/order-workflow-v1.json")
    err := derecho.Replay(OrderWorkflow, events)
    if err != nil {
        t.Fatalf("workflow replay failed: %v", err)
    }
}
```

Run in CI. Every deploy.

## The Rules

**Workflows must be deterministic.** Same inputs, same scheduling decisions.

Don't:
- `time.Now()` : use `derecho.Now(ctx)`
- `rand.Int()` : use `derecho.SideEffect`
- Global mutable state
- Goroutines : use `derecho.Go`
- Direct I/O : use activities

**Activities handle the real world.** HTTP calls, database writes, file I/O. All activities. They fail, retry, timeout. Results are durable.

**Replay catches mistakes.** Workflow makes different decisions on replay, you get `NondeterminismError`. Good. Finds bugs before they corrupt your event log.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                         Engine                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │   Workflow   │  │   Activity   │  │    Timer     │      │
│  │    Worker    │  │    Worker    │  │    Worker    │      │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘      │
│         │                 │                 │               │
│         └─────────────────┼─────────────────┘               │
│                           │                                 │
│                    ┌──────▼──────┐                          │
│                    │journal.Store│                          │
│                    └──────┬──────┘                          │
└───────────────────────────┼─────────────────────────────────┘
                            │
              ┌─────────────┼─────────────┐
              │             │             │
        ┌─────▼─────┐ ┌─────▼─────┐ ┌─────▼─────┐
        │  SQLite   │ │PostgreSQL │ │  Memory   │
        └───────────┘ └───────────┘ └───────────┘
```

Four workers:
- **Workflow Worker**: Loads history, replays scheduler, persists new events
- **Activity Worker**: Runs activities, handles retries and timeouts
- **Timer Worker**: Fires timers when due
- **Timeout Worker**: Fails activities that exceed their timeout

Workers coordinate through Store. Multiple engines against PostgreSQL for horizontal scaling.

---

*Derecho: Spanish for "straight" or "direct." Also a powerful straight-line windstorm.*
