# work

Typed, durable asynchronous execution for Go.

```go
rt.Dispatch(ctx, SendInvoice{InvoiceID: "inv_123"}, work.Queue("billing"))
```

`work` stores a unit of application behavior durably, validates it at both
ends, lets a worker claim it under a lease, and runs it **at least once**.
The default backend is your own database, reached through
[Lathe](https://github.com/tobibamidele/lathe); payload contracts are
[Validate](https://github.com/tobibamidele/validate) schemas.

It is deliberately small. It is not a workflow engine, an actor system, a
saga orchestrator or a message broker.

## The mental model

Read this before the API; everything else follows from it.

1. **Dispatch stores, it does not run.** `Dispatch` validates the payload,
   normalizes it, writes a row and returns once the row is durable. Nothing
   executes in your request.
2. **Work is a row with a lease.** A worker *claims* a row: it becomes
   `running`, owned by that worker until a lease expires. While the handler
   runs, the worker heartbeats to extend the lease. If the worker dies, the
   lease lapses and another worker takes the row.
3. **Execution is at least once.** A crash between the handler's side effect
   and the acknowledgement means the work runs again. This is not a bug to be
   fixed later; it is the contract. Make handlers idempotent (see
   [idempotency](docs/guide.md#8-idempotency)).
4. **Identity is name + version, not a Go type.** `"send-invoice"` v1 is what
   is stored. Renaming a Go struct or moving a package never orphans work.
5. **Payloads are contracts.** The same schema validates at dispatch (invalid
   work is never stored) and again before the handler runs (corrupt or
   incompatible stored work fails clearly instead of reaching your code).
6. **Because the table is yours, enqueue can be transactional.** Work created
   in the same transaction as your own writes exists if and only if the
   transaction commits. That single property removes a whole class of "the
   order was saved but the email job was lost" bugs.

### Lifecycle

```
                  Retry (backoff, attempts left)
              ┌──────────────────────────────────┐
              ▼                                  │
  Dispatch ► available ──Claim──► running ──┬────┘
              │  ▲                  │       ├── handler returns nil ──► completed
              │  └── lease expired ─┘       ├── NonRetryable / attempts exhausted ──► failed
   Cancel     │      (reclaimed)            └── Cancel requested ──► cancelled
              ▼
          cancelled           failed / cancelled ──manual retry──► available
```

There is no `retrying` status: a retry is an `available` record with a future
`available_at`.

## Install

```
go get github.com/tobibamidele/work
```

Go 1.22 or newer. The core package depends on Validate (and Lathe's types only
in `work/database`). Database drivers are your application's choice.

## Getting started

```go
// 1. A payload and its contract.
type SendInvoice struct {
    InvoiceID string `json:"invoice_id"` // tags must match the schema's field names
    Email     string `json:"email"`
}

var SendInvoiceSchema = validate.Object[SendInvoice](
    validate.Field("invoice_id", validate.String().Trim().Min(1).Max(64)),
    validate.Field("email", validate.String().Trim().Lower().Email()),
)

// 2. The handler: an ordinary method.
func (j SendInvoice) Handle(ctx context.Context) error {
    meta, _ := work.FromContext(ctx)       // optional: ID, attempt, idempotency key...
    return mailer.Send(ctx, j.Email, j.InvoiceID, meta.IdempotencyKey)
}

// 3. Register at startup. Name and version are the stored identity.
registry := work.NewRegistry()
registry.MustRegister(
    work.Declare[SendInvoice]("send-invoice", work.Schema(SendInvoiceSchema), work.DefaultQueue("billing")),
)

// 4. Database: create the table once, then build the backend.
database.Migrate(ctx, db)                      // db is a *lathe.DB
backend, _ := database.New(ctx, db)

// 5. Dispatch from anywhere.
rt, _ := work.NewRuntime(backend, registry, work.RuntimeConfig{})
rt.Dispatch(ctx, SendInvoice{InvoiceID: "inv_123", Email: "a@b.co"})

// 6. Run workers (usually a separate process).
worker, _ := work.NewWorker(backend, registry, work.WorkerConfig{
    Queues: []work.QueueConfig{{Name: "billing", Concurrency: 10}},
})
ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer stop()
worker.Run(ctx) // returns after a graceful shutdown
```

Runnable versions: [`examples/basic`](examples/basic) and
[`examples/transactional`](examples/transactional).

## What you get

| | |
|---|---|
| **Typed dispatch** | `Dispatch(ctx, SendInvoice{...})`: no string names or `map[string]any` at call sites |
| **Validation both ways** | invalid payloads are rejected before storage; stored payloads are re-validated before execution; the *normalized* value is what is persisted |
| **Queues, priorities, delays** | per-queue concurrency, global bound, `Priority`, `Delay`, `DispatchAt` |
| **Retries** | exponential backoff with jitter, `NonRetryable`, `RetryAfter`, per-work `MaxAttempts` and `Timeout` |
| **Leases** | atomic claiming, heartbeats, crash recovery, ownership checks on every acknowledgement |
| **Uniqueness** | `Unique(key)` / `UniquePayload()`: at most one *active* item per key, enforced by the database |
| **Cancellation** | immediate for available work; cooperative (context cancellation) for running work |
| **Transactions** | `database.WithTx` / `database.DispatchTx` |
| **Observability** | lifecycle events, `slog` logging, metrics sink, tracing hooks, trace propagation; payloads never logged by default |
| **Versioning** | name + version identity; unsupported versions fail clearly, never mis-decode |
| **Operations** | `work/cli` commands: inspect, list, failed, retry, cancel, stats |

## Backends

| Backend | |
|---|---|
| `work/database` | PostgreSQL, MySQL 8.0.1+ (MariaDB 10.6+), SQLite 3.35+ through Lathe. Tested against PostgreSQL 16, MySQL 8.0 and SQLite. |
| `work/redis` | Redis 5+ (single node, Sentinel or Cluster) through go-redis, in its own module. Atomic Lua scripts; no SQL transactions. Tested against Redis 7.0. |
| `work/memory` | reference implementation for tests; not durable |

A new backend implements `work.Backend` and must pass `backendtest.Run`
unchanged; the database, Redis and memory backends all do. Pick the database
backend when you want transactional dispatch and durability by default; pick
Redis when work should not load your SQL database. RabbitMQ is intentionally
not part of v1; see [docs/design.md](docs/design.md).

## Documentation

- [docs/guide.md](docs/guide.md): the guide (18 topics: defining work, dispatching, queues, retries, uniqueness, idempotency, transactions, workers, shutdown, middleware, backends, database setup, testing, production, failure semantics, at-least-once)
- [docs/design.md](docs/design.md): design decisions and every place this implementation departs from the original spec, with the reason

## Development

```
make test          # unit tests, race detector (no databases needed)
make integration   # real PostgreSQL / MySQL / SQLite / Redis (see Makefile for DSNs)
```

MIT licensed.
