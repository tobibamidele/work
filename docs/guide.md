# Guide

Everything here describes behavior that is covered by the test suite. Where a
rule is a guarantee, the name of the guarantee is in **bold**.

1. [Defining work](#1-defining-work)
2. [Validation and payload contracts](#2-validation-and-payload-contracts)
3. [Dispatching](#3-dispatching)
4. [Queues, priorities and delays](#4-queues-priorities-and-delays)
5. [Retries and backoff](#5-retries-and-backoff)
6. [Failure semantics](#6-failure-semantics)
7. [Uniqueness](#7-uniqueness)
8. [Idempotency](#8-idempotency)
9. [Leases and heartbeats](#9-leases-and-heartbeats)
10. [Cancellation](#10-cancellation)
11. [Transactional dispatch](#11-transactional-dispatch)
12. [Workers, concurrency and shutdown](#12-workers-concurrency-and-shutdown)
13. [Middleware and observability](#13-middleware-and-observability)
14. [Versioning payloads](#14-versioning-payloads)
15. [Database backend setup](#15-database-backend-setup)
16. [Testing your work](#16-testing-your-work)
17. [Production deployment and security](#17-production-deployment-and-security)
18. [Operating: inspection and the CLI](#18-operating-inspection-and-the-cli)

## 1. Defining work

Two forms, same result.

```go
// Ergonomic: the payload type has a Handle method.
func (j SendInvoice) Handle(ctx context.Context) error { ... }
work.Declare[SendInvoice]("send-invoice", work.Schema(SendInvoiceSchema))

// Explicit: schema and function side by side.
work.Define("send-welcome-email", SendWelcomeEmailSchema,
    func(ctx context.Context, p SendWelcomeEmail) error { ... })
```

`Handle` may have a value or a pointer receiver. The handler signature is the
idiomatic `func(context.Context) error`; execution metadata is optional via
`work.FromContext(ctx)`.

The **name** (`"send-invoice"`) is the stored identity. Choose a short stable
identifier. Go import paths and type names are rejected, because renaming a
struct must never orphan stored work.

Definition options: `Version`, `DefaultQueue`, `DefaultMaxAttempts`,
`DefaultPriority`, `DefaultTimeout`, `DefaultBackoff`, `DefinitionSerializer`.

Registration is application-owned (`work.NewRegistry()`), done at startup, and
rejected with `ErrConfig` if invalid: bad name, nil handler, duplicate
name+version, a Go type registered twice, a schema for a different type, or a
`Declare` type without a `Handle` method. Starting a worker freezes the
registry (`ErrRegistryFrozen`).

## 2. Validation and payload contracts

A payload has a [Validate](https://github.com/tobibamidele/validate) schema.

**Both ends validate.** At dispatch the payload is serialized, validated and
normalized, and the *normalized* value is what is stored (a trimmed,
lower-cased email is stored trimmed and lower-cased). Before the handler runs,
the stored bytes are validated again. An invalid payload is rejected before
anything is persisted; stored work that cannot be validated fails permanently
(retrying malformed data can never succeed) with a `PayloadError`.

Errors keep their detail: `errors.Is(err, work.ErrInvalidPayload)` and
`validate.IsValidationError(err)` both hold, and `errors.As(err, &*work.PayloadError)`
tells you the phase and type.

**JSON tags must match the schema's field names.** The default serializer is
`encoding/json`, so a struct field is stored under its json tag and the schema
reads that name. Two mistakes are caught loudly at dispatch rather than
silently losing data:

- a payload field the schema does not declare ("`payload field "b" is not
  declared in the schema and would be dropped`"), and
- a tag that disagrees with the schema (the normalized payload would not
  satisfy its own schema).

Integers above 2^53 survive the round trip exactly. Schema fingerprints are
stored for diagnostics only; **version, not fingerprint, is the compatibility
boundary**, and a differing fingerprint only logs a warning.

Payloads without a schema (`Declare` without `Schema`) are checked
structurally (they must serialize and deserialize) and by an optional
`Validate() error` method.

## 3. Dispatching

```go
ref, err := rt.Dispatch(ctx, SendInvoice{...}, work.Queue("billing"), work.MaxAttempts(10))
ref, err  = rt.DispatchAt(ctx, payload, time.Now().Add(time.Hour))
ref, err  = work.Dispatch(ctx, payload) // runtime from the context or SetDefault
```

`Dispatch` returns after the backend confirmed the record is durable, with a
`Ref` (ID, type, version, queue, available-at). It never executes work.

Options: `Queue`, `Delay`, `At`, `MaxAttempts`, `Priority`, `Unique`,
`UniquePayload`, `IdempotencyKey`, `Timeout`, `DispatchMetadata`. Invalid
options (negative delay, `MaxAttempts(0)`, invalid queue name, `Delay` together
with `At`) fail the dispatch.

Package-level `work.Dispatch` finds its Runtime in the context
(`work.NewContext`) and falls back to one optional process default
(`work.SetDefault`). There is no global registry; the default is a convenience
for application bootstrap, not shared state.

A backend that cannot honor an option refuses it with `ErrUnsupported` instead
of ignoring it.

## 4. Queues, priorities and delays

A worker only claims from the queues it is configured for. Each queue has its
own concurrency; a global `Concurrency` bounds the total.

```go
work.WorkerConfig{
    Concurrency: 20,                       // upper bound across queues
    Queues: []work.QueueConfig{
        {Name: "billing", Concurrency: 10},
        {Name: "emails",  Concurrency: 5},
    },
}
```

Precedence: queue concurrency if set, else the global value, else 10; the
global value, when set, always caps the total number of running handlers.

Claim order inside a queue is highest `Priority` first, then earliest
`available_at`, then creation time. Priority is a preference, not a global
ordering guarantee across workers. Delayed work (`Delay`, `DispatchAt`) is not
claimable before its time.

A worker claims only the work **types it has registered**, so a worker without
a handler never burns an attempt on work it cannot run; such work waits for a
worker that has one.

## 5. Retries and backoff

An attempt fails when the handler returns an error or panics. If attempts
remain, the work returns to `available` at `now + backoff`; otherwise it
becomes `failed`.

```go
work.ExponentialBackoff{Initial: time.Second, Maximum: 10 * time.Minute} // jitter on by default
```

The zero value gives roughly 1s, 5s, 25s, ~2m, 10m with ±20% jitter. A
`BackoffFunc`/`FixedBackoff` is also available. Precedence: the definition's
`DefaultBackoff`, then the worker's `Backoff`.

Handler errors steer the engine:

| return | effect |
|---|---|
| `nil` | completed |
| any error | retry with backoff, or fail when attempts are exhausted |
| `work.NonRetryable(err)` | fail immediately, regardless of attempts left |
| `work.RetryAfter(30*time.Second)` | retry after that delay (still bounded by `MaxAttempts`) |
| panic | recovered; recorded as a retryable failure with the stack |

`Timeout(d)` (or `DefaultTimeout`) gives each attempt a context deadline; the
handler must honor its context for it to take effect. Per-dispatch backoff
policies are not offered: a policy is code, not data, so it is configured per
definition and per worker.

Infrastructure errors (the database is unreachable) are a different thing.
They never consume attempts. Claiming backs off 100ms, 500ms, 1s, 2s, 5s
(then 5s) with jitter; settling a finished attempt retries the same way for up
to one lease duration.

## 6. Failure semantics

What happens in every failure, precisely:

| situation | result |
|---|---|
| handler returns error, attempts left | `available` again after backoff; error text stored in `last_error` |
| attempts exhausted | `failed`; stays inspectable; `Retry` can requeue it |
| handler panics | same as an error; `last_error` includes the stack |
| stored payload invalid / version unsupported | `failed` after one attempt; handler never called |
| worker crashes mid-handler | lease expires; another worker reclaims it (attempt counted) |
| worker crashes and no attempts remain | the reaper settles it as `failed` ("lease expired and no attempts remain") |
| worker can't reach the database to acknowledge | retries for up to a lease; then the work is recovered and runs again |
| lease lost while running | handler context is cancelled with cause `ErrLeaseLost`; the old worker never acknowledges |
| shutdown grace period exceeded | handlers get a cancelled context; interrupted work is **released** without consuming an attempt |

Stored error text is bounded (8 KiB), includes no payload, and keeps panic
stacks.

## 7. Uniqueness

```go
rt.Dispatch(ctx, ReindexAccount{ID: "a1"}, work.Unique("reindex:a1"))
rt.Dispatch(ctx, ReindexAccount{ID: "a1"}, work.UniquePayload()) // key from type+version+payload
```

**At most one active work item per (queue, key).** *Active* means available or
running. Completed, failed and cancelled work never blocks a new dispatch, so
history cannot lock a key. A blocked dispatch returns `ErrDuplicate` together
with the `Ref` of the work holding the key:

```go
ref, err := rt.Dispatch(...)
if errors.Is(err, work.ErrDuplicate) { /* ref.ID is the active one */ }
```

Enforcement is a unique index in the database, so it holds across processes.
Keys are compared with the database's collation: on MySQL's default
collation they are case-insensitive. Uniqueness is not idempotency: it
suppresses duplicate *scheduling*; it does not make a handler safe to run twice.

## 8. Idempotency

**Work runs at least once.** A worker can finish the side effect and crash
before acknowledging; the work then runs again. Design for it:

- Pass the idempotency key to anything external (payment APIs, email
  providers): `work.FromContext(ctx).IdempotencyKey`. It is the work ID unless
  you set `work.IdempotencyKey("order-77-receipt")` at dispatch.
- Make database effects conditional (`INSERT ... ON CONFLICT DO NOTHING`,
  state checks) or do them in the transaction that records completion.
- `Metadata.Attempt` tells you whether this is a retry.

Do not assume exactly-once. No queue gives it without cooperation from the
effect.

## 9. Leases and heartbeats

A claim sets `worker_id`, `locked_at` and `lease_until = now + LeaseDuration`
(default 2m) and increments `attempts`. While the handler runs the worker
extends the lease every `HeartbeatInterval` (default lease/3). Every
acknowledgement (complete, retry, fail, release) succeeds only while the row is
`running` and owned by that worker, otherwise it returns `ErrLeaseLost`
(`ErrCancelled`, `ErrAlreadyCompleted` or `ErrNotFound` when those describe it).

- Temporary heartbeat errors do not cancel the handler: ownership was not
  disproved. Only proven loss (`ErrLeaseLost`) cancels it.
- Keep `LeaseDuration` well above worst-case database latency and GC pauses.
- Timestamps come from the **application clock** (`Clock`), not the database's,
  so run NTP on every node: clock skew shortens or lengthens effective leases.

## 10. Cancellation

```go
outcome, err := rt.Cancel(ctx, ref.ID)
```

- **available** work is cancelled immediately (`work.Cancelled`).
- **running** work cannot be killed. A request is recorded
  (`work.CancelRequested`); the owning worker sees it at its next heartbeat
  and cancels the handler's context. When the handler returns the final state is
  settled: `completed` if it finished anyway, `cancelled` otherwise, and it is
  never retried. If the worker died, the reaper settles it as `cancelled`.
- completed work returns `ErrAlreadyCompleted`; failed work `ErrInvalidState`;
  cancelling cancelled work is a no-op.

Cancellation latency for running work is up to one heartbeat interval. Your
handler must watch `ctx.Done()`.

`rt.Retry(ctx, id)` is the reverse for failed or cancelled work: attempts reset
to zero, available now, previous error text kept.

## 11. Transactional dispatch

The point of keeping work in your database.

```go
err := database.WithTx(ctx, db, rt, func(tx *database.Tx) error {
    if _, err := tx.DB().Exec(ctx, "INSERT INTO orders ...", ...); err != nil {
        return err                       // rolls back: no order, no work
    }
    return tx.Enqueue(ProcessOrder{OrderID: id})
})
```

or, with your own Lathe transaction:

```go
db.Tx(ctx, func(tx *lathe.DB) error {
    ...
    _, err := database.DispatchTx(ctx, rt, tx, ProcessOrder{OrderID: id})
    return err
})
```

Guarantees, each tested on PostgreSQL, MySQL and SQLite:

- the work exists if and only if the transaction commits;
- workers cannot see or claim it before the commit;
- a plain (non-transaction) handle is refused with `ErrNotInTransaction`
  instead of silently inserting outside the transaction;
- a `Unique` conflict inside the transaction does **not** poison it (on
  PostgreSQL a failed statement would abort the whole transaction; the backend
  claims keys without provoking errors), and the returned `Ref` identifies the
  existing work;
- `WithTx` emits `work.enqueued` after the commit; `DispatchTx` emits it when
  the insert succeeds (flagged `Transactional`), which can precede the commit.

Use the same database for the application tables and the work table. Backends
that cannot do this (`memory`) return `ErrUnsupported`.

## 12. Workers, concurrency and shutdown

```go
worker, _ := work.NewWorker(backend, registry, work.WorkerConfig{...}, work.Use(mw...))
err := worker.Run(ctx)   // blocks until ctx is cancelled and shutdown completes
```

Configuration errors are returned from `NewWorker` (invalid or duplicate queue,
heartbeat >= lease, negative values) or `Run` (empty registry, running twice);
they never surface mid-flight. Backend errors while running are retried, never
fatal.

**Graceful shutdown.** Cancel the context passed to `Run` (wire it to
SIGINT/SIGTERM with `signal.NotifyContext`):

1. claiming stops;
2. running handlers get `ShutdownTimeout` (default 30s) to finish; their results
   are still recorded;
3. after that their contexts are cancelled and they get a short window to
   return; interrupted work is released without losing an attempt;
4. a handler that ignores its context keeps its lease (it is recovered later)
   and `Run` returns `ErrShutdownTimeout`.

Set your orchestrator's termination grace period above `ShutdownTimeout`.

A worker also runs a **reaper** every `ReapInterval` (default 30s) that settles
expired work that can no longer be claimed (no attempts left, or cancellation
pending), and, with `WorkerConfig.Metrics`, reports queue depth gauges.

## 13. Middleware and observability

```go
work.NewWorker(backend, registry, cfg,
    work.Use(work.LoggingMiddleware(logger), work.MetricsMiddleware(sink), work.TracingMiddleware(tracer)))
```

`Use(A, B, C)` runs `A, B, C, handler, C, B, A`: the first is outermost.
Middleware wraps execution only; it cannot claim or acknowledge.

**Events.** `Observer`s receive `work.enqueued/started/retrying/completed/failed/
cancelled/lease_lost` (`RuntimeConfig.Observer`, `WorkerConfig.Observer`).
`SlogObserver` logs them; `MetricsObserver` turns them into counters and the
queue-wait distribution.

**Metrics** (`MetricsSink`, adapt to Prometheus/OTel): `work_enqueued_total`,
`work_started_total`, `work_completed_total`, `work_failed_total`,
`work_retried_total`, `work_cancelled_total`, `work_lease_lost_total`,
`work_duration_seconds`, `work_queue_wait_seconds`, gauges `work_queue_depth`,
`work_available`, `work_running`. Labels are only `work_type`, `queue` and
`status`: never IDs, payloads or emails.

**Logging** carries `work_id`, `work_type`, `queue`, `attempt`, `worker_id`,
`duration`, `status`, and **never payloads**. `WorkerConfig.DebugPayloads`
opts in to DEBUG-level payload logging; leave it off in production.

**Tracing.** `RuntimeConfig.Propagator` injects trace context into the
record's metadata at dispatch and `WorkerConfig.Propagator` restores it in the
handler's context, so a trace continues from the request into the worker.
`TracingMiddleware` opens a `work.execute` span with `work.id`, `work.type`,
`work.version`, `work.queue`, `work.attempt`, `work.worker_id`. The core takes
no OpenTelemetry dependency; adapt `Tracer` and `Propagator` in a few lines.

## 14. Versioning payloads

Identity is `(name, version)`. To change a payload incompatibly:

1. Define the new payload as a new Go type and register it under the same name
   with `work.Version(2)`. Both versions are registered side by side.
2. Deploy workers that know v1 **and** v2.
3. Switch producers to the new type.
4. When no v1 work remains (`work list --type send-invoice`), remove v1.

A worker handed a version it has not registered fails the work permanently with
a message listing the supported versions and never decodes it as another
version. After deploying a worker that supports it, `rt.Retry` requeues it.
Compatible changes (adding an optional field the old schema ignores) do not
need a new version; the schema fingerprint stored with each record is there to
help you notice drift.

## 15. Database backend setup

```go
db, _ := postgres.Open(dsn)          // or mysql.Open / sqlite.Open (Lathe)
database.Migrate(ctx, db)            // idempotent; safe at every startup
backend, _ := database.New(ctx, db)  // verifies the database can claim atomically
```

`database.New` refuses to start with a clear error when the database cannot
claim work atomically: PostgreSQL < 9.5, MySQL < 8.0.1, MariaDB < 10.6,
SQLite < 3.35, or any other dialect.

**Migrations.** `Migrate` applies SQL generated by Lathe from the schema in
`database.Tables()` and records it in its own `work_migrations` table, so it can
never appear as "missing" in your application's `lathe_migrations`. If you
manage the whole schema yourself, add `database.Tables()` to your Lathe schema
instead and let `lathe migrate diff` handle the work table with the rest. Use
one approach, not both.

**The table** (`work`) holds the canonical record: id (UUIDv7), type, version,
queue, payload (JSON), status, priority, attempts, max_attempts, available_at,
started/completed/locked/lease-until timestamps, worker_id, cancel_requested_at,
last_error, last_error_at, idempotency_key, unique_key, active_unique_key,
schema_fingerprint, timeout_ms, metadata, created_at, updated_at.

**Indexes:** `(queue, status, available_at, priority, created_at)` for claiming;
`(status, lease_until)` for lease recovery and the reaper; `(type, created_at)`
and `(created_at)` for inspection; unique `(queue, active_unique_key)`.

**Claim algorithm** (the only raw SQL): PostgreSQL uses one statement, a CTE
selecting `FOR UPDATE SKIP LOCKED` joined to an `UPDATE ... RETURNING`; MySQL
does `SELECT ... FOR UPDATE SKIP LOCKED`, an update and a read-back in a
transaction; SQLite uses a single `UPDATE ... RETURNING`, atomic under its
single writer. Concurrent claimers never block on, nor receive, the same row.

Notes: payloads are stored in a JSON column, so the database backend requires
the JSON serializer (anything else is refused with `ErrUnsupported`). Times are
stored as UTC; keep every process on the same driver location settings. SQLite
serializes writers: it is excellent for development, tests and small
deployments, and the wrong choice for heavy concurrent write load.

### The Redis backend

```go
import (
    goredis "github.com/redis/go-redis/v9"
    workredis "github.com/tobibamidele/work/redis"
)

client := goredis.NewClient(&goredis.Options{Addr: "localhost:6379"})
backend, _ := workredis.New(ctx, client, workredis.WithPrefix("myapp")) // everything else is unchanged
```

It is a separate module (`go get github.com/tobibamidele/work/redis`), so the
core never depends on a Redis client. It passes the same conformance suite as
the database backend: atomic claiming, leases and ownership, retries,
cooperative cancellation, unique keys, priorities, delays and crash recovery.
Every state change is a Lua script, which Redis runs atomically.

Choose it when work should not load your SQL database or you already run
Redis. Know what you give up:

- **No transactional dispatch.** Redis cannot join your SQL transaction
  (`Capabilities().Transactions` is false and `database.WithTx` refuses it).
  If "the order and its work commit together" matters, use the database backend.
- **Durability is your Redis configuration.** Enable AOF
  (`appendonly yes`, `appendfsync everysec` at minimum) or use a durable
  managed service. With asynchronous replication a failover can lose
  acknowledged enqueues. Leases and claims make execution at least once either
  way; only the *enqueue* is at risk.
- **Everything lives in memory, forever, unless you prune.** Run
  `backend.Prune(ctx, time.Now().Add(-7*24*time.Hour))` periodically (it deletes
  completed, failed and cancelled work created before the cutoff, in batches of
  5000; call again while it returns 5000). Active work is never touched.
- **One hash slot.** All keys carry a `{prefix}` hash tag, so a Backend works
  on a single node, Sentinel or Cluster, but one prefix lives on one cluster
  shard. Use different prefixes to spread independent systems.
- **Clock semantics.** Delayed work becomes claimable when a claimer's clock
  passes its time and then stays claimable; keep clocks synchronized (as
  always).
- Payloads are opaque bytes (not only JSON), unique keys compare byte-exactly,
  and `List` walks the creation index and filters: right for inspection and the
  CLI, not for hot paths.

Data model, for the curious: a hash per record, ready work in one sorted set
per (queue, type) whose members sort as priority, availability, creation, id;
delayed work and leases in score-ordered sets; a string per active unique key;
counters per queue and status. See `redis/scripts.go`.

## 16. Testing your work

Use the in-memory backend and the real worker; keep everything else real.

```go
backend := memory.New()
reg := work.NewRegistry(); reg.MustRegister(work.Declare[SendInvoice]("send-invoice", work.Schema(S)))
rt, _ := work.NewRuntime(backend, reg, work.RuntimeConfig{})
w, _ := work.NewWorker(backend, reg, work.WorkerConfig{PollInterval: 5 * time.Millisecond})
go w.Run(ctx)
rt.Dispatch(ctx, SendInvoice{...})
// poll rt.Get(ctx, ref.ID) until completed
```

Inject `RuntimeConfig.Clock` / `WorkerConfig.Clock` for deterministic time:
leases, availability and backoff are all computed from the injected clock.
Test your handler directly too: it is an ordinary method.

For a new backend, run the conformance suite:

```go
func TestConformance(t *testing.T) {
    backendtest.Run(t, func(t *testing.T) work.Backend { return newEmptyBackend(t) })
}
```

## 17. Production deployment and security

- Run workers as their own deployment so dispatchers and workers scale
  independently. They only need to share the database.
- Run `database.Migrate` from one place at deploy time, or at every startup
  (it is serialized).
- Size `Concurrency` against your database pool: each running handler holds a
  heartbeat goroutine, and claiming takes a short transaction.
- Set the orchestrator grace period above `ShutdownTimeout`.
- Monitor `work_queue_depth`, `work_failed_total` and `work_queue_wait_seconds`;
  alert on failed work and on queue wait, not on retries.
- Retention is yours: completed and failed rows stay for inspection. Delete or
  archive by `completed_at`/`created_at` (indexed); `work_created_idx` serves it.
- **Security.** Payloads are application data and may be sensitive: they are
  never logged or used as metric labels by default. Protect `Get`/`List` and the
  CLI behind your own authentication and authorization; an ID alone is not an
  access check. Keep personal data and secrets out of payloads when a reference
  will do, and out of `DispatchMetadata`. Error text is stored with the record:
  do not put payload contents in error messages.

## 18. Operating: inspection and the CLI

`rt.Get`, `rt.List`, `rt.Failed`, `rt.Stats`, `rt.Cancel`, `rt.Retry` are the
programmatic API. `work/cli` wraps them for your own command line:

```go
case "work":
    err = cli.Run(ctx, rt, os.Args[2:], os.Stdout)
```

```
myapp work stats
myapp work failed --queue billing --limit 20
myapp work inspect <id>            # payload hidden
myapp work inspect <id> --payload  # explicit opt-in
myapp work retry <id>
myapp work cancel <id>
```
