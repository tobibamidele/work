// Package work is typed, durable asynchronous execution for Go.
//
// Work is a unit of application behavior that is stored durably, validated at
// both ends, claimed by a worker under a lease, and executed at least once. It
// is not a job framework, an actor system, a workflow engine or a broker. The
// default backend is your own database, reached through Lathe; payload
// contracts are Validate schemas.
//
// # Mental model
//
// Dispatch stores work; it never executes it. A Worker claims work, runs the
// handler, and records the outcome. Between those two moments the work is only
// a row: it survives crashes, restarts and deploys.
//
//	producer ── Dispatch ──► [ durable record ] ◄── Claim ── Worker ── Handle
//	            validates                          lease,
//	            normalizes                         heartbeat
//
// Execution is at least once. A worker can crash after the handler's side
// effects and before the acknowledgement, so the same work may run twice.
// Handlers must be idempotent: use the work ID (or an explicit
// IdempotencyKey) when talking to external systems. See docs/guide.md.
//
// # Quick tour
//
//	type SendInvoice struct {
//	    InvoiceID string `json:"invoice_id"`
//	}
//
//	var SendInvoiceSchema = validate.Object[SendInvoice](
//	    validate.Field("invoice_id", validate.String().Trim().Min(1)),
//	)
//
//	func (j SendInvoice) Handle(ctx context.Context) error { /* ... */ }
//
//	registry := work.NewRegistry()
//	registry.MustRegister(work.Declare[SendInvoice]("send-invoice", work.Schema(SendInvoiceSchema)))
//
//	backend, _ := database.New(ctx, db)             // package database, on Lathe
//	rt, _ := work.NewRuntime(backend, registry, work.RuntimeConfig{})
//	rt.Dispatch(ctx, SendInvoice{InvoiceID: "inv_123"}, work.Queue("billing"))
//
//	worker, _ := work.NewWorker(backend, registry, work.WorkerConfig{
//	    Queues: []work.QueueConfig{{Name: "billing", Concurrency: 10}},
//	})
//	worker.Run(ctx)
//
// # Packages
//
//	work           core contracts, Runtime, Worker (depends on Validate only)
//	work/database  the Lathe-based backend: PostgreSQL, MySQL 8, SQLite
//	work/memory    in-memory reference backend for tests
//	work/redis     the Redis backend (separate module, go-redis)
//	work/backendtest  the conformance suite every backend must pass
//	work/cli       operator commands to mount in your application
package work
