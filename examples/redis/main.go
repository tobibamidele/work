// Command redis shows the complete API with the Redis backend.
//
//	go run ./redis            (REDIS_ADDR defaults to localhost:6379)
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/tobibamidele/validate"
	"github.com/tobibamidele/work"
	"github.com/tobibamidele/work/cli"
	workredis "github.com/tobibamidele/work/redis"
)

// ---- payloads: a type, a Validate schema, a Handle method ----

type SendInvoice struct {
	InvoiceID string `json:"invoice_id"` // tags must match the schema's field names
	Email     string `json:"email"`
}

var SendInvoiceSchema = validate.Object[SendInvoice](
	validate.Field("invoice_id", validate.String().Trim().Min(1).Max(64)),
	validate.Field("email", validate.String().Trim().Lower().Email()),
)

func (j SendInvoice) Handle(ctx context.Context) error {
	meta, _ := work.FromContext(ctx) // ID, Attempt, IdempotencyKey, Queue, ...
	fmt.Printf("invoice %s -> %s (attempt %d, idempotency key %s)\n", j.InvoiceID, j.Email, meta.Attempt, meta.IdempotencyKey)
	return nil
}

type Reindex struct {
	AccountID string `json:"account_id"`
}

var ReindexSchema = validate.Object[Reindex](validate.Field("account_id", validate.String().Min(1)))

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	// 1. Redis client (single node here; Sentinel/Cluster clients work too) and backend.
	client := goredis.NewClient(&goredis.Options{Addr: envOr("REDIS_ADDR", "localhost:6379")})
	defer client.Close()
	backend, err := workredis.New(ctx, client, workredis.WithPrefix("myapp")) // fails fast if Redis is unreachable
	if err != nil {
		log.Fatal(err)
	}

	// 2. Registry: stable names + versions.
	registry := work.NewRegistry()
	registry.MustRegister(
		work.Declare[SendInvoice]("send-invoice",
			work.Schema(SendInvoiceSchema),
			work.DefaultQueue("billing"), work.DefaultMaxAttempts(5), work.DefaultTimeout(30*time.Second)),
		work.Define("reindex", ReindexSchema, func(ctx context.Context, r Reindex) error {
			select { // honor ctx: cancellation and timeouts arrive through it
			case <-time.After(100 * time.Millisecond):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}, work.DefaultQueue("maintenance")),
	)

	// 3. Runtime: dispatch + inspect.
	rt, err := work.NewRuntime(backend, registry, work.RuntimeConfig{
		Observer: work.SlogObserver(logger),
	})
	if err != nil {
		log.Fatal(err)
	}
	work.SetDefault(rt) // optional: lets you call work.Dispatch(ctx, ...) anywhere

	// 4. Worker.
	worker, err := work.NewWorker(backend, registry, work.WorkerConfig{
		Queues: []work.QueueConfig{
			{Name: "billing", Concurrency: 10},
			{Name: "maintenance", Concurrency: 2},
		},
		Concurrency:       12, // global cap across queues
		LeaseDuration:     2 * time.Minute,
		HeartbeatInterval: 40 * time.Second,
		PollInterval:      250 * time.Millisecond,
		ShutdownTimeout:   30 * time.Second,
		Backoff:           work.ExponentialBackoff{Initial: time.Second, Maximum: 10 * time.Minute},
		Logger:            logger,
	}, work.Use(work.LoggingMiddleware(logger)))
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		if err := worker.Run(ctx); err != nil { // returns after graceful shutdown
			log.Println("worker:", err)
		}
	}()

	// 5. Dispatch (Redis: no transactional dispatch).
	ref, err := rt.Dispatch(ctx, SendInvoice{InvoiceID: " inv_1 ", Email: "Ada@Example.com"},
		work.Queue("billing"),
		work.MaxAttempts(10),
		work.Priority(5),
		work.IdempotencyKey("invoice-inv_1-email"),
		work.Timeout(20*time.Second),
		work.DispatchMetadata("tenant", "acme"),
	)
	if err != nil {
		log.Fatal(err) // invalid payloads fail here with errors.Is(err, work.ErrInvalidPayload)
	}
	fmt.Println("dispatched", ref.ID)

	_, err = rt.DispatchAt(ctx, Reindex{AccountID: "a1"}, time.Now().Add(2*time.Second), work.Unique("reindex:a1"))
	_, dupErr := rt.Dispatch(ctx, Reindex{AccountID: "a1"}, work.Unique("reindex:a1"))
	fmt.Println("scheduled:", err == nil, "| duplicate blocked:", errors.Is(dupErr, work.ErrDuplicate))
	work.Dispatch(ctx, Reindex{AccountID: "a2"}, work.Delay(time.Second), work.UniquePayload()) // package-level form

	time.Sleep(3 * time.Second)

	// 6. Inspect and operate.
	rec, _ := rt.Get(ctx, ref.ID)
	fmt.Println("status:", rec.Status, "attempts:", rec.Attempts)
	failed, _ := rt.Failed(ctx, 20)
	stats, _ := rt.Stats(ctx)
	fmt.Println("failed:", len(failed), "stats:", stats)
	// rt.Cancel(ctx, id) / rt.Retry(ctx, id) / rt.List(ctx, work.ListFilter{Queue: "billing", Limit: 50})
	_ = cli.Run(ctx, rt, []string{"stats"}, os.Stdout) // the same operations as CLI commands

	// 7. Retention: Redis keeps everything in memory, so prune finished work (run daily).
	n, err := backend.Prune(ctx, time.Now().Add(-7*24*time.Hour))
	fmt.Println("pruned:", n, err)

	stop() // Ctrl-C does this too: the worker finishes running handlers, then exits
	time.Sleep(200 * time.Millisecond)
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
