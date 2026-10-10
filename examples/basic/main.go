// Command basic shows the whole life of durable work in one file: define a
// payload contract, register handlers, dispatch, run a worker, retry a flaky
// handler, and inspect the result.
//
//	go run ./basic
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/tobibamidele/lathe/sqlite"
	"github.com/tobibamidele/validate"
	"github.com/tobibamidele/work"
	"github.com/tobibamidele/work/database"
)

// SendWelcomeEmail is a unit of durable work. The json tags must produce the
// field names of the schema below.
type SendWelcomeEmail struct {
	UserID string `json:"user_id"`
	Email  string `json:"email"`
}

// The payload contract: validated when dispatched and again before the
// handler runs. The normalized value (trimmed, lower-cased) is what is stored.
var SendWelcomeEmailSchema = validate.Object[SendWelcomeEmail](
	validate.Field("user_id", validate.String().Trim().Min(1).Max(64)),
	validate.Field("email", validate.String().Trim().Lower().Email().Max(255)),
)

var attempts atomic.Int32

// Handle is the handler: ordinary Go, the idiomatic signature. Work runs at
// least once, so use the idempotency key with external systems.
func (j SendWelcomeEmail) Handle(ctx context.Context) error {
	meta, _ := work.FromContext(ctx)
	if attempts.Add(1) < 3 {
		return fmt.Errorf("smtp timeout (attempt %d)", meta.Attempt) // retried with backoff
	}
	fmt.Printf("sent welcome email to %s (idempotency key %s, attempt %d)\n", j.Email, meta.IdempotencyKey, meta.Attempt)
	return nil
}

type ChargeCard struct {
	OrderID string `json:"order_id"`
	Cents   int    `json:"cents"`
}

var ChargeCardSchema = validate.Object[ChargeCard](
	validate.Field("order_id", validate.String().Min(1)),
	validate.Field("cents", validate.Int().Min(1)),
)

func main() {
	ctx := context.Background()

	// 1. Database: Lathe handle, migrations shipped with the library.
	dir, err := os.MkdirTemp("", "work-example-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	db, err := sqlite.Open(filepath.Join(dir, "app.db"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	if _, err := database.Migrate(ctx, db); err != nil {
		log.Fatal(err)
	}
	backend, err := database.New(ctx, db)
	if err != nil {
		log.Fatal(err)
	}

	// 2. Registry: stable names, versions, schemas. Application-owned.
	registry := work.NewRegistry()
	registry.MustRegister(
		work.Declare[SendWelcomeEmail]("send-welcome-email",
			work.Schema(SendWelcomeEmailSchema), work.DefaultQueue("emails"), work.DefaultMaxAttempts(5)),
		work.Define("charge-card", ChargeCardSchema, func(ctx context.Context, c ChargeCard) error {
			if c.Cents > 100_000 {
				return work.NonRetryable(errors.New("amount exceeds the limit")) // never retried
			}
			fmt.Printf("charged order %s: %d cents\n", c.OrderID, c.Cents)
			return nil
		}),
	)

	// 3. Runtime for dispatching, Worker for executing.
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	rt, err := work.NewRuntime(backend, registry, work.RuntimeConfig{Observer: work.SlogObserver(logger)})
	if err != nil {
		log.Fatal(err)
	}
	worker, err := work.NewWorker(backend, registry, work.WorkerConfig{
		Queues:       []work.QueueConfig{{Name: "emails", Concurrency: 2}, {Name: "default", Concurrency: 2}},
		PollInterval: 50 * time.Millisecond,
		Backoff:      work.ExponentialBackoff{Initial: 100 * time.Millisecond, Maximum: time.Second},
		Logger:       logger,
	}, work.Use(work.LoggingMiddleware(logger)))
	if err != nil {
		log.Fatal(err)
	}

	// 4. Dispatch. The payload is validated and normalized before it is stored.
	ref, err := rt.Dispatch(ctx, SendWelcomeEmail{UserID: "u_1", Email: "  Ada@Example.COM "})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("dispatched", ref.ID, "to queue", ref.Queue)

	_, err = rt.Dispatch(ctx, SendWelcomeEmail{UserID: "", Email: "nope"}) // rejected, nothing persisted
	fmt.Println("invalid payload rejected:", errors.Is(err, work.ErrInvalidPayload))

	if _, err := rt.Dispatch(ctx, ChargeCard{OrderID: "o_1", Cents: 4_200}, work.Unique("charge:o_1")); err != nil {
		log.Fatal(err)
	}
	_, err = rt.Dispatch(ctx, ChargeCard{OrderID: "o_1", Cents: 4_200}, work.Unique("charge:o_1"))
	fmt.Println("duplicate charge blocked:", errors.Is(err, work.ErrDuplicate))
	huge, _ := rt.Dispatch(ctx, ChargeCard{OrderID: "o_2", Cents: 999_999})
	if _, err := rt.DispatchAt(ctx, ChargeCard{OrderID: "o_3", Cents: 100}, time.Now().Add(300*time.Millisecond)); err != nil {
		log.Fatal(err)
	}

	// 5. Run the worker until the work is done (a real service runs until a
	// signal arrives: signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)).
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- worker.Run(runCtx) }()
	for {
		recs, _ := rt.List(ctx, work.ListFilter{Statuses: []work.Status{work.StatusAvailable, work.StatusRunning}})
		if len(recs) == 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	stop()
	if err := <-done; err != nil {
		log.Fatal(err)
	}

	// 6. Inspect.
	rec, _ := rt.Get(ctx, huge.ID)
	fmt.Printf("charge o_2 is %s after %d attempt: %s\n", rec.Status, rec.Attempts, rec.LastError)
	stats, _ := rt.Stats(ctx)
	for _, s := range stats {
		fmt.Printf("  %-8s %-10s %d\n", s.Queue, s.Status, s.Count)
	}
}
