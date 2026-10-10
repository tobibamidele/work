package work_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/tobibamidele/work"
)

func TestWorkerRunsWorkToCompletion(t *testing.T) {
	var got atomic.Value
	e := newEnv(t, invoiceDef(func(_ context.Context, i Invoice) error { got.Store(i); return nil }))
	ref := e.dispatch(Invoice{InvoiceID: " 7 ", Email: "Bob@Example.com"})
	start(t, fastWorker(e, work.WorkerConfig{}))
	rec := e.waitStatus(ref.ID, work.StatusCompleted)
	if rec.Attempts != 1 || rec.WorkerID != "" || rec.CompletedAt.IsZero() {
		t.Fatalf("record = %+v", rec)
	}
	if inv := got.Load().(Invoice); inv.InvoiceID != "7" || inv.Email != "bob@example.com" {
		t.Fatalf("the handler must receive the validated, normalized payload: %+v", inv)
	}
	eventually(t, "events", func() bool { return e.rec.count(work.EventCompleted) == 1 })
	names := e.rec.names()
	if names[0] != work.EventEnqueued || names[1] != work.EventStarted || names[2] != work.EventCompleted {
		t.Fatalf("events = %v", names)
	}
}

func TestWorkerRetriesUntilSuccess(t *testing.T) {
	var calls atomic.Int32
	e := newEnv(t, defPing("p", func(_ context.Context, _ Ping) error {
		if calls.Add(1) < 3 {
			return errors.New("flaky")
		}
		return nil
	}))
	ref := e.dispatch(Ping{N: 1}, work.MaxAttempts(5))
	start(t, fastWorker(e, work.WorkerConfig{}))
	rec := e.waitStatus(ref.ID, work.StatusCompleted)
	if rec.Attempts != 3 || calls.Load() != 3 {
		t.Fatalf("attempts = %d, calls = %d", rec.Attempts, calls.Load())
	}
	if rec.LastError == "" {
		t.Fatal("the last failure should remain on the record for diagnosis")
	}
	if e.rec.count(work.EventRetrying) != 2 {
		t.Fatalf("events = %v", e.rec.names())
	}
}

func TestWorkerFailsWhenAttemptsAreExhausted(t *testing.T) {
	var calls atomic.Int32
	e := newEnv(t, defPing("p", func(context.Context, Ping) error { calls.Add(1); return errors.New("always broken") }))
	ref := e.dispatch(Ping{N: 1}, work.MaxAttempts(3))
	start(t, fastWorker(e, work.WorkerConfig{}))
	rec := e.waitStatus(ref.ID, work.StatusFailed)
	if rec.Attempts != 3 || calls.Load() != 3 {
		t.Fatalf("attempts = %d, calls = %d", rec.Attempts, calls.Load())
	}
	mustContain(t, rec.LastError, "always broken", "attempt 3")
	ev, _ := e.rec.last(work.EventFailed)
	if ev.Err == nil || ev.Attempt != 3 {
		t.Fatalf("failed event = %+v", ev)
	}
	time.Sleep(60 * time.Millisecond)
	if calls.Load() != 3 {
		t.Fatal("failed work must not run again")
	}
}

func TestNonRetryableErrorsFailImmediately(t *testing.T) {
	var calls atomic.Int32
	e := newEnv(t, defPing("p", func(context.Context, Ping) error {
		calls.Add(1)
		return work.NonRetryable(errors.New("card declined"))
	}))
	ref := e.dispatch(Ping{N: 1}, work.MaxAttempts(5))
	start(t, fastWorker(e, work.WorkerConfig{}))
	rec := e.waitStatus(ref.ID, work.StatusFailed)
	if rec.Attempts != 1 || calls.Load() != 1 {
		t.Fatalf("attempts = %d", rec.Attempts)
	}
	mustContain(t, rec.LastError, "card declined")
}

func TestRetryAfterOverridesTheBackoff(t *testing.T) {
	var mu sync.Mutex
	var times []time.Time
	e := newEnv(t, defPing("p", func(context.Context, Ping) error {
		mu.Lock()
		times = append(times, time.Now())
		n := len(times)
		mu.Unlock()
		if n == 1 {
			return work.RetryAfter(150 * time.Millisecond)
		}
		return nil
	}))
	ref := e.dispatch(Ping{N: 1}, work.MaxAttempts(3))
	start(t, fastWorker(e, work.WorkerConfig{Backoff: work.FixedBackoff(time.Millisecond)}))
	e.waitStatus(ref.ID, work.StatusCompleted)
	mu.Lock()
	defer mu.Unlock()
	if gap := times[1].Sub(times[0]); gap < 140*time.Millisecond {
		t.Fatalf("retry came after %s, RetryAfter asked for 150ms", gap)
	}
}

func TestRetryAfterStillRespectsMaxAttempts(t *testing.T) {
	e := newEnv(t, defPing("p", func(context.Context, Ping) error { return work.RetryAfter(time.Millisecond) }))
	ref := e.dispatch(Ping{N: 1}, work.MaxAttempts(2))
	start(t, fastWorker(e, work.WorkerConfig{}))
	if rec := e.waitStatus(ref.ID, work.StatusFailed); rec.Attempts != 2 {
		t.Fatalf("attempts = %d", rec.Attempts)
	}
}

func TestDefinitionBackoffOverridesTheWorkers(t *testing.T) {
	var delays []int
	b := work.BackoffFunc(func(attempt int) time.Duration { delays = append(delays, attempt); return time.Millisecond })
	e := newEnv(t, defPing("p", func(context.Context, Ping) error { return errors.New("x") }, work.DefaultBackoff(b)))
	ref := e.dispatch(Ping{N: 1}, work.MaxAttempts(3))
	start(t, fastWorker(e, work.WorkerConfig{Backoff: work.FixedBackoff(time.Hour)}))
	e.waitStatus(ref.ID, work.StatusFailed)
	if len(delays) != 2 || delays[0] != 1 || delays[1] != 2 {
		t.Fatalf("backoff saw attempts %v", delays)
	}
}

func TestPanickingHandlersAreRecoveredAndRecorded(t *testing.T) {
	var calls atomic.Int32
	e := newEnv(t, defPing("p", func(_ context.Context, p Ping) error {
		if p.N == 0 {
			calls.Add(1)
			panic("kaboom")
		}
		return nil
	}))
	bad := e.dispatch(Ping{N: 0}, work.MaxAttempts(2))
	start(t, fastWorker(e, work.WorkerConfig{}))
	rec := e.waitStatus(bad.ID, work.StatusFailed)
	mustContain(t, rec.LastError, "panic: kaboom", "goroutine")
	if calls.Load() != 2 {
		t.Fatalf("a panic is a retryable failure, calls = %d", calls.Load())
	}
	good := e.dispatch(Ping{N: 1})
	e.waitStatus(good.ID, work.StatusCompleted) // the worker survived
	ev, _ := e.rec.last(work.EventFailed)
	var pe *work.PanicError
	if !errors.As(ev.Err, &pe) || pe.Value != "kaboom" || len(pe.Stack) == 0 {
		t.Fatalf("the event should carry the *PanicError, got %#v", ev.Err)
	}
}

func TestStoredPayloadThatFailsValidationFailsPermanently(t *testing.T) {
	var called atomic.Bool
	e := newEnv(t, invoiceDef(func(context.Context, Invoice) error { called.Store(true); return nil }))
	now := time.Now().UTC()
	rec := &work.Record{
		ID: work.ID(uuid.NewString()), Type: "send-invoice", Version: 1, Queue: "default",
		Payload: []byte(`{"invoice_id":"","email":"nope"}`), Status: work.StatusAvailable,
		MaxAttempts: 5, AvailableAt: now, CreatedAt: now, UpdatedAt: now,
	}
	if err := e.backend.Enqueue(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	start(t, fastWorker(e, work.WorkerConfig{}))
	got := e.waitStatus(rec.ID, work.StatusFailed)
	if got.Attempts != 1 || called.Load() {
		t.Fatalf("a payload that cannot be validated must not be retried or executed: attempts=%d called=%v", got.Attempts, called.Load())
	}
	mustContain(t, got.LastError, "invalid payload", "execute")
}

func TestUnsupportedVersionFailsClearlyWithoutDecoding(t *testing.T) {
	var called atomic.Bool
	e := newEnv(t, invoiceDef(func(context.Context, Invoice) error { called.Store(true); return nil }))
	now := time.Now().UTC()
	rec := &work.Record{
		ID: work.ID(uuid.NewString()), Type: "send-invoice", Version: 2, Queue: "default",
		Payload: []byte(`{"invoice_id":"1","email":"a@b.co","currency":"EUR"}`), Status: work.StatusAvailable,
		MaxAttempts: 5, AvailableAt: now, CreatedAt: now, UpdatedAt: now,
	}
	if err := e.backend.Enqueue(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	start(t, fastWorker(e, work.WorkerConfig{}))
	got := e.waitStatus(rec.ID, work.StatusFailed)
	mustContain(t, got.LastError, "version 2", "not supported", "[1]")
	if called.Load() {
		t.Fatal("a v2 payload must never be decoded as v1")
	}
	// the operator can retry it after deploying a worker that supports v2
	if err := e.rt.Retry(context.Background(), rec.ID); err != nil {
		t.Fatal(err)
	}
	if e.get(rec.ID).Status != work.StatusAvailable {
		t.Fatal("manual retry should requeue")
	}
}

func TestWorkersOnlyClaimTypesTheyCanExecute(t *testing.T) {
	e := newEnv(t, defPing("p", func(context.Context, Ping) error { return nil }))
	now := time.Now().UTC()
	rec := &work.Record{
		ID: work.ID(uuid.NewString()), Type: "mystery", Version: 1, Queue: "default",
		Payload: []byte(`{}`), Status: work.StatusAvailable, MaxAttempts: 3, AvailableAt: now, CreatedAt: now, UpdatedAt: now,
	}
	if err := e.backend.Enqueue(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	ref := e.dispatch(Ping{N: 1})
	start(t, fastWorker(e, work.WorkerConfig{}))
	e.waitStatus(ref.ID, work.StatusCompleted)
	time.Sleep(50 * time.Millisecond)
	if got := e.get(rec.ID); got.Status != work.StatusAvailable || got.Attempts != 0 {
		t.Fatalf("work of an unknown type must be left for a worker that knows it: %+v", got)
	}
}

func TestWorkersOnlyClaimConfiguredQueues(t *testing.T) {
	e := newEnv(t, defPing("p", func(context.Context, Ping) error { return nil }))
	mine := e.dispatch(Ping{N: 1}, work.Queue("mine"))
	other := e.dispatch(Ping{N: 1}, work.Queue("other"))
	start(t, fastWorker(e, work.WorkerConfig{Queues: []work.QueueConfig{{Name: "mine", Concurrency: 1}}}))
	e.waitStatus(mine.ID, work.StatusCompleted)
	time.Sleep(50 * time.Millisecond)
	if e.get(other.ID).Status != work.StatusAvailable {
		t.Fatal("an unconfigured queue must never be claimed")
	}
}

func TestDelayedWorkWaits(t *testing.T) {
	e := newEnv(t, defPing("p", func(context.Context, Ping) error { return nil }))
	ref := e.dispatch(Ping{N: 1}, work.Delay(200*time.Millisecond))
	start(t, fastWorker(e, work.WorkerConfig{}))
	time.Sleep(100 * time.Millisecond)
	if e.get(ref.ID).Status != work.StatusAvailable {
		t.Fatal("delayed work ran early")
	}
	e.waitStatus(ref.ID, work.StatusCompleted)
}

func TestPriorityDecidesTheOrder(t *testing.T) {
	var mu sync.Mutex
	var order []int
	e := newEnv(t, defPing("p", func(_ context.Context, p Ping) error { mu.Lock(); order = append(order, p.N); mu.Unlock(); return nil }))
	e.dispatch(Ping{N: 1}, work.Priority(0))
	e.dispatch(Ping{N: 2}, work.Priority(9))
	e.dispatch(Ping{N: 3}, work.Priority(5))
	start(t, fastWorker(e, work.WorkerConfig{Concurrency: 1}))
	eventually(t, "all done", func() bool { mu.Lock(); defer mu.Unlock(); return len(order) == 3 })
	if order[0] != 2 || order[1] != 3 || order[2] != 1 {
		t.Fatalf("order = %v, want [2 3 1]", order)
	}
}

func TestMetadataIdempotencyKeyAndMiddleware(t *testing.T) {
	var meta work.Metadata
	var ok bool
	var order []string
	var mu sync.Mutex
	rec := func(s string) { mu.Lock(); order = append(order, s); mu.Unlock() }
	e := newEnv(t, defPing("p", func(ctx context.Context, _ Ping) error {
		meta, ok = work.FromContext(ctx)
		rec("handler")
		return nil
	}, work.DefaultTimeout(time.Minute)))
	mw := func(name string) work.Middleware {
		return func(next work.Handler) work.Handler {
			return func(ctx context.Context) error {
				m, has := work.FromContext(ctx)
				if !has || m.WorkID == "" {
					t.Error("middleware must see the metadata")
				}
				rec(name + ">")
				err := next(ctx)
				rec("<" + name)
				return err
			}
		}
	}
	a := e.dispatch(Ping{N: 1}, work.DispatchMetadata("tenant", "acme"), work.Queue("q1"))
	w := fastWorker(e, work.WorkerConfig{Queues: []work.QueueConfig{{Name: "q1"}}, WorkerID: "worker-x"}, work.Use(mw("A"), mw("B")))
	start(t, w)
	e.waitStatus(a.ID, work.StatusCompleted)
	if !ok || meta.WorkID != a.ID || meta.Type != "p" || meta.Attempt != 1 || meta.MaxAttempts != 3 ||
		meta.Queue != "q1" || meta.WorkerID != "worker-x" || meta.Dispatch["tenant"] != "acme" || meta.Version != 1 {
		t.Fatalf("metadata = %+v", meta)
	}
	if meta.IdempotencyKey != string(a.ID) {
		t.Fatalf("the idempotency key defaults to the work ID, got %q", meta.IdempotencyKey)
	}
	if meta.Deadline.IsZero() || time.Until(meta.Deadline) > time.Minute {
		t.Fatalf("deadline = %v", meta.Deadline)
	}
	mu.Lock()
	got := order
	mu.Unlock()
	want := []string{"A>", "B>", "handler", "<B", "<A"}
	for i := range want {
		if i >= len(got) || got[i] != want[i] {
			t.Fatalf("middleware order = %v, want %v", got, want)
		}
	}
	if _, has := work.FromContext(context.Background()); has {
		t.Fatal("no metadata outside handlers")
	}

	b := e.dispatch(Ping{N: 2}, work.Queue("q1"), work.IdempotencyKey("order-77"))
	e.waitStatus(b.ID, work.StatusCompleted)
	if meta.IdempotencyKey != "order-77" {
		t.Fatalf("key = %q", meta.IdempotencyKey)
	}
}

func TestPerWorkTimeout(t *testing.T) {
	e := newEnv(t, defPing("p", func(ctx context.Context, _ Ping) error { <-ctx.Done(); return ctx.Err() }))
	ref := e.dispatch(Ping{N: 1}, work.MaxAttempts(1), work.Timeout(60*time.Millisecond))
	start(t, fastWorker(e, work.WorkerConfig{}))
	rec := e.waitStatus(ref.ID, work.StatusFailed)
	mustContain(t, rec.LastError, "deadline exceeded")
}

func TestQueueConcurrencyLimits(t *testing.T) {
	var cur, peak atomic.Int32
	e := newEnv(t, defPing("p", func(context.Context, Ping) error {
		n := cur.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(40 * time.Millisecond)
		cur.Add(-1)
		return nil
	}))
	for i := 0; i < 12; i++ {
		e.dispatch(Ping{N: i})
	}
	start(t, fastWorker(e, work.WorkerConfig{Queues: []work.QueueConfig{{Name: "default", Concurrency: 3}}}))
	eventually(t, "done", func() bool { return e.rec.count(work.EventCompleted) == 12 })
	if peak.Load() != 3 {
		t.Fatalf("peak concurrency = %d, want exactly 3", peak.Load())
	}
}

func TestGlobalConcurrencyBoundsAllQueues(t *testing.T) {
	var cur, peak atomic.Int32
	e := newEnv(t, defPing("p", func(context.Context, Ping) error {
		n := cur.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(40 * time.Millisecond)
		cur.Add(-1)
		return nil
	}))
	for i := 0; i < 8; i++ {
		e.dispatch(Ping{N: i}, work.Queue("a"))
		e.dispatch(Ping{N: i}, work.Queue("b"))
	}
	start(t, fastWorker(e, work.WorkerConfig{
		Concurrency: 3, Queues: []work.QueueConfig{{Name: "a", Concurrency: 5}, {Name: "b", Concurrency: 5}},
	}))
	eventually(t, "done", func() bool { return e.rec.count(work.EventCompleted) == 16 })
	if peak.Load() > 3 || peak.Load() < 2 {
		t.Fatalf("peak = %d, want <= 3 (the global bound) and > 1", peak.Load())
	}
}

func TestSeveralWorkersNeverRunTheSameWorkTwice(t *testing.T) {
	var mu sync.Mutex
	seen := map[int]int{}
	e := newEnv(t, defPing("p", func(_ context.Context, p Ping) error {
		mu.Lock()
		seen[p.N]++
		mu.Unlock()
		time.Sleep(2 * time.Millisecond)
		return nil
	}))
	const total = 150
	for i := 0; i < total; i++ {
		e.dispatch(Ping{N: i})
	}
	for i := 0; i < 5; i++ {
		start(t, fastWorker(e, work.WorkerConfig{Concurrency: 4}))
	}
	eventually(t, "all work done", func() bool { return e.rec.count(work.EventCompleted) == total })
	mu.Lock()
	defer mu.Unlock()
	for n, c := range seen {
		if c != 1 {
			t.Fatalf("work %d ran %d times", n, c)
		}
	}
	if len(seen) != total {
		t.Fatalf("ran %d of %d", len(seen), total)
	}
}

// ---- leases, crashes, cancellation, shutdown ----

func TestHeartbeatsKeepALongHandlerOwned(t *testing.T) {
	var runs atomic.Int32
	e := newEnv(t, defPing("slow", func(context.Context, Ping) error {
		runs.Add(1)
		time.Sleep(900 * time.Millisecond) // longer than the 300ms lease
		return nil
	}))
	ref := e.dispatch(Ping{N: 1})
	cfg := work.WorkerConfig{LeaseDuration: 300 * time.Millisecond, HeartbeatInterval: 60 * time.Millisecond}
	start(t, fastWorker(e, cfg))
	start(t, fastWorker(e, cfg)) // a second worker that would steal it if the lease lapsed
	e.waitStatus(ref.ID, work.StatusCompleted)
	if runs.Load() != 1 || e.get(ref.ID).Attempts != 1 {
		t.Fatalf("runs = %d, attempts = %d: the lease must have been extended", runs.Load(), e.get(ref.ID).Attempts)
	}
}

func TestCrashedWorkersWorkIsRecovered(t *testing.T) {
	var runs atomic.Int32
	e := newEnv(t, defPing("p", func(context.Context, Ping) error { runs.Add(1); return nil }))
	ref := e.dispatch(Ping{N: 1})
	// a worker claims the work and "crashes": it never heartbeats or settles
	recs, err := e.backend.Claim(context.Background(), work.ClaimOptions{
		Queues: []string{"default"}, WorkerID: "crashed", Now: time.Now().UTC(), LeaseDuration: 150 * time.Millisecond, Limit: 1,
	})
	if err != nil || len(recs) != 1 {
		t.Fatalf("claim = %v, %v", recs, err)
	}
	start(t, fastWorker(e, work.WorkerConfig{}))
	rec := e.waitStatus(ref.ID, work.StatusCompleted)
	if rec.Attempts != 2 || runs.Load() != 1 {
		t.Fatalf("attempts = %d, runs = %d", rec.Attempts, runs.Load())
	}
}

func TestReaperFailsExhaustedCrashedWork(t *testing.T) {
	e := newEnv(t, defPing("p", func(context.Context, Ping) error { return nil }))
	ref := e.dispatch(Ping{N: 1}, work.MaxAttempts(1))
	if _, err := e.backend.Claim(context.Background(), work.ClaimOptions{
		Queues: []string{"default"}, WorkerID: "crashed", Now: time.Now().UTC(), LeaseDuration: 100 * time.Millisecond, Limit: 1,
	}); err != nil {
		t.Fatal(err)
	}
	start(t, fastWorker(e, work.WorkerConfig{}))
	rec := e.waitStatus(ref.ID, work.StatusFailed)
	mustContain(t, rec.LastError, "lease expired")
	eventually(t, "failed event", func() bool { return e.rec.count(work.EventFailed) == 1 })
}

func TestLostLeaseCancelsTheHandlerAndNeverAcknowledges(t *testing.T) {
	w := &wrapped{Backend: newEnv(t).backend}
	w.extend = func(context.Context, work.ID, string, time.Time, time.Time) (bool, error) {
		return false, work.ErrLeaseLost
	}
	var cause atomic.Value
	var completeCalls atomic.Int32
	w.finish = func(ctx context.Context, id work.ID, wk string, now time.Time) error {
		completeCalls.Add(1)
		return w.Backend.Complete(ctx, id, wk, now)
	}
	e := newEnvWith(t, w, defPing("p", func(ctx context.Context, _ Ping) error {
		<-ctx.Done()
		cause.Store(context.Cause(ctx))
		return ctx.Err()
	}))
	ref := e.dispatch(Ping{N: 1})
	start(t, fastWorker(e, work.WorkerConfig{LeaseDuration: time.Second, HeartbeatInterval: 30 * time.Millisecond}))
	eventually(t, "handler cancelled", func() bool { return cause.Load() != nil })
	if !errors.Is(cause.Load().(error), work.ErrLeaseLost) {
		t.Fatalf("cause = %v", cause.Load())
	}
	eventually(t, "lease lost event", func() bool { return e.rec.count(work.EventLeaseLost) >= 1 })
	time.Sleep(50 * time.Millisecond)
	if completeCalls.Load() != 0 {
		t.Fatal("a worker that lost its lease must not acknowledge the work")
	}
	if e.get(ref.ID).Status != work.StatusRunning {
		t.Fatal("the record belongs to whoever reclaims it; the old worker must leave it alone")
	}
}

func TestTemporaryHeartbeatErrorsDoNotKillTheHandler(t *testing.T) {
	w := &wrapped{Backend: newEnv(t).backend}
	var beats atomic.Int32
	w.extend = func(ctx context.Context, id work.ID, wk string, until, now time.Time) (bool, error) {
		if beats.Add(1) <= 2 {
			return false, errors.New("database hiccup")
		}
		return w.Backend.ExtendLease(ctx, id, wk, until, now)
	}
	var cancelled atomic.Bool
	e := newEnvWith(t, w, defPing("p", func(ctx context.Context, _ Ping) error {
		select {
		case <-ctx.Done():
			cancelled.Store(true)
		case <-time.After(300 * time.Millisecond):
		}
		return nil
	}))
	ref := e.dispatch(Ping{N: 1})
	start(t, fastWorker(e, work.WorkerConfig{LeaseDuration: 5 * time.Second, HeartbeatInterval: 40 * time.Millisecond}))
	e.waitStatus(ref.ID, work.StatusCompleted)
	if cancelled.Load() {
		t.Fatal("ownership was never disproved, so the handler must not be cancelled")
	}
}

func TestCancellingRunningWorkStopsTheHandlerCooperatively(t *testing.T) {
	var runs atomic.Int32
	started := make(chan struct{}, 4)
	e := newEnv(t, defPing("p", func(ctx context.Context, _ Ping) error {
		runs.Add(1)
		started <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	}))
	ref := e.dispatch(Ping{N: 1}, work.MaxAttempts(5))
	start(t, fastWorker(e, work.WorkerConfig{LeaseDuration: 2 * time.Second, HeartbeatInterval: 40 * time.Millisecond}))
	<-started
	out, err := e.rt.Cancel(context.Background(), ref.ID)
	if err != nil || out != work.CancelRequested {
		t.Fatalf("cancel = %v, %v", out, err)
	}
	if e.get(ref.ID).Status != work.StatusRunning {
		t.Fatal("work must not be marked cancelled while its handler may still be running")
	}
	rec := e.waitStatus(ref.ID, work.StatusCancelled)
	time.Sleep(60 * time.Millisecond)
	if runs.Load() != 1 || rec.Attempts != 1 {
		t.Fatalf("cancelled work must not be retried: runs=%d attempts=%d", runs.Load(), rec.Attempts)
	}
	eventually(t, "event", func() bool { return e.rec.count(work.EventCancelled) == 1 })
}

func TestCancellingAvailableWorkIsImmediate(t *testing.T) {
	e := newEnv(t, defPing("p", func(context.Context, Ping) error { return nil }))
	ref := e.dispatch(Ping{N: 1}, work.Delay(time.Hour))
	out, err := e.rt.Cancel(context.Background(), ref.ID)
	if err != nil || out != work.Cancelled || e.get(ref.ID).Status != work.StatusCancelled {
		t.Fatalf("cancel = %v, %v", out, err)
	}
	if e.rec.count(work.EventCancelled) != 1 {
		t.Fatal("event")
	}
	if err := e.rt.Retry(context.Background(), ref.ID); err != nil {
		t.Fatal(err)
	}
	if e.get(ref.ID).Status != work.StatusAvailable {
		t.Fatal("a cancelled item can be retried manually")
	}
}

func TestGracefulShutdownWaitsForRunningHandlers(t *testing.T) {
	running := make(chan struct{})
	e := newEnv(t, defPing("p", func(context.Context, Ping) error {
		close(running)
		time.Sleep(250 * time.Millisecond)
		return nil
	}))
	ref := e.dispatch(Ping{N: 1})
	stop := start(t, fastWorker(e, work.WorkerConfig{ShutdownTimeout: 5 * time.Second}))
	<-running
	startStop := time.Now()
	if err := stop(); err != nil {
		t.Fatalf("Run = %v", err)
	}
	if time.Since(startStop) < 200*time.Millisecond {
		t.Fatal("shutdown must wait for the running handler")
	}
	if rec := e.get(ref.ID); rec.Status != work.StatusCompleted {
		t.Fatalf("status = %s: the handler's result must be recorded before Run returns", rec.Status)
	}
}

func TestShutdownStopsClaimingNewWork(t *testing.T) {
	var runs atomic.Int32
	e := newEnv(t, defPing("p", func(context.Context, Ping) error { runs.Add(1); return nil }))
	w := fastWorker(e, work.WorkerConfig{})
	stop := start(t, w)
	time.Sleep(30 * time.Millisecond)
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	ref := e.dispatch(Ping{N: 1})
	time.Sleep(60 * time.Millisecond)
	if e.get(ref.ID).Status != work.StatusAvailable || runs.Load() != 0 {
		t.Fatal("a stopped worker must not claim")
	}
}

func TestForcedShutdownReleasesWorkWithoutConsumingAnAttempt(t *testing.T) {
	running := make(chan struct{})
	e := newEnv(t, defPing("p", func(ctx context.Context, _ Ping) error {
		close(running)
		<-ctx.Done()
		return ctx.Err()
	}))
	ref := e.dispatch(Ping{N: 1}, work.MaxAttempts(1))
	stop := start(t, fastWorker(e, work.WorkerConfig{ShutdownTimeout: 80 * time.Millisecond}))
	<-running
	if err := stop(); err != nil {
		t.Fatalf("Run = %v: the handler honored its cancelled context", err)
	}
	rec := e.get(ref.ID)
	if rec.Status != work.StatusAvailable || rec.Attempts != 0 {
		t.Fatalf("interrupted work must be handed back untouched, got %s with %d attempts", rec.Status, rec.Attempts)
	}
}

func TestShutdownTimeoutIsReportedForHandlersThatIgnoreTheirContext(t *testing.T) {
	if testing.Short() {
		t.Skip("waits for the forced-return window")
	}
	release := make(chan struct{})
	running := make(chan struct{})
	e := newEnv(t, defPing("p", func(context.Context, Ping) error {
		close(running)
		<-release
		return nil
	}))
	ref := e.dispatch(Ping{N: 1})
	stop := start(t, fastWorker(e, work.WorkerConfig{ShutdownTimeout: 30 * time.Millisecond}))
	<-running
	err := stop()
	wantIs(t, err, work.ErrShutdownTimeout)
	close(release)
	if e.get(ref.ID).Status != work.StatusRunning && e.get(ref.ID).Status != work.StatusCompleted {
		t.Fatalf("status = %s", e.get(ref.ID).Status)
	}
}

// ---- infrastructure failures ----

func TestWorkerSurvivesBackendOutagesWhileClaiming(t *testing.T) {
	w := &wrapped{Backend: newEnv(t).backend}
	var fails atomic.Int32
	w.claim = func(ctx context.Context, o work.ClaimOptions) ([]*work.Record, error) {
		if fails.Add(1) <= 3 {
			return nil, errors.New("connection refused")
		}
		return w.Backend.Claim(ctx, o)
	}
	e := newEnvWith(t, w, defPing("p", func(context.Context, Ping) error { return nil }))
	ref := e.dispatch(Ping{N: 1})
	start(t, fastWorker(e, work.WorkerConfig{}))
	e.waitStatus(ref.ID, work.StatusCompleted)
	if fails.Load() < 4 {
		t.Fatal("outage was not exercised")
	}
}

func TestSettlementRetriesTransientBackendErrors(t *testing.T) {
	w := &wrapped{Backend: newEnv(t).backend}
	var calls atomic.Int32
	w.finish = func(ctx context.Context, id work.ID, wk string, now time.Time) error {
		if calls.Add(1) <= 2 {
			return errors.New("connection reset")
		}
		return w.Backend.Complete(ctx, id, wk, now)
	}
	var runs atomic.Int32
	e := newEnvWith(t, w, defPing("p", func(context.Context, Ping) error { runs.Add(1); return nil }))
	ref := e.dispatch(Ping{N: 1})
	start(t, fastWorker(e, work.WorkerConfig{}))
	e.waitStatus(ref.ID, work.StatusCompleted)
	if runs.Load() != 1 || calls.Load() != 3 {
		t.Fatalf("runs = %d, complete calls = %d: the handler must not rerun just because the acknowledgement failed", runs.Load(), calls.Load())
	}
}

// ---- configuration ----

func TestConfigurationErrorsAreReportedAtStartup(t *testing.T) {
	e := newEnv(t, defPing("p", func(context.Context, Ping) error { return nil }))
	bad := map[string]work.WorkerConfig{
		"heartbeat >= lease": {LeaseDuration: time.Second, HeartbeatInterval: time.Second},
		"duplicate queue":    {Queues: []work.QueueConfig{{Name: "a"}, {Name: "a"}}},
		"invalid queue":      {Queues: []work.QueueConfig{{Name: "a b"}}},
		"negative conc":      {Concurrency: -1},
		"negative queue":     {Queues: []work.QueueConfig{{Name: "a", Concurrency: -1}}},
		"negative duration":  {PollInterval: -time.Second},
	}
	for name, cfg := range bad {
		if _, err := work.NewWorker(e.backend, e.reg, cfg); !errors.Is(err, work.ErrConfig) {
			t.Errorf("%s: err = %v, want ErrConfig", name, err)
		}
	}
	if _, err := work.NewWorker(nil, e.reg, work.WorkerConfig{}); !errors.Is(err, work.ErrConfig) {
		t.Error("nil backend")
	}
	if _, err := work.NewWorker(e.backend, nil, work.WorkerConfig{}); !errors.Is(err, work.ErrConfig) {
		t.Error("nil registry")
	}
	if _, err := work.NewRuntime(nil, e.reg, work.RuntimeConfig{}); !errors.Is(err, work.ErrConfig) {
		t.Error("runtime nil backend")
	}
	if _, err := work.NewRuntime(e.backend, nil, work.RuntimeConfig{}); !errors.Is(err, work.ErrConfig) {
		t.Error("runtime nil registry")
	}
	if _, err := work.NewRuntime(e.backend, e.reg, work.RuntimeConfig{DefaultQueue: "a b"}); !errors.Is(err, work.ErrConfig) {
		t.Error("runtime bad queue")
	}

	empty := work.NewRegistry()
	w, err := work.NewWorker(e.backend, empty, work.WorkerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Run(context.Background()); !errors.Is(err, work.ErrConfig) {
		t.Errorf("a worker with nothing to run must refuse to start, got %v", err)
	}

	w2 := fastWorker(e, work.WorkerConfig{})
	stop := start(t, w2)
	time.Sleep(20 * time.Millisecond)
	if err := w2.Run(context.Background()); !errors.Is(err, work.ErrConfig) {
		t.Errorf("a worker cannot run twice, got %v", err)
	}
	_ = stop()
	if w2.WorkerID() == "" || w2.String() == "" {
		t.Error("worker identity")
	}
}

func TestInspectionAPIs(t *testing.T) {
	e := newEnv(t, defPing("p", func(context.Context, Ping) error { return errors.New("nope") }))
	ref := e.dispatch(Ping{N: 1}, work.MaxAttempts(1))
	start(t, fastWorker(e, work.WorkerConfig{}))
	e.waitStatus(ref.ID, work.StatusFailed)
	failed, err := e.rt.Failed(context.Background(), 10)
	if err != nil || len(failed) != 1 || failed[0].ID != ref.ID {
		t.Fatalf("failed = %v, %v", failed, err)
	}
	stats, err := e.rt.Stats(context.Background())
	if err != nil || len(stats) != 1 || stats[0].Status != work.StatusFailed || stats[0].Count != 1 {
		t.Fatalf("stats = %v, %v", stats, err)
	}
	if _, err := e.rt.Get(context.Background(), "missing"); !errors.Is(err, work.ErrNotFound) {
		t.Fatal("get missing")
	}
	if err := e.rt.Retry(context.Background(), ref.ID); err != nil {
		t.Fatal(err)
	}
	wantIs(t, e.rt.Retry(context.Background(), "missing"), work.ErrNotFound)
	eventually(t, "runs again", func() bool { return e.get(ref.ID).Attempts >= 1 && e.get(ref.ID).Status == work.StatusFailed })
}
