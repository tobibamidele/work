// Package backendtest is the conformance suite every work.Backend must pass.
//
// The suite pins the portable semantics: enqueue durability, claiming,
// ownership, leases, retries, cancellation, uniqueness and recovery. A new
// backend (Redis, RabbitMQ, ...) is not done until it passes this suite
// unchanged, or refuses the unsupported features through Capabilities.
//
//	func TestConformance(t *testing.T) {
//	    backendtest.Run(t, func(t *testing.T) work.Backend { return newTestBackend(t) })
//	}
//
// The factory must return an empty backend for every call.
package backendtest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/tobibamidele/work"
)

// Factory returns a fresh, empty backend.
type Factory func(t *testing.T) work.Backend

var (
	t0      = time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	lease   = 2 * time.Minute
	counter atomic.Int64
	ctx     = context.Background()
)

// mk builds a valid available record created "now" (t0), with distinct
// creation times so ordering is deterministic.
func mk(mod ...func(*work.Record)) *work.Record {
	n := counter.Add(1)
	created := t0.Add(time.Duration(n) * time.Millisecond)
	r := &work.Record{
		ID:          work.ID(uuid.Must(uuid.NewV7()).String()),
		Type:        "test.work",
		Version:     1,
		Queue:       "default",
		Payload:     []byte(`{"n":1}`),
		Status:      work.StatusAvailable,
		MaxAttempts: 3,
		AvailableAt: t0,
		CreatedAt:   created,
		UpdatedAt:   created,
	}
	for _, m := range mod {
		m(r)
	}
	return r
}

func enqueue(t *testing.T, b work.Backend, mod ...func(*work.Record)) *work.Record {
	t.Helper()
	r := mk(mod...)
	if err := b.Enqueue(ctx, r); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	return r
}

func claimOpts(worker string, now time.Time, limit int, queues ...string) work.ClaimOptions {
	if len(queues) == 0 {
		queues = []string{"default"}
	}
	return work.ClaimOptions{Queues: queues, WorkerID: worker, Now: now, LeaseDuration: lease, Limit: limit}
}

func claimOne(t *testing.T, b work.Backend, worker string, now time.Time) *work.Record {
	t.Helper()
	recs, err := b.Claim(ctx, claimOpts(worker, now, 1))
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("claim returned %d records, want 1", len(recs))
	}
	return recs[0]
}

func mustGet(t *testing.T, b work.Backend, id work.ID) *work.Record {
	t.Helper()
	r, err := b.Get(ctx, id)
	if err != nil {
		t.Fatalf("get %s: %v", id, err)
	}
	return r
}

func wantErr(t *testing.T, err, target error) {
	t.Helper()
	if !errors.Is(err, target) {
		t.Fatalf("error = %v, want one matching %v", err, target)
	}
}

func wantNoWork(t *testing.T, b work.Backend, worker string, now time.Time) {
	t.Helper()
	recs, err := b.Claim(ctx, claimOpts(worker, now, 5))
	if !errors.Is(err, work.ErrNoWork) {
		t.Fatalf("claim = %v, %v; want ErrNoWork", recs, err)
	}
}

func sameJSON(t *testing.T, got, want []byte) {
	t.Helper()
	norm := func(b []byte) string {
		dec := json.NewDecoder(bytes.NewReader(b))
		dec.UseNumber()
		var v any
		if err := dec.Decode(&v); err != nil {
			t.Fatalf("payload %q is not JSON: %v", b, err)
		}
		out, _ := json.Marshal(v)
		return string(out)
	}
	if norm(got) != norm(want) {
		t.Fatalf("payload = %s, want %s", got, want)
	}
}

func sameTime(t *testing.T, name string, got, want time.Time) {
	t.Helper()
	if !got.Equal(want) {
		t.Fatalf("%s = %v, want %v", name, got, want)
	}
}

// Run executes the conformance suite.
func Run(t *testing.T, newBackend Factory) {
	cases := []struct {
		name string
		fn   func(*testing.T, work.Backend)
	}{
		{"EnqueuePersistsEveryField", enqueuePersists},
		{"PayloadRoundTrip", payloadRoundTrip},
		{"GetUnknownIsNotFound", getUnknown},
		{"ClaimReturnsAvailableWork", claimAvailable},
		{"ClaimEmptyReturnsErrNoWork", claimEmpty},
		{"ClaimSkipsDelayedWork", claimDelayed},
		{"ClaimRespectsQueues", claimQueues},
		{"ClaimRespectsTypes", claimTypes},
		{"ClaimOrdersByPriorityThenAvailabilityThenCreation", claimOrdering},
		{"ClaimBatchReturnsDistinctRecords", claimBatch},
		{"ConcurrentClaimsNeverShareARecord", claimConcurrent},
		{"CompleteRequiresOwnership", completeOwnership},
		{"CompleteOfUnclaimedWorkIsRejected", completeUnclaimed},
		{"RetryReschedulesAndRecordsError", retryReschedules},
		{"RetryRequiresOwnership", retryOwnership},
		{"FailedWorkIsNotClaimable", failedNotClaimable},
		{"CancelledWorkIsNotClaimable", cancelledNotClaimable},
		{"CancelOutcomes", cancelOutcomes},
		{"CancelRunningIsCooperative", cancelRunning},
		{"ExpiredLeasesBecomeReclaimable", expiredReclaim},
		{"ExpiredLeaseIsReclaimedByExactlyOneWorker", expiredReclaimRace},
		{"ExhaustedExpiredWorkIsReapedNotReclaimed", expiredExhausted},
		{"ExpiredLeaseWithCancelRequestIsReapedAsCancelled", expiredCancelled},
		{"ExtendLeaseKeepsOwnership", extendLease},
		{"ReleaseGivesTheAttemptBack", releaseWork},
		{"UniqueKeyBlocksOnlyActiveWork", uniqueActive},
		{"UniqueKeysAreScopedPerQueue", uniquePerQueue},
		{"WorkWithoutUniqueKeyIsNeverBlocked", uniqueNone},
		{"RequeueStartsANewCycle", requeue},
		{"ListFiltersAndOrders", listFilters},
		{"StatsCountsByQueueAndStatus", stats},
		{"CompletedWorkIsRetained", retained},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			b := newBackend(t)
			c.fn(t, b)
		})
	}
}

func enqueuePersists(t *testing.T, b work.Backend) {
	want := mk(func(r *work.Record) {
		r.Type, r.Version, r.Queue = "send-invoice", 2, "emails"
		r.Payload = []byte(`{"invoice_id":"inv_1"}`)
		r.Priority, r.MaxAttempts = 7, 5
		r.AvailableAt = t0.Add(time.Hour)
		r.IdempotencyKey = "idem-1"
		r.UniqueKey, r.ActiveUniqueKey = "invoice:1", "invoice:1"
		r.SchemaFingerprint = "fp-123"
		r.Timeout = 90 * time.Second
		r.Metadata = map[string]string{"traceparent": "00-abc-def-01", "tenant": "t1"}
	})
	if err := b.Enqueue(ctx, want); err != nil {
		t.Fatal(err)
	}
	got := mustGet(t, b, want.ID)
	if got.Type != want.Type || got.Version != 2 || got.Queue != "emails" {
		t.Fatalf("identity not persisted: %+v", got)
	}
	sameJSON(t, got.Payload, want.Payload)
	if got.Status != work.StatusAvailable || got.Priority != 7 || got.MaxAttempts != 5 || got.Attempts != 0 {
		t.Fatalf("scheduling fields not persisted: %+v", got)
	}
	sameTime(t, "available_at", got.AvailableAt, want.AvailableAt)
	sameTime(t, "created_at", got.CreatedAt, want.CreatedAt)
	if got.IdempotencyKey != "idem-1" || got.UniqueKey != "invoice:1" || got.ActiveUniqueKey != "invoice:1" || got.SchemaFingerprint != "fp-123" {
		t.Fatalf("keys not persisted: %+v", got)
	}
	if got.Timeout != 90*time.Second {
		t.Fatalf("timeout = %v", got.Timeout)
	}
	if len(got.Metadata) != 2 || got.Metadata["traceparent"] != "00-abc-def-01" || got.Metadata["tenant"] != "t1" {
		t.Fatalf("metadata = %v", got.Metadata)
	}
	if got.WorkerID != "" || !got.StartedAt.IsZero() || !got.LeaseUntil.IsZero() || !got.CompletedAt.IsZero() {
		t.Fatalf("a fresh record must have no execution state: %+v", got)
	}
}

func payloadRoundTrip(t *testing.T, b work.Backend) {
	payload := []byte(`{"big":9007199254740993,"s":"héllo \"quoted\" ✓","nested":{"a":[1,2,{"b":null}]},"f":1.5}`)
	r := enqueue(t, b, func(r *work.Record) { r.Payload = payload })
	sameJSON(t, mustGet(t, b, r.ID).Payload, payload)
	claimed := claimOne(t, b, "w1", t0)
	sameJSON(t, claimed.Payload, payload)
}

func getUnknown(t *testing.T, b work.Backend) {
	_, err := b.Get(ctx, work.ID(uuid.NewString()))
	wantErr(t, err, work.ErrNotFound)
	_, err = b.Get(ctx, "not-a-valid-id")
	wantErr(t, err, work.ErrNotFound)
}

func claimAvailable(t *testing.T, b work.Backend) {
	r := enqueue(t, b)
	now := t0.Add(5 * time.Second)
	got := claimOne(t, b, "w1", now)
	if got.ID != r.ID || got.Status != work.StatusRunning || got.Attempts != 1 || got.WorkerID != "w1" {
		t.Fatalf("claimed = %+v", got)
	}
	sameTime(t, "lease_until", got.LeaseUntil, now.Add(lease))
	sameTime(t, "locked_at", got.LockedAt, now)
	sameTime(t, "started_at", got.StartedAt, now)
	stored := mustGet(t, b, r.ID)
	if stored.Status != work.StatusRunning || stored.WorkerID != "w1" || stored.Attempts != 1 {
		t.Fatalf("stored = %+v", stored)
	}
	wantNoWork(t, b, "w2", now)
}

func claimEmpty(t *testing.T, b work.Backend) { wantNoWork(t, b, "w1", t0) }

func claimDelayed(t *testing.T, b work.Backend) {
	due := t0.Add(10 * time.Minute)
	r := enqueue(t, b, func(r *work.Record) { r.AvailableAt = due })
	wantNoWork(t, b, "w1", t0)
	wantNoWork(t, b, "w1", due.Add(-time.Second))
	if got := claimOne(t, b, "w1", due); got.ID != r.ID {
		t.Fatalf("claimed %s, want %s", got.ID, r.ID)
	}
}

func claimQueues(t *testing.T, b work.Backend) {
	a := enqueue(t, b, func(r *work.Record) { r.Queue = "a" })
	c := enqueue(t, b, func(r *work.Record) { r.Queue = "c" })
	recs, err := b.Claim(ctx, claimOpts("w1", t0, 10, "a"))
	if err != nil || len(recs) != 1 || recs[0].ID != a.ID {
		t.Fatalf("claim a = %v, %v", recs, err)
	}
	if _, err := b.Claim(ctx, claimOpts("w1", t0, 10, "a", "other")); !errors.Is(err, work.ErrNoWork) {
		t.Fatalf("a is exhausted, got %v", err)
	}
	recs, err = b.Claim(ctx, claimOpts("w1", t0, 10, "a", "c"))
	if err != nil || len(recs) != 1 || recs[0].ID != c.ID {
		t.Fatalf("claim a,c = %v, %v", recs, err)
	}
}

func claimTypes(t *testing.T, b work.Backend) {
	known := enqueue(t, b, func(r *work.Record) { r.Type = "known" })
	enqueue(t, b, func(r *work.Record) { r.Type = "unknown" })
	o := claimOpts("w1", t0, 10)
	o.Types = []string{"known"}
	recs, err := b.Claim(ctx, o)
	if err != nil || len(recs) != 1 || recs[0].ID != known.ID {
		t.Fatalf("claim = %v, %v", recs, err)
	}
	if _, err := b.Claim(ctx, o); !errors.Is(err, work.ErrNoWork) {
		t.Fatalf("the unknown type must stay unclaimed, got %v", err)
	}
	if mustGet(t, b, known.ID).Status != work.StatusRunning {
		t.Fatal("known should be running")
	}
}

func claimOrdering(t *testing.T, b work.Backend) {
	low := enqueue(t, b, func(r *work.Record) { r.Priority = 0 })
	lateHigh := enqueue(t, b, func(r *work.Record) { r.Priority = 10; r.AvailableAt = t0.Add(time.Second) })
	earlyHigh := enqueue(t, b, func(r *work.Record) { r.Priority = 10; r.AvailableAt = t0 })
	sameA := enqueue(t, b, func(r *work.Record) { r.Priority = 10; r.AvailableAt = t0 }) // created after earlyHigh
	negative := enqueue(t, b, func(r *work.Record) { r.Priority = -5 })

	recs, err := b.Claim(ctx, claimOpts("w1", t0.Add(time.Minute), 10))
	if err != nil {
		t.Fatal(err)
	}
	var got []work.ID
	for _, r := range recs {
		got = append(got, r.ID)
	}
	want := []work.ID{earlyHigh.ID, sameA.ID, lateHigh.ID, low.ID, negative.ID}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("order = %v\nwant    %v", got, want)
	}
}

func claimBatch(t *testing.T, b work.Backend) {
	for i := 0; i < 5; i++ {
		enqueue(t, b)
	}
	first, err := b.Claim(ctx, claimOpts("w1", t0, 3))
	if err != nil || len(first) != 3 {
		t.Fatalf("first = %d, %v", len(first), err)
	}
	second, err := b.Claim(ctx, claimOpts("w2", t0, 3))
	if err != nil || len(second) != 2 {
		t.Fatalf("second = %d, %v", len(second), err)
	}
	seen := map[work.ID]bool{}
	for _, r := range append(first, second...) {
		if seen[r.ID] {
			t.Fatalf("record %s claimed twice", r.ID)
		}
		seen[r.ID] = true
	}
}

func claimConcurrent(t *testing.T, b work.Backend) {
	const total, workers = 120, 8
	for i := 0; i < total; i++ {
		enqueue(t, b)
	}
	var mu sync.Mutex
	owner := map[work.ID]string{}
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			name := fmt.Sprintf("w%d", w)
			for {
				recs, err := b.Claim(ctx, claimOpts(name, t0, 3))
				if errors.Is(err, work.ErrNoWork) {
					return
				}
				if err != nil {
					t.Errorf("%s: claim: %v", name, err)
					return
				}
				mu.Lock()
				for _, r := range recs {
					if prev, dup := owner[r.ID]; dup {
						t.Errorf("record %s claimed by both %s and %s", r.ID, prev, name)
					}
					owner[r.ID] = name
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(owner) != total {
		t.Fatalf("claimed %d of %d records", len(owner), total)
	}
	for id, w := range owner {
		r := mustGet(t, b, id)
		if r.Status != work.StatusRunning || r.WorkerID != w || r.Attempts != 1 {
			t.Fatalf("record %s = %+v, want running on %s with 1 attempt", id, r, w)
		}
	}
}

func completeOwnership(t *testing.T, b work.Backend) {
	r := enqueue(t, b, func(r *work.Record) { r.UniqueKey, r.ActiveUniqueKey = "k", "k" })
	claimOne(t, b, "w1", t0)
	wantErr(t, b.Complete(ctx, r.ID, "w2", t0), work.ErrLeaseLost)
	if mustGet(t, b, r.ID).Status != work.StatusRunning {
		t.Fatal("a foreign worker must not complete the work")
	}
	done := t0.Add(3 * time.Second)
	if err := b.Complete(ctx, r.ID, "w1", done); err != nil {
		t.Fatal(err)
	}
	got := mustGet(t, b, r.ID)
	if got.Status != work.StatusCompleted || got.WorkerID != "" || !got.LeaseUntil.IsZero() || got.ActiveUniqueKey != "" {
		t.Fatalf("completed = %+v", got)
	}
	sameTime(t, "completed_at", got.CompletedAt, done)
	if got.UniqueKey != "k" {
		t.Fatal("the unique key history must be retained")
	}
	wantErr(t, b.Complete(ctx, r.ID, "w1", done), work.ErrAlreadyCompleted)
	wantErr(t, b.Complete(ctx, work.ID(uuid.NewString()), "w1", done), work.ErrNotFound)
}

func completeUnclaimed(t *testing.T, b work.Backend) {
	r := enqueue(t, b)
	wantErr(t, b.Complete(ctx, r.ID, "w1", t0), work.ErrLeaseLost)
	if mustGet(t, b, r.ID).Status != work.StatusAvailable {
		t.Fatal("unclaimed work must stay available")
	}
}

func retryReschedules(t *testing.T, b work.Backend) {
	r := enqueue(t, b)
	claimOne(t, b, "w1", t0)
	next := t0.Add(30 * time.Second)
	now := t0.Add(2 * time.Second)
	if err := b.Retry(ctx, r.ID, "w1", next, errors.New("boom"), now); err != nil {
		t.Fatal(err)
	}
	got := mustGet(t, b, r.ID)
	if got.Status != work.StatusAvailable || got.Attempts != 1 || got.WorkerID != "" || !got.LeaseUntil.IsZero() {
		t.Fatalf("after retry = %+v", got)
	}
	sameTime(t, "available_at", got.AvailableAt, next)
	sameTime(t, "last_error_at", got.LastErrorAt, now)
	if got.LastError != "boom" {
		t.Fatalf("last_error = %q", got.LastError)
	}
	wantNoWork(t, b, "w2", next.Add(-time.Second))
	again := claimOne(t, b, "w2", next)
	if again.Attempts != 2 || again.WorkerID != "w2" || again.LastError != "boom" {
		t.Fatalf("second claim = %+v", again)
	}
}

func retryOwnership(t *testing.T, b work.Backend) {
	r := enqueue(t, b)
	claimOne(t, b, "w1", t0)
	wantErr(t, b.Retry(ctx, r.ID, "intruder", t0, errors.New("x"), t0), work.ErrLeaseLost)
	wantErr(t, b.Fail(ctx, r.ID, "intruder", errors.New("x"), t0), work.ErrLeaseLost)
	wantErr(t, b.Release(ctx, r.ID, "intruder", t0), work.ErrLeaseLost)
	wantErr(t, b.FinishCancel(ctx, r.ID, "intruder", nil, t0), work.ErrLeaseLost)
	_, err := b.ExtendLease(ctx, r.ID, "intruder", t0.Add(time.Hour), t0)
	wantErr(t, err, work.ErrLeaseLost)
	if got := mustGet(t, b, r.ID); got.Status != work.StatusRunning || got.WorkerID != "w1" {
		t.Fatalf("ownership changed: %+v", got)
	}
}

func failedNotClaimable(t *testing.T, b work.Backend) {
	r := enqueue(t, b, func(r *work.Record) { r.UniqueKey, r.ActiveUniqueKey = "k", "k" })
	claimOne(t, b, "w1", t0)
	if err := b.Fail(ctx, r.ID, "w1", errors.New("permanent"), t0); err != nil {
		t.Fatal(err)
	}
	got := mustGet(t, b, r.ID)
	if got.Status != work.StatusFailed || got.LastError != "permanent" || got.ActiveUniqueKey != "" || got.WorkerID != "" {
		t.Fatalf("failed = %+v", got)
	}
	wantNoWork(t, b, "w2", t0.Add(24*time.Hour))
}

func cancelledNotClaimable(t *testing.T, b work.Backend) {
	r := enqueue(t, b, func(r *work.Record) { r.UniqueKey, r.ActiveUniqueKey = "k", "k" })
	out, err := b.Cancel(ctx, r.ID, t0)
	if err != nil || out != work.Cancelled {
		t.Fatalf("cancel = %v, %v", out, err)
	}
	got := mustGet(t, b, r.ID)
	if got.Status != work.StatusCancelled || got.ActiveUniqueKey != "" {
		t.Fatalf("cancelled = %+v", got)
	}
	wantNoWork(t, b, "w1", t0.Add(time.Hour))
}

func cancelOutcomes(t *testing.T, b work.Backend) {
	_, err := b.Cancel(ctx, work.ID(uuid.NewString()), t0)
	wantErr(t, err, work.ErrNotFound)

	done := enqueue(t, b)
	claimOne(t, b, "w1", t0)
	if err := b.Complete(ctx, done.ID, "w1", t0); err != nil {
		t.Fatal(err)
	}
	_, err = b.Cancel(ctx, done.ID, t0)
	wantErr(t, err, work.ErrAlreadyCompleted)

	failed := enqueue(t, b)
	claimOne(t, b, "w1", t0)
	if err := b.Fail(ctx, failed.ID, "w1", errors.New("x"), t0); err != nil {
		t.Fatal(err)
	}
	_, err = b.Cancel(ctx, failed.ID, t0)
	wantErr(t, err, work.ErrInvalidState)

	c := enqueue(t, b)
	for i := 0; i < 2; i++ {
		out, err := b.Cancel(ctx, c.ID, t0)
		if err != nil || out != work.Cancelled {
			t.Fatalf("cancel #%d = %v, %v", i, out, err)
		}
	}
}

func cancelRunning(t *testing.T, b work.Backend) {
	// 1. cancel while running only requests; the handler is still running.
	r := enqueue(t, b)
	claimOne(t, b, "w1", t0)
	out, err := b.Cancel(ctx, r.ID, t0.Add(time.Second))
	if err != nil || out != work.CancelRequested {
		t.Fatalf("cancel = %v, %v", out, err)
	}
	if got := mustGet(t, b, r.ID); got.Status != work.StatusRunning || got.CancelRequestedAt.IsZero() {
		t.Fatalf("running work must stay running until its worker settles it: %+v", got)
	}
	requested, err := b.ExtendLease(ctx, r.ID, "w1", t0.Add(time.Hour), t0.Add(2*time.Second))
	if err != nil || !requested {
		t.Fatalf("extend lease = %v, %v; the worker must learn about the request", requested, err)
	}
	if err := b.FinishCancel(ctx, r.ID, "w1", errors.New("stopped"), t0.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	if got := mustGet(t, b, r.ID); got.Status != work.StatusCancelled || got.WorkerID != "" {
		t.Fatalf("after FinishCancel = %+v", got)
	}

	// 2. a handler that finished anyway completes the work.
	r2 := enqueue(t, b)
	claimOne(t, b, "w1", t0)
	if _, err := b.Cancel(ctx, r2.ID, t0); err != nil {
		t.Fatal(err)
	}
	if err := b.Complete(ctx, r2.ID, "w1", t0); err != nil {
		t.Fatal(err)
	}
	if mustGet(t, b, r2.ID).Status != work.StatusCompleted {
		t.Fatal("work that finished despite the request is completed")
	}

	// 3. a failing or released attempt after a cancel request is cancelled,
	// never retried.
	r3 := enqueue(t, b)
	claimOne(t, b, "w1", t0)
	if _, err := b.Cancel(ctx, r3.ID, t0); err != nil {
		t.Fatal(err)
	}
	if err := b.Retry(ctx, r3.ID, "w1", t0, errors.New("x"), t0); err != nil {
		t.Fatal(err)
	}
	if mustGet(t, b, r3.ID).Status != work.StatusCancelled {
		t.Fatal("a retry after a cancel request must cancel")
	}
	wantNoWork(t, b, "w2", t0.Add(time.Hour))
}

func expiredReclaim(t *testing.T, b work.Backend) {
	r := enqueue(t, b)
	claimOne(t, b, "w1", t0)
	almost := t0.Add(lease - time.Second)
	wantNoWork(t, b, "w2", almost)

	after := t0.Add(lease + time.Second)
	got := claimOne(t, b, "w2", after)
	if got.ID != r.ID || got.Attempts != 2 || got.WorkerID != "w2" || got.Status != work.StatusRunning {
		t.Fatalf("reclaimed = %+v", got)
	}
	sameTime(t, "lease_until", got.LeaseUntil, after.Add(lease))
	if got.LastError == "" {
		t.Fatal("reclaiming must leave a diagnostic in last_error")
	}
	// the crashed worker lost ownership
	wantErr(t, b.Complete(ctx, r.ID, "w1", after), work.ErrLeaseLost)
	if err := b.Complete(ctx, r.ID, "w2", after); err != nil {
		t.Fatal(err)
	}
}

func expiredReclaimRace(t *testing.T, b work.Backend) {
	const workers = 8
	r := enqueue(t, b)
	claimOne(t, b, "crashed", t0)
	after := t0.Add(lease + time.Second)
	var winners atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			recs, err := b.Claim(ctx, claimOpts(fmt.Sprintf("w%d", i), after, 1))
			if err == nil && len(recs) == 1 {
				winners.Add(1)
			} else if !errors.Is(err, work.ErrNoWork) {
				t.Errorf("claim: %v", err)
			}
		}()
	}
	wg.Wait()
	if winners.Load() != 1 {
		t.Fatalf("%d workers reclaimed the expired lease, want exactly 1", winners.Load())
	}
	if got := mustGet(t, b, r.ID); got.Attempts != 2 {
		t.Fatalf("attempts = %d, want 2", got.Attempts)
	}
}

func expiredExhausted(t *testing.T, b work.Backend) {
	r := enqueue(t, b, func(r *work.Record) { r.MaxAttempts = 1; r.UniqueKey, r.ActiveUniqueKey = "k", "k" })
	claimOne(t, b, "w1", t0)
	after := t0.Add(lease + time.Second)
	wantNoWork(t, b, "w2", after) // not reclaimed: no attempt left

	reaped, err := b.Reap(ctx, t0.Add(time.Second), 10)
	if err != nil || len(reaped) != 0 {
		t.Fatalf("reaping before expiry = %v, %v", reaped, err)
	}
	reaped, err = b.Reap(ctx, after, 10)
	if err != nil || len(reaped) != 1 || reaped[0].ID != r.ID {
		t.Fatalf("reap = %v, %v", reaped, err)
	}
	got := mustGet(t, b, r.ID)
	if got.Status != work.StatusFailed || got.LastError == "" || got.ActiveUniqueKey != "" || got.WorkerID != "" {
		t.Fatalf("reaped = %+v", got)
	}
	again, err := b.Reap(ctx, after, 10)
	if err != nil || len(again) != 0 {
		t.Fatalf("reaping twice = %v, %v", again, err)
	}
}

func expiredCancelled(t *testing.T, b work.Backend) {
	r := enqueue(t, b)
	claimOne(t, b, "w1", t0)
	if _, err := b.Cancel(ctx, r.ID, t0); err != nil {
		t.Fatal(err)
	}
	after := t0.Add(lease + time.Second)
	wantNoWork(t, b, "w2", after) // a cancelled run is never re-run
	reaped, err := b.Reap(ctx, after, 10)
	if err != nil || len(reaped) != 1 {
		t.Fatalf("reap = %v, %v", reaped, err)
	}
	if got := mustGet(t, b, r.ID); got.Status != work.StatusCancelled {
		t.Fatalf("status = %s, want cancelled", got.Status)
	}
}

func extendLease(t *testing.T, b work.Backend) {
	r := enqueue(t, b)
	claimOne(t, b, "w1", t0)
	beat := t0.Add(time.Minute)
	requested, err := b.ExtendLease(ctx, r.ID, "w1", beat.Add(lease), beat)
	if err != nil || requested {
		t.Fatalf("extend = %v, %v", requested, err)
	}
	sameTime(t, "lease_until", mustGet(t, b, r.ID).LeaseUntil, beat.Add(lease))
	// the original lease would have expired here, the extended one has not
	wantNoWork(t, b, "w2", t0.Add(lease+time.Second))
	got := claimOne(t, b, "w2", beat.Add(lease+time.Second))
	if got.Attempts != 2 {
		t.Fatalf("attempts = %d", got.Attempts)
	}
	_, err = b.ExtendLease(ctx, r.ID, "w1", beat.Add(time.Hour), beat)
	wantErr(t, err, work.ErrLeaseLost)
	_, err = b.ExtendLease(ctx, work.ID(uuid.NewString()), "w1", beat, beat)
	wantErr(t, err, work.ErrNotFound)
}

func releaseWork(t *testing.T, b work.Backend) {
	r := enqueue(t, b)
	claimOne(t, b, "w1", t0)
	at := t0.Add(5 * time.Second)
	if err := b.Release(ctx, r.ID, "w1", at); err != nil {
		t.Fatal(err)
	}
	got := mustGet(t, b, r.ID)
	if got.Status != work.StatusAvailable || got.Attempts != 0 || got.WorkerID != "" || !got.LeaseUntil.IsZero() {
		t.Fatalf("released = %+v", got)
	}
	sameTime(t, "available_at", got.AvailableAt, at)
	if again := claimOne(t, b, "w2", at); again.Attempts != 1 {
		t.Fatalf("attempts = %d, want 1: a release must not consume an attempt", again.Attempts)
	}
}

func skipUnlessUnique(t *testing.T, b work.Backend) {
	if !b.Capabilities().StrongUniqueness {
		t.Skip("backend does not claim strong uniqueness")
	}
}

func withKey(key string) func(*work.Record) {
	return func(r *work.Record) { r.UniqueKey, r.ActiveUniqueKey = key, key }
}

func uniqueActive(t *testing.T, b work.Backend) {
	skipUnlessUnique(t, b)
	first := enqueue(t, b, withKey("invoice:1"))
	dup := mk(withKey("invoice:1"))
	err := b.Enqueue(ctx, dup)
	wantErr(t, err, work.ErrDuplicate)
	var de *work.DuplicateError
	if !errors.As(err, &de) || de.ExistingID != first.ID || de.UniqueKey != "invoice:1" || de.Queue != "default" {
		t.Fatalf("duplicate error = %#v", err)
	}
	if _, err := b.Get(ctx, dup.ID); !errors.Is(err, work.ErrNotFound) {
		t.Fatalf("the duplicate must not be stored, got %v", err)
	}

	// running work is still active
	claimOne(t, b, "w1", t0)
	wantErr(t, b.Enqueue(ctx, mk(withKey("invoice:1"))), work.ErrDuplicate)

	// a retry returns it to available, still active
	if err := b.Retry(ctx, first.ID, "w1", t0, errors.New("x"), t0); err != nil {
		t.Fatal(err)
	}
	wantErr(t, b.Enqueue(ctx, mk(withKey("invoice:1"))), work.ErrDuplicate)

	// completed work no longer blocks: history never prevents future dispatch
	claimOne(t, b, "w1", t0)
	if err := b.Complete(ctx, first.ID, "w1", t0); err != nil {
		t.Fatal(err)
	}
	second := enqueue(t, b, withKey("invoice:1"))

	// failed and cancelled work do not block either
	claimOne(t, b, "w1", t0)
	if err := b.Fail(ctx, second.ID, "w1", errors.New("x"), t0); err != nil {
		t.Fatal(err)
	}
	third := enqueue(t, b, withKey("invoice:1"))
	if _, err := b.Cancel(ctx, third.ID, t0); err != nil {
		t.Fatal(err)
	}
	enqueue(t, b, withKey("invoice:1"))
}

func uniquePerQueue(t *testing.T, b work.Backend) {
	skipUnlessUnique(t, b)
	enqueue(t, b, withKey("k"), func(r *work.Record) { r.Queue = "a" })
	enqueue(t, b, withKey("k"), func(r *work.Record) { r.Queue = "b" })
	wantErr(t, b.Enqueue(ctx, mk(withKey("k"), func(r *work.Record) { r.Queue = "a" })), work.ErrDuplicate)
	enqueue(t, b, withKey("other"), func(r *work.Record) { r.Queue = "a" })
}

func uniqueNone(t *testing.T, b work.Backend) {
	for i := 0; i < 5; i++ {
		enqueue(t, b)
	}
}

func requeue(t *testing.T, b work.Backend) {
	r := enqueue(t, b, func(r *work.Record) { r.MaxAttempts = 1 })
	claimOne(t, b, "w1", t0)
	if err := b.Fail(ctx, r.ID, "w1", errors.New("first failure"), t0); err != nil {
		t.Fatal(err)
	}
	now := t0.Add(time.Hour)
	if err := b.Requeue(ctx, r.ID, now); err != nil {
		t.Fatal(err)
	}
	got := mustGet(t, b, r.ID)
	if got.Status != work.StatusAvailable || got.Attempts != 0 || !got.CompletedAt.IsZero() {
		t.Fatalf("requeued = %+v", got)
	}
	sameTime(t, "available_at", got.AvailableAt, now)
	if got.LastError != "first failure" {
		t.Fatalf("history must be preserved, last_error = %q", got.LastError)
	}
	if again := claimOne(t, b, "w2", now); again.ID != r.ID || again.Attempts != 1 {
		t.Fatalf("claim after requeue = %+v", again)
	}

	// only failed and cancelled work can be requeued
	wantErr(t, b.Requeue(ctx, r.ID, now), work.ErrInvalidState) // running
	if err := b.Complete(ctx, r.ID, "w2", now); err != nil {
		t.Fatal(err)
	}
	wantErr(t, b.Requeue(ctx, r.ID, now), work.ErrInvalidState) // completed
	wantErr(t, b.Requeue(ctx, work.ID(uuid.NewString()), now), work.ErrNotFound)

	// cancelled work can be requeued
	c := enqueue(t, b)
	if _, err := b.Cancel(ctx, c.ID, now); err != nil {
		t.Fatal(err)
	}
	if err := b.Requeue(ctx, c.ID, now); err != nil {
		t.Fatal(err)
	}
	if mustGet(t, b, c.ID).Status != work.StatusAvailable {
		t.Fatal("cancelled work should be requeueable")
	}
}

func listFilters(t *testing.T, b work.Backend) {
	a1 := enqueue(t, b, func(r *work.Record) { r.Queue, r.Type = "a", "x" })
	a2 := enqueue(t, b, func(r *work.Record) { r.Queue, r.Type = "a", "y" })
	b1 := enqueue(t, b, func(r *work.Record) { r.Queue, r.Type = "b", "x" })
	if recs, err := b.Claim(ctx, claimOpts("w1", t0, 1, "a")); err != nil || len(recs) != 1 || recs[0].ID != a1.ID {
		t.Fatalf("claim = %v, %v", recs, err) // a1 becomes running (oldest in a)
	}

	ids := func(f work.ListFilter) []work.ID {
		recs, err := b.List(ctx, f)
		if err != nil {
			t.Fatal(err)
		}
		out := make([]work.ID, len(recs))
		for i, r := range recs {
			out[i] = r.ID
		}
		return out
	}
	eq := func(name string, got, want []work.ID) {
		t.Helper()
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("%s = %v, want %v", name, got, want)
		}
	}
	eq("all newest first", ids(work.ListFilter{}), []work.ID{b1.ID, a2.ID, a1.ID})
	eq("oldest first", ids(work.ListFilter{OldestFirst: true}), []work.ID{a1.ID, a2.ID, b1.ID})
	eq("queue", ids(work.ListFilter{Queue: "a"}), []work.ID{a2.ID, a1.ID})
	eq("type", ids(work.ListFilter{Type: "x"}), []work.ID{b1.ID, a1.ID})
	eq("status", ids(work.ListFilter{Statuses: []work.Status{work.StatusRunning}}), []work.ID{a1.ID})
	eq("statuses", ids(work.ListFilter{Statuses: []work.Status{work.StatusRunning, work.StatusAvailable}}), []work.ID{b1.ID, a2.ID, a1.ID})
	eq("limit", ids(work.ListFilter{Limit: 2}), []work.ID{b1.ID, a2.ID})
	eq("offset", ids(work.ListFilter{Limit: 2, Offset: 1}), []work.ID{a2.ID, a1.ID})
	eq("created after", ids(work.ListFilter{CreatedAfter: a1.CreatedAt}), []work.ID{b1.ID, a2.ID})
	eq("created before", ids(work.ListFilter{CreatedBefore: b1.CreatedAt}), []work.ID{a2.ID, a1.ID})
	eq("combined", ids(work.ListFilter{Queue: "a", Type: "x"}), []work.ID{a1.ID})
	eq("no match", ids(work.ListFilter{Queue: "nope"}), nil)
}

func stats(t *testing.T, b work.Backend) {
	enqueue(t, b, func(r *work.Record) { r.Queue = "a" })
	enqueue(t, b, func(r *work.Record) { r.Queue = "a" })
	enqueue(t, b, func(r *work.Record) { r.Queue = "b" })
	if _, err := b.Claim(ctx, claimOpts("w1", t0, 1, "a")); err != nil {
		t.Fatal(err)
	}
	got, err := b.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	m := map[string]int64{}
	for _, s := range got {
		m[s.Queue+"/"+string(s.Status)] = s.Count
	}
	want := map[string]int64{"a/available": 1, "a/running": 1, "b/available": 1}
	keys := func(m map[string]int64) string {
		var ks []string
		for k, v := range m {
			ks = append(ks, fmt.Sprintf("%s=%d", k, v))
		}
		sort.Strings(ks)
		return fmt.Sprint(ks)
	}
	if keys(m) != keys(want) {
		t.Fatalf("stats = %s, want %s", keys(m), keys(want))
	}
}

func retained(t *testing.T, b work.Backend) {
	r := enqueue(t, b)
	claimOne(t, b, "w1", t0)
	if err := b.Complete(ctx, r.ID, "w1", t0); err != nil {
		t.Fatal(err)
	}
	got := mustGet(t, b, r.ID)
	if got.Status != work.StatusCompleted || got.Type != r.Type {
		t.Fatalf("completed work must remain inspectable: %+v", got)
	}
	sameJSON(t, got.Payload, r.Payload)
}
