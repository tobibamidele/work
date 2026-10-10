package work

import (
	"context"
	"time"
)

// Capabilities describes what a backend can do. Features a backend cannot
// provide must be refused explicitly with ErrUnsupported; correctness is
// never silently downgraded.
type Capabilities struct {
	// Name identifies the backend, for diagnostics.
	Name string
	// DelayedWork: AvailableAt in the future is honored.
	DelayedWork bool
	// Cancellation: Cancel works on available and running work.
	Cancellation bool
	// Priorities: claim order honors Priority.
	Priorities bool
	// StrongUniqueness: unique keys are enforced atomically across processes.
	StrongUniqueness bool
	// Transactions: the backend can enqueue inside an application transaction.
	Transactions bool
}

// ClaimOptions selects work for Backend.Claim.
type ClaimOptions struct {
	// Queues restricts the claim. A worker only claims from queues it is
	// configured for.
	Queues []string
	// Types restricts the claim to work types the worker can execute, so a
	// worker never burns an attempt on work it has no handler for.
	Types []string
	// WorkerID becomes the lease owner.
	WorkerID string
	// Now is the claim time; leases and availability are evaluated against it.
	Now time.Time
	// LeaseDuration is how long the lease lasts from Now.
	LeaseDuration time.Duration
	// Limit is the maximum number of records to claim, at least 1.
	Limit int
}

// ListFilter selects records for inspection. Results are newest first unless
// OldestFirst is set.
type ListFilter struct {
	Statuses      []Status
	Queue         string
	Type          string
	CreatedAfter  time.Time
	CreatedBefore time.Time
	Limit         int
	Offset        int
	OldestFirst   bool
}

// QueueStat is the number of records of one status in one queue.
type QueueStat struct {
	Queue  string
	Status Status
	Count  int64
}

// CancelOutcome says what Cancel did.
type CancelOutcome int

const (
	// Cancelled means the work was available and is now cancelled.
	Cancelled CancelOutcome = iota + 1
	// CancelRequested means the work is running; its worker was asked to stop
	// and will settle the final state once the handler has returned.
	CancelRequested
)

func (o CancelOutcome) String() string {
	switch o {
	case Cancelled:
		return "cancelled"
	case CancelRequested:
		return "cancel_requested"
	}
	return "unknown"
}

// Backend is the durable storage and transport for work. It represents
// portable work semantics only. All times are supplied by the caller so that
// one Clock governs the whole system.
//
// Ownership is enforced wherever a worker acts on a record: Complete, Retry,
// Fail, Release, FinishCancel and ExtendLease succeed only while the record is
// running and leased to workerID, and otherwise return ErrLeaseLost (or
// ErrCancelled / ErrAlreadyCompleted / ErrNotFound when those describe the
// situation precisely).
//
// A backend must pass the conformance suite in package backendtest.
type Backend interface {
	// Capabilities reports what the backend supports.
	Capabilities() Capabilities

	// Enqueue durably stores rec. rec.ID, timestamps and status are already
	// set by the caller. It returns only once the record is durable. A unique
	// key collision returns a *DuplicateError.
	Enqueue(ctx context.Context, rec *Record) error

	// Get returns one record or ErrNotFound.
	Get(ctx context.Context, id ID) (*Record, error)

	// List returns records matching f.
	List(ctx context.Context, f ListFilter) ([]*Record, error)

	// Stats returns record counts per queue and status.
	Stats(ctx context.Context) ([]QueueStat, error)

	// Claim atomically leases up to o.Limit eligible records to o.WorkerID,
	// incrementing their attempts. Eligible: status available with
	// AvailableAt <= Now, or status running with an expired lease, attempts
	// remaining and no pending cancellation. Two callers never receive the
	// same record for the same lease. It returns ErrNoWork when nothing is
	// eligible.
	Claim(ctx context.Context, o ClaimOptions) ([]*Record, error)

	// Complete marks owned work completed.
	Complete(ctx context.Context, id ID, workerID string, now time.Time) error

	// Retry returns owned work to available at next, recording cause.
	Retry(ctx context.Context, id ID, workerID string, next time.Time, cause error, now time.Time) error

	// Fail marks owned work permanently failed, recording cause.
	Fail(ctx context.Context, id ID, workerID string, cause error, now time.Time) error

	// Release returns owned work to available immediately and gives back the
	// attempt that the claim consumed. It is used when a worker shuts down
	// with work in flight.
	Release(ctx context.Context, id ID, workerID string, now time.Time) error

	// FinishCancel settles owned work whose cancellation was requested.
	FinishCancel(ctx context.Context, id ID, workerID string, cause error, now time.Time) error

	// ExtendLease moves the lease of owned work to until. It reports whether
	// cancellation has been requested for the work.
	ExtendLease(ctx context.Context, id ID, workerID string, until time.Time, now time.Time) (cancelRequested bool, err error)

	// Cancel cancels available work immediately and requests cooperative
	// cancellation of running work. Cancelling cancelled work succeeds;
	// completed work returns ErrAlreadyCompleted; failed work returns
	// ErrInvalidState.
	Cancel(ctx context.Context, id ID, now time.Time) (CancelOutcome, error)

	// Requeue starts a new execution cycle for failed or cancelled work:
	// attempts reset to zero, AvailableAt = now. Other states return
	// ErrInvalidState. It may return a *DuplicateError if a unique key is now
	// held by other active work.
	Requeue(ctx context.Context, id ID, now time.Time) error

	// Reap settles running records whose lease expired and that cannot be
	// claimed again: those with no attempts left become failed, those with a
	// pending cancellation become cancelled. It returns the settled records.
	Reap(ctx context.Context, now time.Time, limit int) ([]*Record, error)
}
