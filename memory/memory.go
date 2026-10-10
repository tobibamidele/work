// Package memory is an in-memory work.Backend.
//
// It implements the full portable semantics (leases, ownership, priorities,
// uniqueness, cooperative cancellation) and passes the same conformance suite
// as the database backend, which makes it the reference implementation and
// the right backend for tests and examples. It is not durable and not shared
// between processes, and it cannot take part in application transactions.
package memory

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/tobibamidele/work"
)

// Backend is an in-memory work.Backend. The zero value is not usable; call New.
type Backend struct {
	mu   sync.Mutex
	recs map[work.ID]*work.Record
}

var _ work.Backend = (*Backend)(nil)

// New returns an empty backend.
func New() *Backend { return &Backend{recs: map[work.ID]*work.Record{}} }

// Capabilities implements work.Backend.
func (b *Backend) Capabilities() work.Capabilities {
	return work.Capabilities{
		Name: "memory", DelayedWork: true, Cancellation: true, Priorities: true,
		StrongUniqueness: true, Transactions: false,
	}
}

func notFound(id work.ID) error { return fmt.Errorf("%w: %s", work.ErrNotFound, id) }

func invalidState(rec *work.Record) error {
	return fmt.Errorf("%w: work %s is %s", work.ErrInvalidState, rec.ID, rec.Status)
}

// activeHolder returns the active record holding key in queue, other than
// except.
func (b *Backend) activeHolder(queue, key string, except work.ID) *work.Record {
	for _, r := range b.recs {
		if r.ID != except && r.Queue == queue && r.ActiveUniqueKey == key && r.Status.Active() {
			return r
		}
	}
	return nil
}

// Enqueue implements work.Backend.
func (b *Backend) Enqueue(_ context.Context, rec *work.Record) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, dup := b.recs[rec.ID]; dup {
		return fmt.Errorf("memory: work %s already exists", rec.ID)
	}
	if rec.ActiveUniqueKey != "" {
		if h := b.activeHolder(rec.Queue, rec.ActiveUniqueKey, ""); h != nil {
			return &work.DuplicateError{Queue: rec.Queue, UniqueKey: rec.ActiveUniqueKey, ExistingID: h.ID}
		}
	}
	b.recs[rec.ID] = rec.Clone()
	return nil
}

// Get implements work.Backend.
func (b *Backend) Get(_ context.Context, id work.ID) (*work.Record, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	r, ok := b.recs[id]
	if !ok {
		return nil, notFound(id)
	}
	return r.Clone(), nil
}

// List implements work.Backend.
func (b *Backend) List(_ context.Context, f work.ListFilter) ([]*work.Record, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []*work.Record
	for _, r := range b.recs {
		if len(f.Statuses) > 0 && !containsStatus(f.Statuses, r.Status) {
			continue
		}
		if f.Queue != "" && r.Queue != f.Queue {
			continue
		}
		if f.Type != "" && r.Type != f.Type {
			continue
		}
		if !f.CreatedAfter.IsZero() && !r.CreatedAt.After(f.CreatedAfter) {
			continue
		}
		if !f.CreatedBefore.IsZero() && !r.CreatedAt.Before(f.CreatedBefore) {
			continue
		}
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			if f.OldestFirst {
				return out[i].CreatedAt.Before(out[j].CreatedAt)
			}
			return out[i].CreatedAt.After(out[j].CreatedAt)
		}
		if f.OldestFirst {
			return out[i].ID < out[j].ID
		}
		return out[i].ID > out[j].ID
	})
	if f.Offset > 0 {
		if f.Offset >= len(out) {
			return nil, nil
		}
		out = out[f.Offset:]
	}
	if f.Limit > 0 && len(out) > f.Limit {
		out = out[:f.Limit]
	}
	res := make([]*work.Record, len(out))
	for i, r := range out {
		res[i] = r.Clone()
	}
	return res, nil
}

func containsStatus(set []work.Status, s work.Status) bool {
	for _, v := range set {
		if v == s {
			return true
		}
	}
	return false
}

// Stats implements work.Backend.
func (b *Backend) Stats(_ context.Context) ([]work.QueueStat, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	type k struct {
		q string
		s work.Status
	}
	counts := map[k]int64{}
	for _, r := range b.recs {
		counts[k{r.Queue, r.Status}]++
	}
	out := make([]work.QueueStat, 0, len(counts))
	for key, n := range counts {
		out = append(out, work.QueueStat{Queue: key.q, Status: key.s, Count: n})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Queue != out[j].Queue {
			return out[i].Queue < out[j].Queue
		}
		return out[i].Status < out[j].Status
	})
	return out, nil
}

// Claim implements work.Backend.
func (b *Backend) Claim(_ context.Context, o work.ClaimOptions) ([]*work.Record, error) {
	if o.Limit < 1 {
		o.Limit = 1
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	var cands []*work.Record
	for _, r := range b.recs {
		if !contains(o.Queues, r.Queue) || (len(o.Types) > 0 && !contains(o.Types, r.Type)) {
			continue
		}
		switch r.Status {
		case work.StatusAvailable:
			if r.AvailableAt.After(o.Now) {
				continue
			}
		case work.StatusRunning:
			if !r.LeaseUntil.Before(o.Now) || r.Attempts >= r.MaxAttempts || !r.CancelRequestedAt.IsZero() {
				continue
			}
		default:
			continue
		}
		cands = append(cands, r)
	}
	if len(cands) == 0 {
		return nil, work.ErrNoWork
	}
	sort.Slice(cands, func(i, j int) bool {
		a, c := cands[i], cands[j]
		switch {
		case a.Priority != c.Priority:
			return a.Priority > c.Priority
		case !a.AvailableAt.Equal(c.AvailableAt):
			return a.AvailableAt.Before(c.AvailableAt)
		case !a.CreatedAt.Equal(c.CreatedAt):
			return a.CreatedAt.Before(c.CreatedAt)
		}
		return a.ID < c.ID
	})
	if len(cands) > o.Limit {
		cands = cands[:o.Limit]
	}
	out := make([]*work.Record, len(cands))
	for i, r := range cands {
		if r.Status == work.StatusRunning {
			r.LastError = "lease expired; reclaimed by another worker"
			r.LastErrorAt = o.Now
		}
		r.Status = work.StatusRunning
		r.Attempts++
		r.WorkerID = o.WorkerID
		r.LockedAt = o.Now
		r.LeaseUntil = o.Now.Add(o.LeaseDuration)
		r.StartedAt = o.Now
		r.UpdatedAt = o.Now
		out[i] = r.Clone()
	}
	return out, nil
}

func contains(set []string, v string) bool {
	for _, s := range set {
		if s == v {
			return true
		}
	}
	return false
}

// owned returns the running record leased to workerID or the precise error.
func (b *Backend) owned(id work.ID, workerID string) (*work.Record, error) {
	r, ok := b.recs[id]
	if !ok {
		return nil, notFound(id)
	}
	switch {
	case r.Status == work.StatusRunning && r.WorkerID == workerID:
		return r, nil
	case r.Status == work.StatusCompleted:
		return nil, fmt.Errorf("%w: %s", work.ErrAlreadyCompleted, id)
	case r.Status == work.StatusCancelled:
		return nil, fmt.Errorf("%w: %s", work.ErrCancelled, id)
	}
	return nil, fmt.Errorf("%w: %s is %s and not leased to %s", work.ErrLeaseLost, id, r.Status, workerID)
}

func clearLease(r *work.Record) {
	r.WorkerID = ""
	r.LockedAt = time.Time{}
	r.LeaseUntil = time.Time{}
}

// Complete implements work.Backend.
func (b *Backend) Complete(_ context.Context, id work.ID, workerID string, now time.Time) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	r, err := b.owned(id, workerID)
	if err != nil {
		return err
	}
	r.Status = work.StatusCompleted
	r.CompletedAt = now
	r.UpdatedAt = now
	r.ActiveUniqueKey = ""
	r.CancelRequestedAt = time.Time{}
	clearLease(r)
	return nil
}

// Retry implements work.Backend.
func (b *Backend) Retry(_ context.Context, id work.ID, workerID string, next time.Time, cause error, now time.Time) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	r, err := b.owned(id, workerID)
	if err != nil {
		return err
	}
	r.LastError, r.LastErrorAt, r.UpdatedAt = work.ErrorText(cause), now, now
	b.backToAvailable(r, next)
	return nil
}

// backToAvailable returns r to the queue, or cancels it when cancellation was
// requested while it ran.
func (b *Backend) backToAvailable(r *work.Record, at time.Time) {
	clearLease(r)
	if !r.CancelRequestedAt.IsZero() {
		r.Status = work.StatusCancelled
		r.ActiveUniqueKey = ""
		return
	}
	r.Status = work.StatusAvailable
	r.AvailableAt = at
}

// Fail implements work.Backend.
func (b *Backend) Fail(_ context.Context, id work.ID, workerID string, cause error, now time.Time) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	r, err := b.owned(id, workerID)
	if err != nil {
		return err
	}
	r.Status = work.StatusFailed
	r.LastError, r.LastErrorAt, r.UpdatedAt = work.ErrorText(cause), now, now
	r.ActiveUniqueKey = ""
	r.CancelRequestedAt = time.Time{}
	clearLease(r)
	return nil
}

// Release implements work.Backend.
func (b *Backend) Release(_ context.Context, id work.ID, workerID string, now time.Time) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	r, err := b.owned(id, workerID)
	if err != nil {
		return err
	}
	if r.Attempts > 0 {
		r.Attempts--
	}
	r.UpdatedAt = now
	b.backToAvailable(r, now)
	return nil
}

// FinishCancel implements work.Backend.
func (b *Backend) FinishCancel(_ context.Context, id work.ID, workerID string, cause error, now time.Time) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	r, err := b.owned(id, workerID)
	if err != nil {
		return err
	}
	r.Status = work.StatusCancelled
	if cause != nil {
		r.LastError, r.LastErrorAt = work.ErrorText(cause), now
	}
	r.UpdatedAt = now
	r.ActiveUniqueKey = ""
	clearLease(r)
	return nil
}

// ExtendLease implements work.Backend.
func (b *Backend) ExtendLease(_ context.Context, id work.ID, workerID string, until, now time.Time) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	r, err := b.owned(id, workerID)
	if err != nil {
		return false, err
	}
	r.LeaseUntil = until
	r.UpdatedAt = now
	return !r.CancelRequestedAt.IsZero(), nil
}

// Cancel implements work.Backend.
func (b *Backend) Cancel(_ context.Context, id work.ID, now time.Time) (work.CancelOutcome, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	r, ok := b.recs[id]
	if !ok {
		return 0, notFound(id)
	}
	switch r.Status {
	case work.StatusAvailable:
		r.Status = work.StatusCancelled
		r.ActiveUniqueKey = ""
		r.UpdatedAt = now
		return work.Cancelled, nil
	case work.StatusRunning:
		if r.CancelRequestedAt.IsZero() {
			r.CancelRequestedAt = now
			r.UpdatedAt = now
		}
		return work.CancelRequested, nil
	case work.StatusCancelled:
		return work.Cancelled, nil
	case work.StatusCompleted:
		return 0, fmt.Errorf("%w: %s", work.ErrAlreadyCompleted, id)
	}
	return 0, invalidState(r)
}

// Requeue implements work.Backend.
func (b *Backend) Requeue(_ context.Context, id work.ID, now time.Time) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	r, ok := b.recs[id]
	if !ok {
		return notFound(id)
	}
	if r.Status != work.StatusFailed && r.Status != work.StatusCancelled {
		return invalidState(r)
	}
	if r.UniqueKey != "" {
		if h := b.activeHolder(r.Queue, r.UniqueKey, r.ID); h != nil {
			return &work.DuplicateError{Queue: r.Queue, UniqueKey: r.UniqueKey, ExistingID: h.ID}
		}
		r.ActiveUniqueKey = r.UniqueKey
	}
	r.Status = work.StatusAvailable
	r.Attempts = 0
	r.AvailableAt = now
	r.UpdatedAt = now
	r.CompletedAt = time.Time{}
	r.CancelRequestedAt = time.Time{}
	clearLease(r)
	return nil
}

// Reap implements work.Backend.
func (b *Backend) Reap(_ context.Context, now time.Time, limit int) ([]*work.Record, error) {
	if limit < 1 {
		limit = 100
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	var due []*work.Record
	for _, r := range b.recs {
		if r.Status == work.StatusRunning && r.LeaseUntil.Before(now) &&
			(r.Attempts >= r.MaxAttempts || !r.CancelRequestedAt.IsZero()) {
			due = append(due, r)
		}
	}
	sort.Slice(due, func(i, j int) bool { return due[i].ID < due[j].ID })
	if len(due) > limit {
		due = due[:limit]
	}
	out := make([]*work.Record, len(due))
	for i, r := range due {
		if r.CancelRequestedAt.IsZero() {
			r.Status = work.StatusFailed
			r.LastError = "lease expired and no attempts remain"
		} else {
			r.Status = work.StatusCancelled
			r.LastError = "lease expired after cancellation was requested"
		}
		r.LastErrorAt, r.UpdatedAt = now, now
		r.ActiveUniqueKey = ""
		clearLease(r)
		out[i] = r.Clone()
	}
	return out, nil
}
