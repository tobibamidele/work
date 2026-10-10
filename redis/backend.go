// Package redis is a work.Backend that stores work in Redis.
//
// Every state change is a Lua script, which Redis executes atomically, so the
// backend has the same semantics as the database backend (atomic claiming,
// leases and ownership, retries, cooperative cancellation, unique keys) and
// passes the same conformance suite. It lives in its own module so the core
// package never depends on a Redis client.
//
// What it is not: Redis cannot take part in your application's SQL
// transactions, so there is no transactional dispatch (Capabilities reports
// it, and DispatchTx-style APIs return work.ErrUnsupported). Durability is
// whatever your Redis is configured for: enable AOF (appendfsync everysec at
// least) or use a durable managed service, or an acknowledged enqueue can be
// lost in a Redis crash. If that matters, use the database backend.
//
// The client may be a single node, Sentinel or Cluster (any
// go-redis UniversalClient). All keys carry a hash tag ({prefix}), so one
// Backend lives in a single cluster slot: it scales with the work, not with
// the number of cluster nodes.
package redis

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/tobibamidele/work"
)

// Backend implements work.Backend on Redis.
type Backend struct {
	client goredis.UniversalClient
	prefix string // "{name}"
}

var _ work.Backend = (*Backend)(nil)

// Option configures New.
type Option func(*Backend) error

var prefixRE = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,100}$`)

// WithPrefix sets the key namespace. Default "work". Several independent
// systems can share one Redis with different prefixes.
func WithPrefix(name string) Option {
	return func(b *Backend) error {
		if !prefixRE.MatchString(name) {
			return fmt.Errorf("%w: invalid key prefix %q", work.ErrConfig, name)
		}
		b.prefix = "{" + name + "}"
		return nil
	}
}

// New returns a Backend using client. It verifies the connection and that the
// server is Redis 5 or newer.
func New(ctx context.Context, client goredis.UniversalClient, opts ...Option) (*Backend, error) {
	if client == nil {
		return nil, fmt.Errorf("%w: nil redis client", work.ErrConfig)
	}
	b := &Backend{client: client, prefix: "{work}"}
	for _, o := range opts {
		if err := o(b); err != nil {
			return nil, err
		}
	}
	info, err := client.Info(ctx, "server").Result()
	if err != nil {
		return nil, fmt.Errorf("work/redis: cannot reach Redis: %w", err)
	}
	for _, line := range strings.Split(info, "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), "redis_version:"); ok {
			if major, _ := strconv.Atoi(strings.SplitN(v, ".", 2)[0]); major < 5 {
				return nil, fmt.Errorf("%w: Redis %s is too old (needs 5.0+)", work.ErrUnsupported, v)
			}
			break
		}
	}
	return b, nil
}

// Capabilities implements work.Backend.
func (b *Backend) Capabilities() work.Capabilities {
	return work.Capabilities{
		Name: "redis", DelayedWork: true, Cancellation: true, Priorities: true,
		StrongUniqueness: true, Transactions: false,
	}
}

func (b *Backend) rk(id work.ID) string    { return b.prefix + ":rec|" + string(id) }
func (b *Backend) meta() string            { return b.prefix + ":meta" }
func (b *Backend) all() string             { return b.prefix + ":all" }
func (b *Backend) sk(s work.Status) string { return b.prefix + ":s|" + string(s) }

// run executes a script. KEYS carries one key in the prefix's slot so cluster
// clients route the call correctly; the script derives every other key from
// the prefix, which is safe because they share the hash tag.
func (b *Backend) run(ctx context.Context, s *goredis.Script, args ...any) ([]any, error) {
	res, err := s.Run(ctx, b.client, []string{b.meta()}, append([]any{b.prefix}, args...)...).Result()
	if err != nil {
		return nil, err
	}
	out, ok := res.([]any)
	if !ok {
		return nil, fmt.Errorf("work/redis: unexpected script reply %T", res)
	}
	return out, nil
}

// status checks a {"OK"} / {"ERR", code, ...} reply.
func status(reply []any, id work.ID, worker string) error {
	if len(reply) > 0 && reply[0] == "OK" {
		return nil
	}
	return scriptError(reply, id, worker)
}

func scriptError(reply []any, id work.ID, worker string) error {
	code := ""
	if len(reply) > 1 {
		code, _ = reply[1].(string)
	}
	switch code {
	case "NOTFOUND":
		return fmt.Errorf("%w: %s", work.ErrNotFound, id)
	case "COMPLETED":
		return fmt.Errorf("%w: %s", work.ErrAlreadyCompleted, id)
	case "CANCELLED":
		return fmt.Errorf("%w: %s", work.ErrCancelled, id)
	case "LOST":
		return fmt.Errorf("%w: %s is no longer running under %s", work.ErrLeaseLost, id, worker)
	case "INVALID":
		st := ""
		if len(reply) > 2 {
			st, _ = reply[2].(string)
		}
		return fmt.Errorf("%w: work %s is %s", work.ErrInvalidState, id, st)
	case "EXISTS":
		return fmt.Errorf("work/redis: work %s already exists", id)
	}
	return fmt.Errorf("work/redis: unexpected script reply %v", reply)
}

// Enqueue implements work.Backend.
func (b *Backend) Enqueue(ctx context.Context, rec *work.Record) error {
	fields, err := recordFields(rec)
	if err != nil {
		return err
	}
	reply, err := b.run(ctx, enqueueScript, fields...)
	if err != nil {
		return err
	}
	if len(reply) > 1 && reply[0] == "ERR" && reply[1] == "DUP" {
		existing, _ := reply[2].(string)
		return &work.DuplicateError{Queue: rec.Queue, UniqueKey: rec.ActiveUniqueKey, ExistingID: work.ID(existing)}
	}
	return status(reply, rec.ID, "")
}

// Get implements work.Backend.
func (b *Backend) Get(ctx context.Context, id work.ID) (*work.Record, error) {
	h, err := b.client.HGetAll(ctx, b.rk(id)).Result()
	if err != nil {
		return nil, err
	}
	if len(h) == 0 {
		return nil, fmt.Errorf("%w: %s", work.ErrNotFound, id)
	}
	return recordFromHash(h), nil
}

// List implements work.Backend. It walks the creation-time index and filters,
// which is right for inspection and CLI use but not meant for hot paths.
func (b *Backend) List(ctx context.Context, f work.ListFilter) ([]*work.Record, error) {
	index := b.all()
	if len(f.Statuses) == 1 {
		index = b.sk(f.Statuses[0])
	}
	min, max := "-inf", "+inf"
	if !f.CreatedAfter.IsZero() {
		min = "(" + i64(micros(f.CreatedAfter))
	}
	if !f.CreatedBefore.IsZero() {
		max = "(" + i64(micros(f.CreatedBefore))
	}
	statuses := map[work.Status]bool{}
	for _, s := range f.Statuses {
		statuses[s] = true
	}
	matches := func(r *work.Record) bool {
		return (len(statuses) == 0 || statuses[r.Status]) &&
			(f.Queue == "" || r.Queue == f.Queue) && (f.Type == "" || r.Type == f.Type)
	}

	const chunk = 200
	var out []*work.Record
	skip := f.Offset
	for offset := int64(0); ; offset += chunk {
		var ids []string
		var err error
		if f.OldestFirst {
			ids, err = b.client.ZRangeByScore(ctx, index, &goredis.ZRangeBy{Min: min, Max: max, Offset: offset, Count: chunk}).Result()
		} else {
			ids, err = b.client.ZRevRangeByScore(ctx, index, &goredis.ZRangeBy{Min: min, Max: max, Offset: offset, Count: chunk}).Result()
		}
		if err != nil {
			return nil, err
		}
		if len(ids) == 0 {
			return out, nil
		}
		cmds := make([]*goredis.MapStringStringCmd, len(ids))
		_, err = b.client.Pipelined(ctx, func(p goredis.Pipeliner) error {
			for i, id := range ids {
				cmds[i] = p.HGetAll(ctx, b.rk(work.ID(id)))
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		for _, c := range cmds {
			h, err := c.Result()
			if err != nil || len(h) == 0 {
				continue
			}
			rec := recordFromHash(h)
			if !matches(rec) {
				continue
			}
			if skip > 0 {
				skip--
				continue
			}
			out = append(out, rec)
			if f.Limit > 0 && len(out) >= f.Limit {
				return out, nil
			}
		}
		if len(ids) < chunk {
			return out, nil
		}
	}
}

// Stats implements work.Backend.
func (b *Backend) Stats(ctx context.Context) ([]work.QueueStat, error) {
	h, err := b.client.HGetAll(ctx, b.prefix+":stats").Result()
	if err != nil {
		return nil, err
	}
	var out []work.QueueStat
	for k, v := range h {
		n, _ := strconv.ParseInt(v, 10, 64)
		if n <= 0 {
			continue
		}
		i := strings.LastIndexByte(k, '|')
		if i < 0 {
			continue
		}
		out = append(out, work.QueueStat{Queue: k[:i], Status: work.Status(k[i+1:]), Count: n})
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
func (b *Backend) Claim(ctx context.Context, o work.ClaimOptions) ([]*work.Record, error) {
	switch {
	case len(o.Queues) == 0:
		return nil, fmt.Errorf("%w: Claim needs at least one queue", work.ErrConfig)
	case o.WorkerID == "":
		return nil, fmt.Errorf("%w: Claim needs a worker ID", work.ErrConfig)
	case o.LeaseDuration <= 0:
		return nil, fmt.Errorf("%w: Claim needs a positive lease duration", work.ErrConfig)
	}
	if o.Limit < 1 {
		o.Limit = 1
	}
	args := []any{i64(micros(o.Now)), i64(o.LeaseDuration.Microseconds()), o.WorkerID, strconv.Itoa(o.Limit), strconv.Itoa(len(o.Queues))}
	for _, q := range o.Queues {
		args = append(args, q)
	}
	args = append(args, strconv.Itoa(len(o.Types)))
	for _, t := range o.Types {
		args = append(args, t)
	}
	reply, err := b.run(ctx, claimScript, args...)
	if err != nil {
		return nil, err
	}
	return recordsFromReplies(reply, true)
}

func recordsFromReplies(reply []any, claim bool) ([]*work.Record, error) {
	if len(reply) == 0 {
		if claim {
			return nil, work.ErrNoWork
		}
		return nil, nil
	}
	out := make([]*work.Record, 0, len(reply))
	for _, r := range reply {
		rec, err := recordFromReply(r)
		if err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	if claim {
		// the script returns records in claim order
		return out, nil
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// Complete implements work.Backend.
func (b *Backend) Complete(ctx context.Context, id work.ID, workerID string, now time.Time) error {
	r, err := b.run(ctx, completeScript, string(id), workerID, i64(micros(now)))
	if err != nil {
		return err
	}
	return status(r, id, workerID)
}

// Retry implements work.Backend.
func (b *Backend) Retry(ctx context.Context, id work.ID, workerID string, next time.Time, cause error, now time.Time) error {
	r, err := b.run(ctx, retryScript, string(id), workerID, i64(micros(next)), i64(micros(now)), work.ErrorText(cause))
	if err != nil {
		return err
	}
	return status(r, id, workerID)
}

// Fail implements work.Backend.
func (b *Backend) Fail(ctx context.Context, id work.ID, workerID string, cause error, now time.Time) error {
	r, err := b.run(ctx, failScript, string(id), workerID, i64(micros(now)), work.ErrorText(cause))
	if err != nil {
		return err
	}
	return status(r, id, workerID)
}

// Release implements work.Backend.
func (b *Backend) Release(ctx context.Context, id work.ID, workerID string, now time.Time) error {
	r, err := b.run(ctx, releaseScript, string(id), workerID, i64(micros(now)))
	if err != nil {
		return err
	}
	return status(r, id, workerID)
}

// FinishCancel implements work.Backend.
func (b *Backend) FinishCancel(ctx context.Context, id work.ID, workerID string, cause error, now time.Time) error {
	has, text := "0", ""
	if cause != nil {
		has, text = "1", work.ErrorText(cause)
	}
	r, err := b.run(ctx, finishCancelScript, string(id), workerID, i64(micros(now)), has, text)
	if err != nil {
		return err
	}
	return status(r, id, workerID)
}

// ExtendLease implements work.Backend.
func (b *Backend) ExtendLease(ctx context.Context, id work.ID, workerID string, until time.Time, now time.Time) (bool, error) {
	r, err := b.run(ctx, extendScript, string(id), workerID, i64(micros(until)), i64(micros(now)))
	if err != nil {
		return false, err
	}
	if err := status(r, id, workerID); err != nil {
		return false, err
	}
	n, _ := r[1].(int64)
	return n == 1, nil
}

// Cancel implements work.Backend.
func (b *Backend) Cancel(ctx context.Context, id work.ID, now time.Time) (work.CancelOutcome, error) {
	r, err := b.run(ctx, cancelScript, string(id), i64(micros(now)))
	if err != nil {
		return 0, err
	}
	if err := status(r, id, ""); err != nil {
		return 0, err
	}
	if s, _ := r[1].(string); s == "REQUESTED" {
		return work.CancelRequested, nil
	}
	return work.Cancelled, nil
}

// Requeue implements work.Backend.
func (b *Backend) Requeue(ctx context.Context, id work.ID, now time.Time) error {
	r, err := b.run(ctx, requeueScript, string(id), i64(micros(now)))
	if err != nil {
		return err
	}
	if len(r) > 2 && r[0] == "ERR" && r[1] == "DUP" {
		rec, gerr := b.Get(ctx, id)
		if gerr != nil {
			return gerr
		}
		existing, _ := r[2].(string)
		return &work.DuplicateError{Queue: rec.Queue, UniqueKey: rec.UniqueKey, ExistingID: work.ID(existing)}
	}
	return status(r, id, "")
}

// Reap implements work.Backend.
func (b *Backend) Reap(ctx context.Context, now time.Time, limit int) ([]*work.Record, error) {
	if limit < 1 {
		limit = 100
	}
	reply, err := b.run(ctx, reapScript, i64(micros(now)), strconv.Itoa(limit))
	if err != nil {
		return nil, err
	}
	return recordsFromReplies(reply, false)
}

// Prune deletes finished work (completed, failed or cancelled, as selected)
// that was created before the given time, and returns how many records were
// removed. Redis holds every record in memory, so finished work should not be
// kept forever: run Prune periodically (for example daily, keeping a week).
// Active work is never touched. At most 5000 records are removed per call;
// call it again while the returned count equals 5000.
func (b *Backend) Prune(ctx context.Context, before time.Time, statuses ...work.Status) (int, error) {
	if len(statuses) == 0 {
		statuses = []work.Status{work.StatusCompleted, work.StatusFailed, work.StatusCancelled}
	}
	total := 0
	for _, st := range statuses {
		if !st.Terminal() {
			return total, fmt.Errorf("%w: only finished work can be pruned, not %q", work.ErrInvalidState, st)
		}
		r, err := b.run(ctx, pruneScript, string(st), i64(micros(before)), "5000")
		if err != nil {
			return total, err
		}
		if len(r) < 2 {
			return total, fmt.Errorf("work/redis: unexpected prune reply %v", r)
		}
		n, _ := r[1].(int64)
		total += int(n)
	}
	return total, nil
}

var _ = errors.Is
