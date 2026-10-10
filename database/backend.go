// Package database is the work.Backend that stores work in the application's
// own SQL database through Lathe.
//
// It supports every database Lathe supports: PostgreSQL, MySQL 8 (and
// MariaDB 10.6+) and SQLite 3.35+. Schema, typed columns, transactions and
// error classification all come from Lathe; the only raw SQL is the atomic
// claim and the reaper (claim.go), which need row locking and conditional
// multi-row updates that a generic query builder cannot express.
//
// Because the work table lives in your database, work can be enqueued in the
// same transaction as your own writes (see Tx and DispatchTx): the work exists
// if and only if the surrounding transaction commits.
package database

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/tobibamidele/lathe"
	"github.com/tobibamidele/work"
)

// ErrNotInTransaction is returned when a transactional operation is given a
// handle that is not bound to a transaction. Using the plain handle would
// silently break the atomicity the caller asked for, so it is refused.
var ErrNotInTransaction = errors.New("work/database: the *lathe.DB is not inside a transaction")

// Backend implements work.Backend on a Lathe database handle.
type Backend struct {
	db      *lathe.DB
	model   *lathe.Model[row]
	dialect string
	q       func(string) string
	frags   fragments
}

var _ work.Backend = (*Backend)(nil)

// fragments are SQL snippets shared by several statements, built once with the
// dialect's identifier quoting.
type fragments struct {
	cancelPending string // cancel_requested_at IS NOT NULL
	// statusBack is the status a worker-abandoned record goes to: available,
	// or cancelled when cancellation was requested while it ran.
	statusBack  string
	activeBack  string
	attemptsDec string
}

// New returns a Backend for db. db must be a plain handle (not bound to a
// transaction) opened with Lathe's postgres.Open, mysql.Open or sqlite.Open,
// or built from your own *sql.DB with their New functions.
//
// New verifies that the database provides the primitives the backend needs
// and refuses to start, with a clear error, when it does not: a database that
// cannot claim work atomically must not pretend to.
func New(ctx context.Context, db *lathe.DB) (*Backend, error) {
	if db == nil {
		return nil, fmt.Errorf("%w: nil database", work.ErrConfig)
	}
	if db.InTx() {
		return nil, fmt.Errorf("%w: New needs a plain *lathe.DB, not one bound to a transaction", work.ErrConfig)
	}
	b := &Backend{db: db, dialect: db.Dialect().Name(), q: db.Dialect().Quote}
	if err := b.checkSupport(ctx); err != nil {
		return nil, err
	}
	b.model = lathe.NewModel(db, spec)
	q := b.q
	cp := q("cancel_requested_at")
	b.frags = fragments{
		cancelPending: cp + " IS NOT NULL",
		statusBack:    "CASE WHEN " + cp + " IS NULL THEN 'available' ELSE 'cancelled' END",
		activeBack:    "CASE WHEN " + cp + " IS NULL THEN " + q("active_unique_key") + " ELSE NULL END",
		attemptsDec:   "CASE WHEN " + q("attempts") + " > 0 THEN " + q("attempts") + " - 1 ELSE 0 END",
	}
	if _, err := b.model.Count(ctx, col.ID.Eq(zeroUUID)); err != nil {
		return nil, fmt.Errorf("work/database: the %q table is not usable (%w); apply the migrations with database.Migrate or add database.Tables() to your Lathe schema", TableName, err)
	}
	return b, nil
}

// checkSupport enforces the minimum capabilities of the database: atomic
// claiming needs row locking with SKIP LOCKED (PostgreSQL, MySQL 8), or
// single-statement UPDATE ... RETURNING under SQLite's single writer.
func (b *Backend) checkSupport(ctx context.Context) error {
	switch b.dialect {
	case "postgres":
		rows, err := lathe.Raw[int64](ctx, b.db, "SELECT current_setting('server_version_num')::bigint")
		if err != nil || len(rows) != 1 {
			return fmt.Errorf("work/database: reading the PostgreSQL version: %v", err)
		}
		if rows[0] < 90500 {
			return fmt.Errorf("%w: PostgreSQL %d lacks FOR UPDATE SKIP LOCKED (needs 9.5+)", work.ErrUnsupported, rows[0])
		}
	case "mysql":
		rows, err := lathe.Raw[string](ctx, b.db, "SELECT VERSION()")
		if err != nil || len(rows) != 1 {
			return fmt.Errorf("work/database: reading the MySQL version: %v", err)
		}
		major, minor, patch := parseVersion(rows[0])
		maria := strings.Contains(strings.ToLower(rows[0]), "mariadb")
		switch {
		case maria && (major < 10 || (major == 10 && minor < 6)):
			return fmt.Errorf("%w: MariaDB %s lacks SKIP LOCKED (needs 10.6+), so work cannot be claimed atomically", work.ErrUnsupported, rows[0])
		case !maria && (major < 8 || (major == 8 && minor == 0 && patch < 1)):
			return fmt.Errorf("%w: MySQL %s lacks SKIP LOCKED (needs 8.0.1+), so work cannot be claimed atomically", work.ErrUnsupported, rows[0])
		}
	case "sqlite":
		rows, err := lathe.Raw[string](ctx, b.db, "SELECT sqlite_version()")
		if err != nil || len(rows) != 1 {
			return fmt.Errorf("work/database: reading the SQLite version: %v", err)
		}
		major, minor, _ := parseVersion(rows[0])
		if major < 3 || (major == 3 && minor < 35) {
			return fmt.Errorf("%w: SQLite %s lacks UPDATE ... RETURNING (needs 3.35+)", work.ErrUnsupported, rows[0])
		}
	default:
		return fmt.Errorf("%w: dialect %q has no safe atomic claim strategy in this backend", work.ErrUnsupported, b.dialect)
	}
	return nil
}

func parseVersion(s string) (major, minor, patch int) {
	nums := make([]int, 0, 3)
	cur := ""
	flush := func() bool {
		if cur == "" {
			return false
		}
		n, _ := strconv.Atoi(cur)
		nums = append(nums, n)
		cur = ""
		return true
	}
	for _, r := range s {
		if r >= '0' && r <= '9' {
			cur += string(r)
			continue
		}
		if !flush() || r != '.' || len(nums) == 3 {
			break
		}
	}
	flush()
	for len(nums) < 3 {
		nums = append(nums, 0)
	}
	return nums[0], nums[1], nums[2]
}

// Capabilities implements work.Backend.
func (b *Backend) Capabilities() work.Capabilities {
	return work.Capabilities{
		Name: "database/" + b.dialect, DelayedWork: true, Cancellation: true, Priorities: true,
		StrongUniqueness: true, Transactions: true,
	}
}

func notFound(id work.ID) error { return fmt.Errorf("%w: %s", work.ErrNotFound, id) }

// ---- enqueue ----

// Enqueue implements work.Backend.
func (b *Backend) Enqueue(ctx context.Context, rec *work.Record) error {
	return b.enqueue(ctx, b.db, rec)
}

func (b *Backend) enqueue(ctx context.Context, db *lathe.DB, rec *work.Record) error {
	r, err := fromRecord(rec)
	if err != nil {
		return err
	}
	m := lathe.NewModel(db, spec)
	if rec.ActiveUniqueKey == "" {
		return m.Create(ctx, &r)
	}
	// A unique key is claimed with INSERT ... ON CONFLICT DO NOTHING (ON
	// DUPLICATE KEY on MySQL) rather than by provoking a unique violation.
	// A violation would abort the caller's whole transaction on PostgreSQL;
	// this does not. Lathe reads back the row that holds the key, so a row
	// with a different ID means someone else owns it.
	for attempt := 0; attempt < 3; attempt++ {
		cp := r
		err := m.Upsert(&cp).OnConflict(col.Queue, col.ActiveUniqueKey).DoNothing().Exec(ctx)
		switch {
		case err == nil && cp.ID == r.ID:
			return nil
		case err == nil:
			return &work.DuplicateError{Queue: rec.Queue, UniqueKey: rec.ActiveUniqueKey, ExistingID: work.ID(cp.ID.String())}
		case errors.Is(err, lathe.ErrNotFound):
			// The holder finished between the conflict and the read-back;
			// the key is free again, so try to insert again.
			continue
		default:
			return err
		}
	}
	return fmt.Errorf("work/database: could not settle unique key %q after repeated races", rec.ActiveUniqueKey)
}

// ---- reads ----

// Get implements work.Backend.
func (b *Backend) Get(ctx context.Context, id work.ID) (*work.Record, error) {
	uid, err := parseID(id)
	if err != nil {
		return nil, err
	}
	r, err := b.model.FindFirst().Where(col.ID.Eq(uid)).One(ctx)
	if err != nil {
		if errors.Is(err, lathe.ErrNotFound) {
			return nil, notFound(id)
		}
		return nil, err
	}
	return toRecord(r), nil
}

// List implements work.Backend.
func (b *Backend) List(ctx context.Context, f work.ListFilter) ([]*work.Record, error) {
	var where []lathe.Expr
	if len(f.Statuses) > 0 {
		ss := make([]string, len(f.Statuses))
		for i, s := range f.Statuses {
			ss[i] = string(s)
		}
		where = append(where, col.Status.In(ss...))
	}
	if f.Queue != "" {
		where = append(where, col.Queue.Eq(f.Queue))
	}
	if f.Type != "" {
		where = append(where, col.Type.Eq(f.Type))
	}
	if !f.CreatedAfter.IsZero() {
		where = append(where, col.CreatedAt.Gt(f.CreatedAfter.UTC()))
	}
	if !f.CreatedBefore.IsZero() {
		where = append(where, col.CreatedAt.Lt(f.CreatedBefore.UTC()))
	}
	q := b.model.FindMany().Where(where...)
	if f.OldestFirst {
		q = q.OrderBy(col.CreatedAt.Asc(), col.ID.Asc())
	} else {
		q = q.OrderBy(col.CreatedAt.Desc(), col.ID.Desc())
	}
	if f.Limit > 0 {
		q = q.Limit(f.Limit)
	}
	if f.Offset > 0 {
		q = q.Offset(f.Offset)
	}
	rows, err := q.All(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]*work.Record, len(rows))
	for i := range rows {
		out[i] = toRecord(&rows[i])
	}
	return out, nil
}

// Stats implements work.Backend.
func (b *Backend) Stats(ctx context.Context) ([]work.QueueStat, error) {
	type stat struct {
		Queue  string
		Status string
		N      int64
	}
	q := b.q
	rows, err := lathe.Raw[stat](ctx, b.db, fmt.Sprintf(
		"SELECT %s AS queue, %s AS status, COUNT(*) AS n FROM %s GROUP BY %s, %s ORDER BY %s, %s",
		q("queue"), q("status"), q(TableName), q("queue"), q("status"), q("queue"), q("status")))
	if err != nil {
		return nil, err
	}
	out := make([]work.QueueStat, len(rows))
	for i, r := range rows {
		out[i] = work.QueueStat{Queue: r.Queue, Status: work.Status(r.Status), Count: r.N}
	}
	return out, nil
}

// ---- ownership guarded transitions ----

const (
	stAvailable = "available"
	stRunning   = "running"
	stCompleted = "completed"
	stFailed    = "failed"
	stCancelled = "cancelled"
)

// owned applies sets to the record only while it is running and leased to
// workerID. The returned row is non-nil only when zero rows matched but the
// worker does own the record (a driver reporting changed rather than matched
// rows); callers that need nothing from it ignore it.
func (b *Backend) owned(ctx context.Context, id work.ID, workerID string, sets ...lathe.Assignment) (*row, error) {
	uid, err := parseID(id)
	if err != nil {
		return nil, err
	}
	n, err := b.model.Update().Set(sets...).
		Where(col.ID.Eq(uid), col.Status.Eq(stRunning), col.WorkerID.Eq(workerID)).Exec(ctx)
	if err != nil {
		return nil, err
	}
	if n > 0 {
		return nil, nil
	}
	cur, err := b.model.FindFirst().Where(col.ID.Eq(uid)).One(ctx)
	if err != nil {
		if errors.Is(err, lathe.ErrNotFound) {
			return nil, notFound(id)
		}
		return nil, err
	}
	switch {
	case cur.Status == stRunning && cur.WorkerID != nil && *cur.WorkerID == workerID:
		return cur, nil
	case cur.Status == stCompleted:
		return nil, fmt.Errorf("%w: %s", work.ErrAlreadyCompleted, id)
	case cur.Status == stCancelled:
		return nil, fmt.Errorf("%w: %s", work.ErrCancelled, id)
	}
	return nil, fmt.Errorf("%w: %s is %s and not leased to %s", work.ErrLeaseLost, id, cur.Status, workerID)
}

func releaseLease() []lathe.Assignment {
	return []lathe.Assignment{col.WorkerID.SetNull(), col.LockedAt.SetNull(), col.LeaseUntil.SetNull()}
}

// Complete implements work.Backend.
func (b *Backend) Complete(ctx context.Context, id work.ID, workerID string, now time.Time) error {
	now = now.UTC()
	sets := append([]lathe.Assignment{
		col.Status.Set(stCompleted), col.CompletedAt.Set(now), col.UpdatedAt.Set(now),
		col.ActiveUniqueKey.SetNull(), col.CancelRequestedAt.SetNull(),
	}, releaseLease()...)
	_, err := b.owned(ctx, id, workerID, sets...)
	return err
}

// Retry implements work.Backend.
func (b *Backend) Retry(ctx context.Context, id work.ID, workerID string, next time.Time, cause error, now time.Time) error {
	now = now.UTC()
	sets := append([]lathe.Assignment{
		col.Status.SetExpr(lathe.SQL(b.frags.statusBack)),
		col.ActiveUniqueKey.SetExpr(lathe.SQL(b.frags.activeBack)),
		col.AvailableAt.Set(next.UTC()),
		col.LastError.Set(work.ErrorText(cause)), col.LastErrorAt.Set(now), col.UpdatedAt.Set(now),
	}, releaseLease()...)
	_, err := b.owned(ctx, id, workerID, sets...)
	return err
}

// Fail implements work.Backend.
func (b *Backend) Fail(ctx context.Context, id work.ID, workerID string, cause error, now time.Time) error {
	now = now.UTC()
	sets := append([]lathe.Assignment{
		col.Status.Set(stFailed),
		col.LastError.Set(work.ErrorText(cause)), col.LastErrorAt.Set(now), col.UpdatedAt.Set(now),
		col.ActiveUniqueKey.SetNull(), col.CancelRequestedAt.SetNull(),
	}, releaseLease()...)
	_, err := b.owned(ctx, id, workerID, sets...)
	return err
}

// Release implements work.Backend.
func (b *Backend) Release(ctx context.Context, id work.ID, workerID string, now time.Time) error {
	now = now.UTC()
	sets := append([]lathe.Assignment{
		col.Status.SetExpr(lathe.SQL(b.frags.statusBack)),
		col.ActiveUniqueKey.SetExpr(lathe.SQL(b.frags.activeBack)),
		col.Attempts.SetExpr(lathe.SQL(b.frags.attemptsDec)),
		col.AvailableAt.Set(now), col.UpdatedAt.Set(now),
	}, releaseLease()...)
	_, err := b.owned(ctx, id, workerID, sets...)
	return err
}

// FinishCancel implements work.Backend.
func (b *Backend) FinishCancel(ctx context.Context, id work.ID, workerID string, cause error, now time.Time) error {
	now = now.UTC()
	sets := []lathe.Assignment{
		col.Status.Set(stCancelled), col.UpdatedAt.Set(now), col.ActiveUniqueKey.SetNull(),
	}
	if cause != nil {
		sets = append(sets, col.LastError.Set(work.ErrorText(cause)), col.LastErrorAt.Set(now))
	}
	_, err := b.owned(ctx, id, workerID, append(sets, releaseLease()...)...)
	return err
}

// ExtendLease implements work.Backend.
func (b *Backend) ExtendLease(ctx context.Context, id work.ID, workerID string, until time.Time, now time.Time) (bool, error) {
	cur, err := b.owned(ctx, id, workerID, col.LeaseUntil.Set(until.UTC()), col.UpdatedAt.Set(now.UTC()))
	if err != nil {
		return false, err
	}
	if cur != nil {
		return cur.CancelRequestedAt != nil, nil
	}
	uid, _ := parseID(id)
	r, err := b.model.FindFirst().Select(col.CancelRequestedAt).Where(col.ID.Eq(uid)).One(ctx)
	if err != nil {
		if errors.Is(err, lathe.ErrNotFound) {
			return false, notFound(id)
		}
		return false, err
	}
	return r.CancelRequestedAt != nil, nil
}

// ---- cancel and manual retry ----

// Cancel implements work.Backend.
func (b *Backend) Cancel(ctx context.Context, id work.ID, now time.Time) (work.CancelOutcome, error) {
	uid, err := parseID(id)
	if err != nil {
		return 0, err
	}
	now = now.UTC()
	for attempt := 0; attempt < 5; attempt++ {
		// available -> cancelled, immediately
		n, err := b.model.Update().
			Set(col.Status.Set(stCancelled), col.ActiveUniqueKey.SetNull(), col.UpdatedAt.Set(now)).
			Where(col.ID.Eq(uid), col.Status.Eq(stAvailable)).Exec(ctx)
		if err != nil {
			return 0, err
		}
		if n > 0 {
			return work.Cancelled, nil
		}
		// running -> record the request; the owner settles the final state
		n, err = b.model.Update().
			Set(col.CancelRequestedAt.Set(now), col.UpdatedAt.Set(now)).
			Where(col.ID.Eq(uid), col.Status.Eq(stRunning), col.CancelRequestedAt.IsNull()).Exec(ctx)
		if err != nil {
			return 0, err
		}
		if n > 0 {
			return work.CancelRequested, nil
		}
		cur, err := b.model.FindFirst().Where(col.ID.Eq(uid)).One(ctx)
		if err != nil {
			if errors.Is(err, lathe.ErrNotFound) {
				return 0, notFound(id)
			}
			return 0, err
		}
		switch cur.Status {
		case stCancelled:
			return work.Cancelled, nil
		case stCompleted:
			return 0, fmt.Errorf("%w: %s", work.ErrAlreadyCompleted, id)
		case stFailed:
			return 0, fmt.Errorf("%w: work %s is %s", work.ErrInvalidState, id, cur.Status)
		case stRunning:
			if cur.CancelRequestedAt != nil {
				return work.CancelRequested, nil
			}
		}
		// the state moved between our statements; look again
	}
	return 0, fmt.Errorf("work/database: work %s kept changing state while cancelling", id)
}

// Requeue implements work.Backend.
func (b *Backend) Requeue(ctx context.Context, id work.ID, now time.Time) error {
	uid, err := parseID(id)
	if err != nil {
		return err
	}
	now = now.UTC()
	cur, err := b.model.FindFirst().Where(col.ID.Eq(uid)).One(ctx)
	if err != nil {
		if errors.Is(err, lathe.ErrNotFound) {
			return notFound(id)
		}
		return err
	}
	if cur.Status != stFailed && cur.Status != stCancelled {
		return fmt.Errorf("%w: work %s is %s", work.ErrInvalidState, id, cur.Status)
	}
	sets := append([]lathe.Assignment{
		col.Status.Set(stAvailable), col.Attempts.Set(0), col.AvailableAt.Set(now), col.UpdatedAt.Set(now),
		col.CompletedAt.SetNull(), col.CancelRequestedAt.SetNull(),
	}, releaseLease()...)
	if cur.UniqueKey != nil {
		sets = append(sets, col.ActiveUniqueKey.Set(*cur.UniqueKey))
	}
	n, err := b.model.Update().Set(sets...).Where(col.ID.Eq(uid), col.Status.Eq(cur.Status)).Exec(ctx)
	if err != nil {
		if errors.Is(err, lathe.ErrUniqueViolation) && cur.UniqueKey != nil {
			if holder, herr := b.model.FindFirst().Where(col.Queue.Eq(cur.Queue), col.ActiveUniqueKey.Eq(*cur.UniqueKey)).One(ctx); herr == nil {
				return &work.DuplicateError{Queue: cur.Queue, UniqueKey: *cur.UniqueKey, ExistingID: work.ID(holder.ID.String())}
			}
		}
		return err
	}
	if n == 0 {
		return fmt.Errorf("%w: work %s changed state concurrently", work.ErrInvalidState, id)
	}
	return nil
}

func asDup(err error, target **work.DuplicateError) bool { return errors.As(err, target) }
