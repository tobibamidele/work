package database

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/tobibamidele/lathe"
	"github.com/tobibamidele/work"
)

// This file holds the only raw SQL of the backend: the atomic claim and the
// reaper.
//
// Why raw SQL: claiming must select candidate rows, lock them so concurrent
// workers skip rather than wait for them, and update them as one atomic step.
// FOR UPDATE SKIP LOCKED and UPDATE ... RETURNING are not part of a portable
// query builder, and expressing the eligibility predicate (available, or
// running with an expired lease) through the builder would obscure the one
// piece of logic whose correctness the whole system depends on.
//
// The algorithm is the same on every database; only the syntax differs.
//
//	PostgreSQL  one statement: a CTE selects candidates FOR UPDATE SKIP LOCKED
//	            and UPDATE ... FROM it RETURNING the claimed rows.
//	SQLite      one statement: UPDATE ... WHERE id IN (SELECT ... LIMIT n)
//	            RETURNING. A single write statement is atomic under SQLite's
//	            single-writer model, so no row locks are needed.
//	MySQL 8     a transaction: SELECT ... FOR UPDATE SKIP LOCKED, UPDATE the
//	            selected ids, SELECT them back (MySQL has no RETURNING).
//
// All times are bound parameters supplied by the caller. Timestamps are
// always UTC, which also keeps SQLite's text comparison ordered correctly.

type stmt struct {
	sql  string
	args []any
}

func placeholders(n int) string {
	if n <= 0 {
		return ""
	}
	return strings.TrimSuffix(strings.Repeat("?, ", n), ", ")
}

func anyStrings(ss []string) []any {
	out := make([]any, len(ss))
	for i, s := range ss {
		out[i] = s
	}
	return out
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
	now := o.Now.UTC()
	lease := now.Add(o.LeaseDuration)
	q := b.q

	// Eligibility: queue and type filters, then available-and-due work or
	// running work whose lease expired. Expired work with no attempts left, or
	// with a pending cancellation, is never re-run; the reaper settles it.
	var where stmt
	where.sql = q("queue") + " IN (" + placeholders(len(o.Queues)) + ")"
	where.args = anyStrings(o.Queues)
	if len(o.Types) > 0 {
		where.sql += " AND " + q("type") + " IN (" + placeholders(len(o.Types)) + ")"
		where.args = append(where.args, anyStrings(o.Types)...)
	}
	where.sql += fmt.Sprintf(" AND ((%s = 'available' AND %s <= ?) OR (%s = 'running' AND %s < ? AND %s < %s AND %s IS NULL))",
		q("status"), q("available_at"), q("status"), q("lease_until"), q("attempts"), q("max_attempts"), q("cancel_requested_at"))
	where.args = append(where.args, now, now)

	order := fmt.Sprintf("ORDER BY %s DESC, %s ASC, %s ASC, %s ASC", q("priority"), q("available_at"), q("created_at"), q("id"))

	// SET list. last_error is assigned first on purpose: MySQL evaluates
	// assignments left to right, and these CASE expressions must see the row's
	// status before it is overwritten below.
	set := stmt{
		sql: fmt.Sprintf("%s = CASE WHEN %s = 'running' THEN 'lease expired; reclaimed by another worker' ELSE %s END, "+
			"%s = CASE WHEN %s = 'running' THEN ? ELSE %s END, "+
			"%s = %s + 1, %s = ?, %s = ?, %s = ?, %s = ?, %s = ?, %s = 'running'",
			q("last_error"), q("status"), q("last_error"),
			q("last_error_at"), q("status"), q("last_error_at"),
			q("attempts"), q("attempts"), q("worker_id"), q("locked_at"), q("lease_until"), q("started_at"), q("updated_at"), q("status")),
		args: []any{now, o.WorkerID, now, lease, now, now},
	}

	recs, err := b.pickAndUpdate(ctx, where, order, o.Limit, set)
	if err != nil {
		return nil, err
	}
	if len(recs) == 0 {
		return nil, work.ErrNoWork
	}
	sort.Slice(recs, func(i, j int) bool {
		a, c := recs[i], recs[j]
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
	return recs, nil
}

// Reap implements work.Backend. It settles running records whose lease expired
// and that Claim will never pick up again.
func (b *Backend) Reap(ctx context.Context, now time.Time, limit int) ([]*work.Record, error) {
	if limit < 1 {
		limit = 100
	}
	now = now.UTC()
	q := b.q
	where := stmt{
		sql: fmt.Sprintf("%s = 'running' AND %s < ? AND (%s >= %s OR %s IS NOT NULL)",
			q("status"), q("lease_until"), q("attempts"), q("max_attempts"), q("cancel_requested_at")),
		args: []any{now},
	}
	order := "ORDER BY " + q("id") + " ASC"
	cp := q("cancel_requested_at")
	set := stmt{
		sql: fmt.Sprintf("%s = CASE WHEN %s IS NULL THEN 'failed' ELSE 'cancelled' END, "+
			"%s = CASE WHEN %s IS NULL THEN 'lease expired and no attempts remain' ELSE 'lease expired after cancellation was requested' END, "+
			"%s = ?, %s = ?, %s = NULL, %s = NULL, %s = NULL, %s = NULL",
			q("status"), cp, q("last_error"), cp,
			q("last_error_at"), q("updated_at"), q("active_unique_key"), q("worker_id"), q("locked_at"), q("lease_until")),
		args: []any{now, now},
	}
	recs, err := b.pickAndUpdate(ctx, where, order, limit, set)
	if err != nil {
		return nil, err
	}
	sort.Slice(recs, func(i, j int) bool { return recs[i].ID < recs[j].ID })
	return recs, nil
}

// pickAndUpdate atomically applies set to up to limit rows matching where, in
// the given order, skipping rows other transactions have locked, and returns
// the updated rows.
func (b *Backend) pickAndUpdate(ctx context.Context, where stmt, order string, limit int, set stmt) ([]*work.Record, error) {
	q := b.q
	table := q(TableName)
	pick := fmt.Sprintf("SELECT %s FROM %s WHERE %s %s LIMIT ?", q("id"), table, where.sql, order)
	pickArgs := append(append([]any(nil), where.args...), limit)

	switch b.dialect {
	case "postgres":
		qualified := make([]string, len(columnNames))
		for i, c := range columnNames {
			qualified[i] = table + "." + q(c)
		}
		sqlText := fmt.Sprintf("WITH picked AS (%s FOR UPDATE SKIP LOCKED) UPDATE %s SET %s FROM picked WHERE %s.%s = picked.%s RETURNING %s",
			pick, table, set.sql, table, q("id"), q("id"), strings.Join(qualified, ", "))
		args := append(append([]any(nil), pickArgs...), set.args...)
		return b.queryRows(ctx, b.db, sqlText, args)

	case "sqlite":
		cols := make([]string, len(columnNames))
		for i, c := range columnNames {
			cols[i] = q(c)
		}
		sqlText := fmt.Sprintf("UPDATE %s SET %s WHERE %s IN (%s) RETURNING %s",
			table, set.sql, q("id"), pick, strings.Join(cols, ", "))
		args := append(append([]any(nil), set.args...), pickArgs...)
		return b.queryRows(ctx, b.db, sqlText, args)

	case "mysql":
		var out []*work.Record
		err := b.db.Tx(ctx, func(tx *lathe.DB) error {
			ids, err := lathe.Raw[string](ctx, tx, pick+" FOR UPDATE SKIP LOCKED", pickArgs...)
			if err != nil {
				return err
			}
			if len(ids) == 0 {
				return nil
			}
			in := placeholders(len(ids))
			idArgs := anyStrings(ids)
			if _, err := tx.Exec(ctx, fmt.Sprintf("UPDATE %s SET %s WHERE %s IN (%s)", table, set.sql, q("id"), in),
				append(append([]any(nil), set.args...), idArgs...)...); err != nil {
				return err
			}
			cols := make([]string, len(columnNames))
			for i, c := range columnNames {
				cols[i] = q(c)
			}
			out, err = b.queryRows(ctx, tx, fmt.Sprintf("SELECT %s FROM %s WHERE %s IN (%s)", strings.Join(cols, ", "), table, q("id"), in), idArgs)
			return err
		})
		return out, err
	}
	return nil, fmt.Errorf("%w: dialect %q", work.ErrUnsupported, b.dialect)
}

// queryRows runs a statement returning whole rows (in table column order) and
// converts them to records. Scanning goes through the same field accessors as
// the Lathe Model.
func (b *Backend) queryRows(ctx context.Context, db *lathe.DB, sqlText string, args []any) ([]*work.Record, error) {
	rows, err := db.Query(ctx, sqlText, args...)
	if err != nil {
		return nil, err
	}
	return scanRecords(rows)
}

func scanRecords(rows *sql.Rows) ([]*work.Record, error) {
	defer rows.Close()
	var out []*work.Record
	dests := make([]any, len(spec.Fields))
	for rows.Next() {
		var r row
		for i, f := range spec.Fields {
			dests[i] = f.Ptr(&r)
		}
		if err := rows.Scan(dests...); err != nil {
			return nil, err
		}
		out = append(out, toRecord(&r))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
