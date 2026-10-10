package database

import (
	s "github.com/tobibamidele/lathe/schema"
)

// TableName is the name of the work table.
const TableName = "work"

// Status values stored in the status column.
var statusValues = []string{"available", "running", "completed", "failed", "cancelled"}

// Tables returns the schema of the work table for applications that describe
// their whole database with Lathe's schema package. Add it to your schema and
// let `lathe migrate diff` manage it with the rest:
//
//	var Schema = s.New(cfg, append(appTables, workdb.Tables()...)...)
//
// Applications that do not want to manage the table themselves call Migrate
// instead, which applies the embedded migrations (generated from this schema)
// and keeps its own bookkeeping table. Use one of the two, not both.
//
// Everything the backend needs is expressible in plain Lathe schema terms:
// there are no partial indexes or dialect specific objects. Uniqueness of
// active work is enforced by a unique index over (queue, active_unique_key),
// where active_unique_key is NULL for inactive work; NULLs are distinct in all
// supported databases, so history never blocks new dispatch.
func Tables() []*s.TableBuilder {
	return []*s.TableBuilder{workTable()}
}

func workTable() *s.TableBuilder {
	return s.Table(TableName,
		s.UUID("id").PrimaryKey(),
		s.VarChar("type", 255),
		s.Int("version"),
		s.VarChar("queue", 255),
		s.JSON("payload"),
		s.Enum("status", statusValues...),
		s.Int("priority"),
		s.Int("attempts"),
		s.Int("max_attempts"),
		s.Timestamp("available_at"),
		s.Timestamp("started_at").Nullable(),
		s.Timestamp("completed_at").Nullable(),
		s.Timestamp("locked_at").Nullable(),
		s.Timestamp("lease_until").Nullable(),
		s.VarChar("worker_id", 255).Nullable(),
		s.Timestamp("cancel_requested_at").Nullable(),
		s.Text("last_error").Nullable(),
		s.Timestamp("last_error_at").Nullable(),
		s.VarChar("idempotency_key", 255).Nullable(),
		s.VarChar("unique_key", 255).Nullable(),
		s.VarChar("active_unique_key", 255).Nullable(),
		s.VarChar("schema_fingerprint", 64).Nullable(),
		s.BigInt("timeout_ms").Nullable(),
		s.JSON("metadata").Nullable(),
		s.Timestamp("created_at"),
		s.Timestamp("updated_at"),

		// Claiming: (queue, status, available_at, priority, created_at).
		s.Index("queue", "status", "available_at", "priority", "created_at").Name("work_claim_idx"),
		// Lease recovery and the reaper: (status, lease_until). Its status
		// prefix also serves status inspection.
		s.Index("status", "lease_until").Name("work_lease_idx"),
		// Inspection: (type, created_at) and created_at.
		s.Index("type", "created_at").Name("work_type_created_idx"),
		s.Index("created_at").Name("work_created_idx"),
		// Unique active work.
		s.UniqueIndex("queue", "active_unique_key").Name("work_active_unique_idx"),
	)
}
