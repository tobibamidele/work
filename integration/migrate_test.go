package integration

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/tobibamidele/lathe"
	"github.com/tobibamidele/lathe/migrate"
	"github.com/tobibamidele/lathe/sqlite"
	"github.com/tobibamidele/work"
	"github.com/tobibamidele/work/database"
)

func TestMigrationsAreIdempotentAndIsolated(t *testing.T) {
	ctx := context.Background()
	for _, d := range dialects {
		d := d
		t.Run(d.name, func(t *testing.T) {
			db := d.open(t)
			applied, err := database.Migrate(ctx, db)
			if err != nil || len(applied) != 0 {
				t.Fatalf("a second Migrate must be a no-op: applied=%v err=%v", applied, err)
			}
			states, err := database.MigrationStatus(ctx, db)
			if err != nil || len(states) != 1 || !states[0].Applied {
				t.Fatalf("status = %+v, %v", states, err)
			}
			// the work migrations keep their own bookkeeping table, so they can
			// never show up as "missing" in the application's own migrations
			if _, err := db.Exec(ctx, "SELECT COUNT(*) FROM work_migrations"); err != nil {
				t.Fatalf("work_migrations: %v", err)
			}
			_, err = migrate.Status(ctx, db, mustFS(t))
			if err != nil {
				t.Fatalf("the application's own migration table must be unaffected: %v", err)
			}
		})
	}
}

func TestMigrateDownAndUpRoundTrip(t *testing.T) {
	ctx := context.Background()
	// use a private sqlite database so the shared ones keep their schema
	dir := t.TempDir()
	db, err := sqlite.Open(dir + "/rt.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := database.Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	b, err := database.New(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	_ = b
	if _, err := database.MigrateDown(ctx, db, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := database.New(ctx, db); err == nil {
		t.Fatal("the table is gone after MigrateDown")
	}
	if _, err := database.Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	if _, err := database.New(ctx, db); err != nil {
		t.Fatalf("after migrating up again: %v", err)
	}
}

func TestNewRefusesBadHandles(t *testing.T) {
	ctx := context.Background()
	if _, err := database.New(ctx, nil); !errors.Is(err, work.ErrConfig) {
		t.Fatalf("nil: %v", err)
	}
	// the table was never created
	db, err := sqlite.Open(t.TempDir() + "/empty.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = database.New(ctx, db)
	if err == nil || !strings.Contains(err.Error(), "database.Migrate") {
		t.Fatalf("a missing table must produce an actionable error, got %v", err)
	}
	// a transaction-bound handle
	if _, err := database.Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	err = db.Tx(ctx, func(tx *lathe.DB) error {
		_, err := database.New(ctx, tx)
		if !errors.Is(err, work.ErrConfig) {
			t.Errorf("tx-bound handle: %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestTablesMatchTheShippedMigrations(t *testing.T) {
	// Tables() is the source the migrations were generated from. Check that the
	// migrated table has exactly its columns and indexes.
	tables := database.Tables()
	if len(tables) != 1 {
		t.Fatalf("tables = %d", len(tables))
	}
	ctx := context.Background()
	db := dialects[2].open(t) // sqlite: PRAGMA introspection
	rows, err := lathe.Raw[struct {
		Name string
	}](ctx, db, "SELECT name FROM pragma_table_info('work')")
	if err != nil {
		t.Fatal(err)
	}
	have := map[string]bool{}
	for _, r := range rows {
		have[r.Name] = true
	}
	for _, want := range []string{"id", "type", "version", "queue", "payload", "status", "priority", "attempts", "max_attempts",
		"available_at", "started_at", "completed_at", "locked_at", "lease_until", "worker_id", "cancel_requested_at",
		"last_error", "last_error_at", "idempotency_key", "unique_key", "active_unique_key", "schema_fingerprint",
		"timeout_ms", "metadata", "created_at", "updated_at"} {
		if !have[want] {
			t.Errorf("column %q missing", want)
		}
	}
	if len(have) != 26 {
		t.Errorf("table has %d columns, want 26", len(have))
	}
	idx, err := lathe.Raw[struct{ Name string }](ctx, db, "SELECT name FROM pragma_index_list('work')")
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, i := range idx {
		names[i.Name] = true
	}
	for _, want := range []string{"work_claim_idx", "work_lease_idx", "work_type_created_idx", "work_created_idx", "work_active_unique_idx"} {
		if !names[want] {
			t.Errorf("index %q missing", want)
		}
	}
}

func mustFS(t *testing.T) fsLike {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(dir+"/20260101000000_noop.up.sql", []byte("SELECT 1;"), 0o600); err != nil {
		t.Fatal(err)
	}
	return os.DirFS(dir)
}
