// Package integration runs the work database backend against real PostgreSQL,
// MySQL and SQLite databases. It is a separate module so the drivers never
// become dependencies of github.com/tobibamidele/work.
//
// Databases are taken from the environment, with local defaults:
//
//	WORK_TEST_POSTGRES  postgres://work:work@127.0.0.1:5432/work_test?sslmode=disable
//	WORK_TEST_MYSQL     work:work@tcp(127.0.0.1:3306)/work_test
//
// SQLite always runs, on a temporary file. A database that cannot be reached
// is skipped, unless WORK_TEST_REQUIRE_DB=1, which makes it a failure (CI).
package integration

import (
	"context"
	"os"
	"sync"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"

	"github.com/tobibamidele/lathe"
	"github.com/tobibamidele/lathe/mysql"
	"github.com/tobibamidele/lathe/postgres"
	"github.com/tobibamidele/lathe/sqlite"
	"github.com/tobibamidele/work/database"
)

type dialectCase struct {
	name string
	open func(t *testing.T) *lathe.DB
}

var (
	shared   = map[string]*lathe.DB{}
	sharedMu sync.Mutex
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// openShared opens (once per test binary) and migrates a database, skipping the
// test when it is unreachable.
func openShared(t *testing.T, name string, open func() (*lathe.DB, error)) *lathe.DB {
	t.Helper()
	sharedMu.Lock()
	defer sharedMu.Unlock()
	if db, ok := shared[name]; ok {
		if db == nil {
			t.Skipf("%s is not reachable", name)
		}
		return db
	}
	unavailable := func(err error) {
		shared[name] = nil
		if os.Getenv("WORK_TEST_REQUIRE_DB") == "1" {
			t.Fatalf("%s: %v", name, err)
		}
		t.Skipf("%s is not reachable: %v", name, err)
	}
	db, err := open()
	if err != nil {
		unavailable(err)
	}
	ctx := context.Background()
	if err := db.Ping(ctx); err != nil {
		unavailable(err)
	}
	// start from a clean slate, then migrate with the shipped migrations
	for _, stmt := range []string{"DROP TABLE IF EXISTS work", "DROP TABLE IF EXISTS work_migrations"} {
		if _, err := db.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %s: %v", name, stmt, err)
		}
	}
	if _, err := database.Migrate(ctx, db); err != nil {
		t.Fatalf("%s: migrate: %v", name, err)
	}
	shared[name] = db
	return db
}

var dialects = []dialectCase{
	{"postgres", func(t *testing.T) *lathe.DB {
		return openShared(t, "postgres", func() (*lathe.DB, error) {
			return postgres.Open(envOr("WORK_TEST_POSTGRES", "postgres://work:work@127.0.0.1:5432/work_test?sslmode=disable"))
		})
	}},
	{"mysql", func(t *testing.T) *lathe.DB {
		return openShared(t, "mysql", func() (*lathe.DB, error) {
			return mysql.Open(envOr("WORK_TEST_MYSQL", "work:work@tcp(127.0.0.1:3306)/work_test"))
		})
	}},
	{"sqlite", func(t *testing.T) *lathe.DB {
		return openShared(t, "sqlite", func() (*lathe.DB, error) {
			dir, err := os.MkdirTemp("", "work-sqlite-*")
			if err != nil {
				return nil, err
			}
			return sqlite.Open(dir + "/work.db")
		})
	}},
}

// freshDB returns a migrated database with an empty work table.
func freshDB(t *testing.T, d dialectCase) *lathe.DB {
	t.Helper()
	db := d.open(t)
	if _, err := db.Exec(context.Background(), "DELETE FROM work"); err != nil {
		t.Fatalf("clearing work table: %v", err)
	}
	return db
}

// forEachDialect runs fn as a subtest per database.
func forEachDialect(t *testing.T, fn func(t *testing.T, db *lathe.DB)) {
	for _, d := range dialects {
		d := d
		t.Run(d.name, func(t *testing.T) { fn(t, freshDB(t, d)) })
	}
}
