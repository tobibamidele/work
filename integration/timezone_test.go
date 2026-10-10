package integration

import (
	"context"
	"testing"
	"time"

	"github.com/tobibamidele/lathe"
	"github.com/tobibamidele/lathe/mysql"
	"github.com/tobibamidele/lathe/postgres"
	"github.com/tobibamidele/work"
	"github.com/tobibamidele/work/backendtest"
)

// Timestamps are passed in UTC and compared by instant, so a session or driver
// time zone must not change behavior. The whole conformance suite runs on
// connections configured for a far-away zone.

func TestPostgresSessionTimeZoneDoesNotMatter(t *testing.T) {
	base := dialects[0].open(t) // ensures the schema exists, or skips
	_ = base
	dsn := envOr("WORK_TEST_POSTGRES", "postgres://work:work@127.0.0.1:5432/work_test?sslmode=disable")
	sep := "&"
	if !containsQuery(dsn) {
		sep = "?"
	}
	db, err := postgres.Open(dsn + sep + "options=-c%20TimeZone%3DPacific/Auckland")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	zone, err := lathe.Raw[string](context.Background(), db, "SHOW TimeZone")
	if err != nil || len(zone) != 1 || zone[0] != "Pacific/Auckland" {
		t.Fatalf("the session time zone was not applied (%v, %v): the test would prove nothing", zone, err)
	}
	backendtest.Run(t, func(t *testing.T) work.Backend {
		if _, err := db.Exec(context.Background(), "DELETE FROM work"); err != nil {
			t.Fatal(err)
		}
		return newBackend(t, db)
	})
}

func TestMySQLDriverLocationDoesNotMatter(t *testing.T) {
	base := dialects[1].open(t)
	_ = base
	dsn := envOr("WORK_TEST_MYSQL", "work:work@tcp(127.0.0.1:3306)/work_test")
	sep := "&"
	if !containsQuery(dsn) {
		sep = "?"
	}
	db, err := mysql.Open(dsn + sep + "loc=Asia%2FTokyo")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now, err := lathe.Raw[time.Time](context.Background(), db, "SELECT NOW(6)")
	if err != nil || len(now) != 1 || now[0].Location().String() != "Asia/Tokyo" {
		t.Fatalf("the driver location was not applied (%v, %v): the test would prove nothing", now, err)
	}
	backendtest.Run(t, func(t *testing.T) work.Backend {
		if _, err := db.Exec(context.Background(), "DELETE FROM work"); err != nil {
			t.Fatal(err)
		}
		return newBackend(t, db)
	})
}

func containsQuery(dsn string) bool {
	for _, c := range dsn {
		if c == '?' {
			return true
		}
	}
	return false
}
