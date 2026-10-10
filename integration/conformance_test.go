package integration

import (
	"context"
	"testing"

	"github.com/tobibamidele/lathe"
	"github.com/tobibamidele/work"
	"github.com/tobibamidele/work/backendtest"
	"github.com/tobibamidele/work/database"
)

func newBackend(t *testing.T, db *lathe.DB) *database.Backend {
	t.Helper()
	b, err := database.New(context.Background(), db)
	if err != nil {
		t.Fatalf("database.New: %v", err)
	}
	return b
}

// TestConformance runs the same semantic suite the in-memory reference backend
// passes, against every real database.
func TestConformance(t *testing.T) {
	for _, d := range dialects {
		d := d
		t.Run(d.name, func(t *testing.T) {
			backendtest.Run(t, func(t *testing.T) work.Backend {
				return newBackend(t, freshDB(t, d))
			})
		})
	}
}
