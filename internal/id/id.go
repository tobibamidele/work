// Package id generates identifiers for work records and workers.
package id

import (
	"fmt"
	"os"

	"github.com/google/uuid"
)

// New returns a time-ordered (UUIDv7) identifier. UUIDv7 is unique, safe to
// expose, sortable by creation time and index friendly. It matches Lathe's
// UUID column convention.
func New() string {
	u, err := uuid.NewV7()
	if err != nil {
		// NewV7 only fails when the system random source fails; fall back to
		// a random UUID, which is still unique.
		return uuid.NewString()
	}
	return u.String()
}

// Worker returns a worker identifier of the form host-pid-suffix. It is unique
// per Worker instance, which lease ownership relies on.
func Worker() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "worker"
	}
	suffix := uuid.NewString()[:8]
	return fmt.Sprintf("%s-%d-%s", host, os.Getpid(), suffix)
}
