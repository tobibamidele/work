package work

import (
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/tobibamidele/work/internal/id"
)

// ID identifies one durable work record. IDs are unique, time-ordered, safe
// to expose and stable for the lifetime of the record, which makes them
// suitable for logs, traces and as an idempotency key for external systems.
type ID string

// String returns the ID as a string.
func (id ID) String() string { return string(id) }

// Status is the persisted state of a work record.
//
// There is deliberately no "retrying" status: a retry is an available record
// with a future AvailableAt and an incremented Attempts.
type Status string

// The persisted states.
const (
	StatusAvailable Status = "available"
	StatusRunning   Status = "running"
	StatusCompleted Status = "completed"
	StatusFailed    Status = "failed"
	StatusCancelled Status = "cancelled"
)

// AllStatuses lists every valid status.
var AllStatuses = []Status{StatusAvailable, StatusRunning, StatusCompleted, StatusFailed, StatusCancelled}

// Valid reports whether s is a known status.
func (s Status) Valid() bool {
	for _, v := range AllStatuses {
		if s == v {
			return true
		}
	}
	return false
}

// Active reports whether work in this status still counts for uniqueness:
// available and running work is active, terminal work is not.
func (s Status) Active() bool { return s == StatusAvailable || s == StatusRunning }

// Terminal reports whether no further execution happens without a manual
// retry.
func (s Status) Terminal() bool {
	return s == StatusCompleted || s == StatusFailed || s == StatusCancelled
}

// Record is the canonical durable representation of one unit of work. Zero
// times mean "not set".
type Record struct {
	ID      ID
	Type    string
	Version int
	Queue   string

	// Payload is the serialized, validated, normalized payload.
	Payload []byte

	Status      Status
	Priority    int
	Attempts    int
	MaxAttempts int

	AvailableAt time.Time
	StartedAt   time.Time
	CompletedAt time.Time

	// Lease ownership.
	LockedAt   time.Time
	LeaseUntil time.Time
	WorkerID   string

	// CancelRequestedAt is set when cancellation was requested while the work
	// was running. The owning worker observes it at its next heartbeat.
	CancelRequestedAt time.Time

	LastError   string
	LastErrorAt time.Time

	// IdempotencyKey is a stable key handlers may pass to external systems.
	// When empty, handlers should use the work ID.
	IdempotencyKey string

	// UniqueKey is the key given to Unique. ActiveUniqueKey is what the
	// uniqueness mechanism enforces: it equals UniqueKey while the work is
	// active and is empty afterwards, so history never blocks new dispatch.
	UniqueKey       string
	ActiveUniqueKey string

	// SchemaFingerprint is Validate's fingerprint of the payload schema at
	// dispatch time, when available. It is diagnostic only: Version is the
	// compatibility boundary.
	SchemaFingerprint string

	// Timeout is the per-execution timeout, zero for none.
	Timeout time.Duration

	// Metadata carries dispatch metadata, including propagated trace context.
	Metadata map[string]string

	CreatedAt time.Time
	UpdatedAt time.Time
}

// Clone returns a deep copy.
func (r *Record) Clone() *Record {
	if r == nil {
		return nil
	}
	c := *r
	if r.Payload != nil {
		c.Payload = append([]byte(nil), r.Payload...)
	}
	if r.Metadata != nil {
		c.Metadata = make(map[string]string, len(r.Metadata))
		for k, v := range r.Metadata {
			c.Metadata[k] = v
		}
	}
	return &c
}

// Ref is the durable reference Dispatch returns. It is available only after
// the backend confirmed the record is persisted.
type Ref struct {
	ID          ID
	Type        string
	Version     int
	Queue       string
	AvailableAt time.Time
}

// String returns the work ID.
func (r Ref) String() string { return string(r.ID) }

func refOf(rec *Record) Ref {
	return Ref{ID: rec.ID, Type: rec.Type, Version: rec.Version, Queue: rec.Queue, AvailableAt: rec.AvailableAt}
}

// MaxErrorBytes bounds the error text stored with a record.
const MaxErrorBytes = 8 << 10

// ErrorText renders err for storage in Record.LastError. Panics include the
// stack. The result is truncated at a rune boundary to MaxErrorBytes. It
// never includes payloads.
func ErrorText(err error) string {
	if err == nil {
		return ""
	}
	var b strings.Builder
	b.WriteString(err.Error())
	var pe *PanicError
	if errors.As(err, &pe) && len(pe.Stack) > 0 {
		b.WriteString("\n")
		b.Write(pe.Stack)
	}
	return truncateUTF8(b.String(), MaxErrorBytes)
}

func truncateUTF8(s string, max int) string {
	if len(s) <= max {
		return s
	}
	cut := max
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
}

func newID() string { return id.New() }
