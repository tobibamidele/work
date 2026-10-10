package database

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/tobibamidele/lathe"
	"github.com/tobibamidele/work"
)

// row is the Go shape of one work table row. It is the hand-written
// equivalent of what `lathe generate` emits for the schema in Tables(): the
// backend uses Lathe's runtime (Model, TableSpec, typed Columns, transactions,
// error classification) without requiring applications to run code generation
// for a table they do not own.
type row struct {
	ID                uuid.UUID
	Type              string
	Version           int32
	Queue             string
	Payload           lathe.JSON
	Status            string
	Priority          int32
	Attempts          int32
	MaxAttempts       int32
	AvailableAt       time.Time
	StartedAt         *time.Time
	CompletedAt       *time.Time
	LockedAt          *time.Time
	LeaseUntil        *time.Time
	WorkerID          *string
	CancelRequestedAt *time.Time
	LastError         *string
	LastErrorAt       *time.Time
	IdempotencyKey    *string
	UniqueKey         *string
	ActiveUniqueKey   *string
	SchemaFingerprint *string
	TimeoutMS         *int64
	Metadata          lathe.JSON
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

// spec lists every column. No field has a database default or is generated:
// the engine sets every value (including timestamps, from its Clock), so
// Create sends everything explicitly.
var spec = &lathe.TableSpec[row]{
	Table: TableName,
	Fields: []lathe.Field[row]{
		{Column: "id", PrimaryKey: true, Ptr: func(r *row) any { return &r.ID }},
		{Column: "type", Ptr: func(r *row) any { return &r.Type }},
		{Column: "version", Ptr: func(r *row) any { return &r.Version }},
		{Column: "queue", Ptr: func(r *row) any { return &r.Queue }},
		{Column: "payload", Ptr: func(r *row) any { return &r.Payload }},
		{Column: "status", Ptr: func(r *row) any { return &r.Status }},
		{Column: "priority", Ptr: func(r *row) any { return &r.Priority }},
		{Column: "attempts", Ptr: func(r *row) any { return &r.Attempts }},
		{Column: "max_attempts", Ptr: func(r *row) any { return &r.MaxAttempts }},
		{Column: "available_at", Ptr: func(r *row) any { return &r.AvailableAt }},
		{Column: "started_at", Ptr: func(r *row) any { return &r.StartedAt }},
		{Column: "completed_at", Ptr: func(r *row) any { return &r.CompletedAt }},
		{Column: "locked_at", Ptr: func(r *row) any { return &r.LockedAt }},
		{Column: "lease_until", Ptr: func(r *row) any { return &r.LeaseUntil }},
		{Column: "worker_id", Ptr: func(r *row) any { return &r.WorkerID }},
		{Column: "cancel_requested_at", Ptr: func(r *row) any { return &r.CancelRequestedAt }},
		{Column: "last_error", Ptr: func(r *row) any { return &r.LastError }},
		{Column: "last_error_at", Ptr: func(r *row) any { return &r.LastErrorAt }},
		{Column: "idempotency_key", Ptr: func(r *row) any { return &r.IdempotencyKey }},
		{Column: "unique_key", Ptr: func(r *row) any { return &r.UniqueKey }},
		{Column: "active_unique_key", Ptr: func(r *row) any { return &r.ActiveUniqueKey }},
		{Column: "schema_fingerprint", Ptr: func(r *row) any { return &r.SchemaFingerprint }},
		{Column: "timeout_ms", Ptr: func(r *row) any { return &r.TimeoutMS }},
		{Column: "metadata", Ptr: func(r *row) any { return &r.Metadata }},
		{Column: "created_at", Ptr: func(r *row) any { return &r.CreatedAt }},
		{Column: "updated_at", Ptr: func(r *row) any { return &r.UpdatedAt }},
	},
}

// columnNames is the column list in spec order, used by the raw claim SQL so
// that scanning goes through the same field accessors as the Model.
var columnNames = func() []string {
	out := make([]string, len(spec.Fields))
	for i, f := range spec.Fields {
		out[i] = f.Column
	}
	return out
}()

// Typed columns. Nullable columns are typed by their base type; use SetNull to
// write NULL.
var col = struct {
	ID                lathe.Column[uuid.UUID]
	Type              lathe.Column[string]
	Version           lathe.Column[int32]
	Queue             lathe.Column[string]
	Payload           lathe.Column[lathe.JSON]
	Status            lathe.Column[string]
	Priority          lathe.Column[int32]
	Attempts          lathe.Column[int32]
	MaxAttempts       lathe.Column[int32]
	AvailableAt       lathe.Column[time.Time]
	StartedAt         lathe.Column[time.Time]
	CompletedAt       lathe.Column[time.Time]
	LockedAt          lathe.Column[time.Time]
	LeaseUntil        lathe.Column[time.Time]
	WorkerID          lathe.Column[string]
	CancelRequestedAt lathe.Column[time.Time]
	LastError         lathe.Column[string]
	LastErrorAt       lathe.Column[time.Time]
	IdempotencyKey    lathe.Column[string]
	UniqueKey         lathe.Column[string]
	ActiveUniqueKey   lathe.Column[string]
	SchemaFingerprint lathe.Column[string]
	TimeoutMS         lathe.Column[int64]
	Metadata          lathe.Column[lathe.JSON]
	CreatedAt         lathe.Column[time.Time]
	UpdatedAt         lathe.Column[time.Time]
}{
	ID:                lathe.NewColumn[uuid.UUID](TableName, "id"),
	Type:              lathe.NewColumn[string](TableName, "type"),
	Version:           lathe.NewColumn[int32](TableName, "version"),
	Queue:             lathe.NewColumn[string](TableName, "queue"),
	Payload:           lathe.NewColumn[lathe.JSON](TableName, "payload"),
	Status:            lathe.NewColumn[string](TableName, "status"),
	Priority:          lathe.NewColumn[int32](TableName, "priority"),
	Attempts:          lathe.NewColumn[int32](TableName, "attempts"),
	MaxAttempts:       lathe.NewColumn[int32](TableName, "max_attempts"),
	AvailableAt:       lathe.NewColumn[time.Time](TableName, "available_at"),
	StartedAt:         lathe.NewColumn[time.Time](TableName, "started_at"),
	CompletedAt:       lathe.NewColumn[time.Time](TableName, "completed_at"),
	LockedAt:          lathe.NewColumn[time.Time](TableName, "locked_at"),
	LeaseUntil:        lathe.NewColumn[time.Time](TableName, "lease_until"),
	WorkerID:          lathe.NewColumn[string](TableName, "worker_id"),
	CancelRequestedAt: lathe.NewColumn[time.Time](TableName, "cancel_requested_at"),
	LastError:         lathe.NewColumn[string](TableName, "last_error"),
	LastErrorAt:       lathe.NewColumn[time.Time](TableName, "last_error_at"),
	IdempotencyKey:    lathe.NewColumn[string](TableName, "idempotency_key"),
	UniqueKey:         lathe.NewColumn[string](TableName, "unique_key"),
	ActiveUniqueKey:   lathe.NewColumn[string](TableName, "active_unique_key"),
	SchemaFingerprint: lathe.NewColumn[string](TableName, "schema_fingerprint"),
	TimeoutMS:         lathe.NewColumn[int64](TableName, "timeout_ms"),
	Metadata:          lathe.NewColumn[lathe.JSON](TableName, "metadata"),
	CreatedAt:         lathe.NewColumn[time.Time](TableName, "created_at"),
	UpdatedAt:         lathe.NewColumn[time.Time](TableName, "updated_at"),
}

func tptr(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	u := t.UTC()
	return &u
}

func sptr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func tval(p *time.Time) time.Time {
	if p == nil {
		return time.Time{}
	}
	return p.UTC()
}

func sval(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

// parseID converts a work ID to the UUID the table stores. An ID that is not
// a UUID cannot exist in the table.
func parseID(id work.ID) (uuid.UUID, error) {
	u, err := uuid.Parse(string(id))
	if err != nil {
		return uuid.Nil, fmt.Errorf("%w: %q is not a valid work ID", work.ErrNotFound, string(id))
	}
	return u, nil
}

// fromRecord converts a record for insertion.
func fromRecord(rec *work.Record) (row, error) {
	id, err := uuid.Parse(string(rec.ID))
	if err != nil {
		return row{}, fmt.Errorf("work/database: record ID %q is not a UUID: %w", string(rec.ID), err)
	}
	if !json.Valid(rec.Payload) {
		return row{}, fmt.Errorf("%w: the database backend stores payloads in a JSON column and the payload is not valid JSON; use the JSON serializer", work.ErrUnsupported)
	}
	if !rec.Status.Valid() {
		return row{}, fmt.Errorf("work/database: invalid status %q", rec.Status)
	}
	r := row{
		ID: id, Type: rec.Type, Version: int32(rec.Version), Queue: rec.Queue,
		Payload: lathe.JSON(rec.Payload), Status: string(rec.Status),
		Priority: int32(rec.Priority), Attempts: int32(rec.Attempts), MaxAttempts: int32(rec.MaxAttempts),
		AvailableAt: rec.AvailableAt.UTC(),
		StartedAt:   tptr(rec.StartedAt), CompletedAt: tptr(rec.CompletedAt),
		LockedAt: tptr(rec.LockedAt), LeaseUntil: tptr(rec.LeaseUntil),
		WorkerID: sptr(rec.WorkerID), CancelRequestedAt: tptr(rec.CancelRequestedAt),
		LastError: sptr(rec.LastError), LastErrorAt: tptr(rec.LastErrorAt),
		IdempotencyKey: sptr(rec.IdempotencyKey), UniqueKey: sptr(rec.UniqueKey),
		ActiveUniqueKey: sptr(rec.ActiveUniqueKey), SchemaFingerprint: sptr(rec.SchemaFingerprint),
		CreatedAt: rec.CreatedAt.UTC(), UpdatedAt: rec.UpdatedAt.UTC(),
	}
	if rec.Timeout > 0 {
		ms := int64((rec.Timeout + time.Millisecond - 1) / time.Millisecond)
		r.TimeoutMS = &ms
	}
	if len(rec.Metadata) > 0 {
		b, err := json.Marshal(rec.Metadata)
		if err != nil {
			return row{}, fmt.Errorf("work/database: metadata: %w", err)
		}
		r.Metadata = lathe.JSON(b)
	}
	return r, nil
}

// toRecord converts a stored row.
func toRecord(r *row) *work.Record {
	rec := &work.Record{
		ID: work.ID(r.ID.String()), Type: r.Type, Version: int(r.Version), Queue: r.Queue,
		Payload: append([]byte(nil), r.Payload...), Status: work.Status(r.Status),
		Priority: int(r.Priority), Attempts: int(r.Attempts), MaxAttempts: int(r.MaxAttempts),
		AvailableAt: r.AvailableAt.UTC(),
		StartedAt:   tval(r.StartedAt), CompletedAt: tval(r.CompletedAt),
		LockedAt: tval(r.LockedAt), LeaseUntil: tval(r.LeaseUntil),
		WorkerID: sval(r.WorkerID), CancelRequestedAt: tval(r.CancelRequestedAt),
		LastError: sval(r.LastError), LastErrorAt: tval(r.LastErrorAt),
		IdempotencyKey: sval(r.IdempotencyKey), UniqueKey: sval(r.UniqueKey),
		ActiveUniqueKey: sval(r.ActiveUniqueKey), SchemaFingerprint: sval(r.SchemaFingerprint),
		CreatedAt: r.CreatedAt.UTC(), UpdatedAt: r.UpdatedAt.UTC(),
	}
	if r.TimeoutMS != nil {
		rec.Timeout = time.Duration(*r.TimeoutMS) * time.Millisecond
	}
	if len(r.Metadata) > 0 {
		var m map[string]string
		if err := json.Unmarshal(r.Metadata, &m); err == nil && len(m) > 0 {
			rec.Metadata = m
		}
	}
	return rec
}
