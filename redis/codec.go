package redis

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/tobibamidele/work"
)

func micros(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UTC().UnixMicro()
}

func fromMicros(n int64) time.Time {
	if n == 0 {
		return time.Time{}
	}
	return time.UnixMicro(n).UTC()
}

func i64(n int64) string { return strconv.FormatInt(n, 10) }

// recordFields flattens a record into the name/value pairs of its hash.
func recordFields(rec *work.Record) ([]any, error) {
	if rec.ID == "" {
		return nil, fmt.Errorf("work/redis: record has no ID")
	}
	if !rec.Status.Valid() {
		return nil, fmt.Errorf("work/redis: invalid status %q", rec.Status)
	}
	meta := ""
	if len(rec.Metadata) > 0 {
		b, err := json.Marshal(rec.Metadata)
		if err != nil {
			return nil, fmt.Errorf("work/redis: metadata: %w", err)
		}
		meta = string(b)
	}
	var timeoutMS int64
	if rec.Timeout > 0 {
		timeoutMS = int64((rec.Timeout + time.Millisecond - 1) / time.Millisecond)
	}
	return []any{
		"id", string(rec.ID),
		"type", rec.Type,
		"version", strconv.Itoa(rec.Version),
		"queue", rec.Queue,
		"payload", string(rec.Payload),
		"status", string(rec.Status),
		"priority", strconv.Itoa(rec.Priority),
		"attempts", strconv.Itoa(rec.Attempts),
		"max_attempts", strconv.Itoa(rec.MaxAttempts),
		"available_at", i64(micros(rec.AvailableAt)),
		"started_at", i64(micros(rec.StartedAt)),
		"completed_at", i64(micros(rec.CompletedAt)),
		"locked_at", i64(micros(rec.LockedAt)),
		"lease_until", i64(micros(rec.LeaseUntil)),
		"worker_id", rec.WorkerID,
		"cancel_requested_at", i64(micros(rec.CancelRequestedAt)),
		"last_error", rec.LastError,
		"last_error_at", i64(micros(rec.LastErrorAt)),
		"idempotency_key", rec.IdempotencyKey,
		"unique_key", rec.UniqueKey,
		"active_unique_key", rec.ActiveUniqueKey,
		"schema_fingerprint", rec.SchemaFingerprint,
		"timeout_ms", i64(timeoutMS),
		"metadata", meta,
		"created_at", i64(micros(rec.CreatedAt)),
		"updated_at", i64(micros(rec.UpdatedAt)),
	}, nil
}

// recordFromReply converts a flat HGETALL reply to a record.
func recordFromReply(reply any) (*work.Record, error) {
	flat, ok := reply.([]any)
	if !ok || len(flat) == 0 || len(flat)%2 != 0 {
		return nil, fmt.Errorf("work/redis: unexpected record reply %T", reply)
	}
	h := make(map[string]string, len(flat)/2)
	for i := 0; i < len(flat); i += 2 {
		k, _ := flat[i].(string)
		v, _ := flat[i+1].(string)
		h[k] = v
	}
	return recordFromHash(h), nil
}

func recordFromHash(h map[string]string) *work.Record {
	atoi := func(k string) int { n, _ := strconv.Atoi(h[k]); return n }
	atoi64 := func(k string) int64 { n, _ := strconv.ParseInt(h[k], 10, 64); return n }
	rec := &work.Record{
		ID:                work.ID(h["id"]),
		Type:              h["type"],
		Version:           atoi("version"),
		Queue:             h["queue"],
		Payload:           []byte(h["payload"]),
		Status:            work.Status(h["status"]),
		Priority:          atoi("priority"),
		Attempts:          atoi("attempts"),
		MaxAttempts:       atoi("max_attempts"),
		AvailableAt:       fromMicros(atoi64("available_at")),
		StartedAt:         fromMicros(atoi64("started_at")),
		CompletedAt:       fromMicros(atoi64("completed_at")),
		LockedAt:          fromMicros(atoi64("locked_at")),
		LeaseUntil:        fromMicros(atoi64("lease_until")),
		WorkerID:          h["worker_id"],
		CancelRequestedAt: fromMicros(atoi64("cancel_requested_at")),
		LastError:         h["last_error"],
		LastErrorAt:       fromMicros(atoi64("last_error_at")),
		IdempotencyKey:    h["idempotency_key"],
		UniqueKey:         h["unique_key"],
		ActiveUniqueKey:   h["active_unique_key"],
		SchemaFingerprint: h["schema_fingerprint"],
		Timeout:           time.Duration(atoi64("timeout_ms")) * time.Millisecond,
		CreatedAt:         fromMicros(atoi64("created_at")),
		UpdatedAt:         fromMicros(atoi64("updated_at")),
	}
	if m := h["metadata"]; m != "" {
		var md map[string]string
		if json.Unmarshal([]byte(m), &md) == nil && len(md) > 0 {
			rec.Metadata = md
		}
	}
	return rec
}
