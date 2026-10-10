package work

import (
	"context"
	"time"
)

// Metadata describes the execution a handler's context belongs to. Retrieve
// it with FromContext. Handlers keep the plain Handle(context.Context) error
// signature; metadata is optional.
type Metadata struct {
	WorkID      ID
	Type        string
	Version     int
	Queue       string
	Attempt     int
	MaxAttempts int
	WorkerID    string

	// IdempotencyKey is the key to give external systems so duplicate
	// executions of at-least-once work are harmless. It is the dispatch-time
	// IdempotencyKey when one was set, and the work ID otherwise.
	IdempotencyKey string

	// Deadline is the execution deadline, zero when there is none.
	Deadline time.Time

	// EnqueuedAt is when the work was dispatched.
	EnqueuedAt time.Time

	// Dispatch holds the metadata attached at dispatch time.
	Dispatch map[string]string
}

type metadataKey struct{}

// FromContext returns execution metadata when ctx belongs to a handler run.
func FromContext(ctx context.Context) (Metadata, bool) {
	m, ok := ctx.Value(metadataKey{}).(Metadata)
	return m, ok
}

func withMetadata(ctx context.Context, m Metadata) context.Context {
	return context.WithValue(ctx, metadataKey{}, m)
}

type runtimeKey struct{}

// NewContext attaches rt to ctx so the package-level Dispatch functions use
// it. This keeps application code free of global state.
func NewContext(ctx context.Context, rt *Runtime) context.Context {
	return context.WithValue(ctx, runtimeKey{}, rt)
}

// RuntimeFromContext returns the Runtime attached with NewContext.
func RuntimeFromContext(ctx context.Context) (*Runtime, bool) {
	rt, ok := ctx.Value(runtimeKey{}).(*Runtime)
	return rt, ok && rt != nil
}
