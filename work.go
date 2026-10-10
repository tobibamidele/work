package work

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"
)

// RuntimeConfig configures a Runtime. The zero value is usable.
type RuntimeConfig struct {
	// Clock is the time source. Default SystemClock.
	Clock Clock
	// Observer receives lifecycle events (enqueued). Default none.
	Observer Observer
	// Propagator injects trace context into dispatched work. Default none.
	Propagator Propagator
	// DefaultQueue is used when neither Dispatch nor the definition choose
	// one. Default "default".
	DefaultQueue string
	// DefaultMaxAttempts is the attempt limit when neither Dispatch nor the
	// definition choose one. Default 3.
	DefaultMaxAttempts int
}

// Runtime is the application-owned entry point for dispatching and
// inspecting work. It ties a Backend to a Registry. There is no global state:
// create one per application and pass it around, attach it to a context with
// NewContext, or install it once with SetDefault for the package-level
// convenience functions.
type Runtime struct {
	backend  Backend
	registry *Registry
	cfg      RuntimeConfig
	caps     Capabilities
}

// NewRuntime creates a Runtime. It returns an ErrConfig error for a nil
// backend or registry or invalid defaults.
func NewRuntime(backend Backend, registry *Registry, cfg RuntimeConfig) (*Runtime, error) {
	switch {
	case backend == nil:
		return nil, configError("backend must not be nil")
	case registry == nil:
		return nil, configError("registry must not be nil")
	case cfg.DefaultMaxAttempts < 0:
		return nil, configError("default max attempts must not be negative")
	case cfg.DefaultQueue != "" && !validQueueName(cfg.DefaultQueue):
		return nil, configError("invalid default queue %q", cfg.DefaultQueue)
	}
	if cfg.Clock == nil {
		cfg.Clock = SystemClock
	}
	if cfg.DefaultQueue == "" {
		cfg.DefaultQueue = DefaultQueueName
	}
	if cfg.DefaultMaxAttempts == 0 {
		cfg.DefaultMaxAttempts = 3
	}
	return &Runtime{backend: backend, registry: registry, cfg: cfg, caps: backend.Capabilities()}, nil
}

// Backend returns the backend.
func (rt *Runtime) Backend() Backend { return rt.backend }

// Registry returns the registry.
func (rt *Runtime) Registry() *Registry { return rt.registry }

// Clock returns the runtime's clock.
func (rt *Runtime) Clock() Clock { return rt.cfg.Clock }

// Dispatch validates payload, serializes the validated and normalized value
// and durably stores it. It returns only after the backend confirmed the
// record is persisted. Dispatch never executes work.
//
// An invalid payload is rejected before anything is persisted, with an error
// matching ErrInvalidPayload that wraps the *validate.ValidationError. A
// Unique collision returns an error matching ErrDuplicate together with the
// Ref of the active work.
func (rt *Runtime) Dispatch(ctx context.Context, payload any, opts ...DispatchOption) (Ref, error) {
	rec, err := rt.Prepare(ctx, payload, opts...)
	if err != nil {
		return Ref{}, err
	}
	if err := rt.backend.Enqueue(ctx, rec); err != nil {
		return duplicateRef(err), err
	}
	rt.Enqueued(ctx, rec, false)
	return refOf(rec), nil
}

// DispatchAt is Dispatch with the work becoming claimable at the given time.
func (rt *Runtime) DispatchAt(ctx context.Context, payload any, at time.Time, opts ...DispatchOption) (Ref, error) {
	return rt.Dispatch(ctx, payload, append(append([]DispatchOption(nil), opts...), At(at))...)
}

func duplicateRef(err error) Ref {
	if de, ok := asDuplicate(err); ok {
		return Ref{ID: de.ExistingID, Queue: de.Queue}
	}
	return Ref{}
}

// Prepare validates and serializes payload and builds the record Dispatch
// would persist, without persisting it. Backends that enqueue inside an
// application transaction (see package database) use it, then call Enqueued
// once the insert succeeded. Applications normally call Dispatch.
func (rt *Runtime) Prepare(ctx context.Context, payload any, opts ...DispatchOption) (*Record, error) {
	cfg, err := newDispatchConfig(opts)
	if err != nil {
		return nil, err
	}
	def, value, err := rt.registry.lookupValue(payload)
	if err != nil {
		return nil, err
	}
	data, err := def.encode(ctx, rt.registry.serializerFor(def), value)
	if err != nil {
		return nil, err
	}

	now := rt.cfg.Clock.Now().UTC()
	rec := &Record{
		ID:                ID(newID()),
		Type:              def.name,
		Version:           def.version,
		Queue:             firstNonEmpty(cfg.queue, def.queue, rt.cfg.DefaultQueue),
		Payload:           data,
		Status:            StatusAvailable,
		Priority:          def.priority,
		MaxAttempts:       rt.cfg.DefaultMaxAttempts,
		AvailableAt:       now,
		SchemaFingerprint: def.fingerprint,
		Timeout:           def.timeout,
		CreatedAt:         now,
		UpdatedAt:         now,
	}
	if def.maxAttempts > 0 {
		rec.MaxAttempts = def.maxAttempts
	}
	if cfg.hasMaxAttempts {
		rec.MaxAttempts = cfg.maxAttempts
	}
	if cfg.hasPriority {
		rec.Priority = cfg.priority
	}
	if cfg.hasTimeout {
		rec.Timeout = cfg.timeout
	}
	switch {
	case !cfg.at.IsZero():
		rec.AvailableAt = cfg.at
	case cfg.hasDelay:
		rec.AvailableAt = now.Add(cfg.delay)
	}
	if rec.AvailableAt.After(now) && !rt.caps.DelayedWork {
		return nil, unsupported(rt.caps, "delayed work")
	}
	if rec.Priority != 0 && !rt.caps.Priorities {
		return nil, unsupported(rt.caps, "priorities")
	}
	if cfg.uniquePayload {
		cfg.unique = payloadUniqueKey(def.name, def.version, data)
	}
	if cfg.unique != "" {
		if !rt.caps.StrongUniqueness {
			return nil, unsupported(rt.caps, "unique keys")
		}
		rec.UniqueKey = cfg.unique
		rec.ActiveUniqueKey = cfg.unique
	}
	rec.IdempotencyKey = cfg.idempotency
	if len(cfg.metadata) > 0 || rt.cfg.Propagator != nil {
		rec.Metadata = map[string]string{}
		for k, v := range cfg.metadata {
			rec.Metadata[k] = v
		}
		if rt.cfg.Propagator != nil {
			rt.cfg.Propagator.Inject(ctx, rec.Metadata)
		}
		if len(rec.Metadata) == 0 {
			rec.Metadata = nil
		}
	}
	return rec, nil
}

// Enqueued emits the work.enqueued event for a record persisted by an
// alternative path such as a transactional dispatch.
func (rt *Runtime) Enqueued(ctx context.Context, rec *Record, transactional bool) {
	safeObserve(ctx, rt.cfg.Observer, Event{
		Name: EventEnqueued, WorkID: rec.ID, Type: rec.Type, Version: rec.Version, Queue: rec.Queue,
		Time: rt.cfg.Clock.Now().UTC(), Transactional: transactional,
	})
}

func unsupported(c Capabilities, feature string) error {
	name := c.Name
	if name == "" {
		name = "this backend"
	}
	return fmt.Errorf("%w: %s does not support %s", ErrUnsupported, name, feature)
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// ---- package-level convenience ----

var defaultRuntime atomic.Pointer[Runtime]

// SetDefault installs rt as the process-wide fallback used by the
// package-level functions when the context carries no Runtime. It is a thin
// convenience around an application-owned Runtime, not a registry: pass nil
// to clear it. Prefer NewContext or calling the Runtime's methods.
func SetDefault(rt *Runtime) { defaultRuntime.Store(rt) }

// Default returns the runtime installed with SetDefault, or nil.
func Default() *Runtime { return defaultRuntime.Load() }

func resolve(ctx context.Context) (*Runtime, error) {
	if rt, ok := RuntimeFromContext(ctx); ok {
		return rt, nil
	}
	if rt := defaultRuntime.Load(); rt != nil {
		return rt, nil
	}
	return nil, ErrNoRuntime
}

// Dispatch dispatches payload on the Runtime attached to ctx (NewContext) or,
// failing that, the default Runtime (SetDefault). It is shorthand for
// Runtime.Dispatch.
func Dispatch(ctx context.Context, payload any, opts ...DispatchOption) (Ref, error) {
	rt, err := resolve(ctx)
	if err != nil {
		return Ref{}, err
	}
	return rt.Dispatch(ctx, payload, opts...)
}

// DispatchAt is Dispatch with a claim time.
func DispatchAt(ctx context.Context, payload any, at time.Time, opts ...DispatchOption) (Ref, error) {
	rt, err := resolve(ctx)
	if err != nil {
		return Ref{}, err
	}
	return rt.DispatchAt(ctx, payload, at, opts...)
}
