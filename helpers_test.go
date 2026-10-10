package work_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tobibamidele/validate"
	"github.com/tobibamidele/work"
	"github.com/tobibamidele/work/memory"
)

type Invoice struct {
	InvoiceID string `json:"invoice_id"`
	Email     string `json:"email"`
}

var InvoiceSchema = validate.Object[Invoice](
	validate.Field("invoice_id", validate.String().Trim().Min(1).Max(64)),
	validate.Field("email", validate.String().Trim().Lower().Email()),
)

type Ping struct {
	N int `json:"n"`
}

var PingSchema = validate.Object[Ping](validate.Field("n", validate.Int().Min(0)))

// recorder collects lifecycle events.
type recorder struct {
	mu     sync.Mutex
	events []work.Event
}

func (r *recorder) Observe(_ context.Context, e work.Event) {
	r.mu.Lock()
	r.events = append(r.events, e)
	r.mu.Unlock()
}

func (r *recorder) names() []work.EventName {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]work.EventName, len(r.events))
	for i, e := range r.events {
		out[i] = e.Name
	}
	return out
}

func (r *recorder) count(n work.EventName) int {
	c := 0
	for _, x := range r.names() {
		if x == n {
			c++
		}
	}
	return c
}

func (r *recorder) last(n work.EventName) (work.Event, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := len(r.events) - 1; i >= 0; i-- {
		if r.events[i].Name == n {
			return r.events[i], true
		}
	}
	return work.Event{}, false
}

type env struct {
	t       *testing.T
	backend work.Backend
	mem     *memory.Backend
	reg     *work.Registry
	rt      *work.Runtime
	rec     *recorder
}

func newEnv(t *testing.T, defs ...work.Definition) *env {
	return newEnvWith(t, memory.New(), defs...)
}

func newEnvWith(t *testing.T, b work.Backend, defs ...work.Definition) *env {
	t.Helper()
	reg := work.NewRegistry()
	if err := reg.Register(defs...); err != nil {
		t.Fatalf("register: %v", err)
	}
	rec := &recorder{}
	rt, err := work.NewRuntime(b, reg, work.RuntimeConfig{Observer: rec})
	if err != nil {
		t.Fatal(err)
	}
	mem, _ := b.(*memory.Backend)
	return &env{t: t, backend: b, mem: mem, reg: reg, rt: rt, rec: rec}
}

func fastWorker(e *env, cfg work.WorkerConfig, opts ...work.WorkerOption) *work.Worker {
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 5 * time.Millisecond
	}
	if cfg.Backoff == nil {
		cfg.Backoff = work.FixedBackoff(5 * time.Millisecond)
	}
	if cfg.Observer == nil {
		cfg.Observer = e.rec
	}
	if cfg.ShutdownTimeout == 0 {
		cfg.ShutdownTimeout = 5 * time.Second
	}
	if cfg.ReapInterval == 0 {
		cfg.ReapInterval = 20 * time.Millisecond
	}
	w, err := work.NewWorker(e.backend, e.reg, cfg, opts...)
	if err != nil {
		e.t.Fatalf("NewWorker: %v", err)
	}
	return w
}

// start runs w and returns a stop function that cancels it and waits.
func start(t *testing.T, w *work.Worker) (stop func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	var once sync.Once
	var result error
	stop = func() error {
		once.Do(func() {
			cancel()
			select {
			case result = <-done:
			case <-time.After(15 * time.Second):
				t.Error("worker did not stop")
			}
		})
		return result
	}
	t.Cleanup(func() { _ = stop() })
	return stop
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func (e *env) get(id work.ID) *work.Record {
	e.t.Helper()
	r, err := e.backend.Get(context.Background(), id)
	if err != nil {
		e.t.Fatalf("get %s: %v", id, err)
	}
	return r
}

func (e *env) waitStatus(id work.ID, s work.Status) *work.Record {
	e.t.Helper()
	eventually(e.t, "status "+string(s), func() bool { return e.get(id).Status == s })
	return e.get(id)
}

func (e *env) dispatch(payload any, opts ...work.DispatchOption) work.Ref {
	e.t.Helper()
	ref, err := e.rt.Dispatch(context.Background(), payload, opts...)
	if err != nil {
		e.t.Fatalf("dispatch: %v", err)
	}
	return ref
}

func mustContain(t *testing.T, s string, subs ...string) {
	t.Helper()
	for _, sub := range subs {
		if !strings.Contains(s, sub) {
			t.Fatalf("%q does not contain %q", s, sub)
		}
	}
}

func wantIs(t *testing.T, err, target error) {
	t.Helper()
	if !errors.Is(err, target) {
		t.Fatalf("error = %v, want one matching %v", err, target)
	}
}

// defPing builds a Ping definition around fn.
func defPing(name string, fn func(ctx context.Context, p Ping) error, opts ...work.DefinitionOption) work.Definition {
	return work.Define(name, PingSchema, fn, opts...)
}

// wrapped lets tests intercept backend calls.
type wrapped struct {
	work.Backend
	claim  func(ctx context.Context, o work.ClaimOptions) ([]*work.Record, error)
	extend func(ctx context.Context, id work.ID, w string, until, now time.Time) (bool, error)
	finish func(ctx context.Context, id work.ID, w string, now time.Time) error
	caps   *work.Capabilities
}

func (w *wrapped) Claim(ctx context.Context, o work.ClaimOptions) ([]*work.Record, error) {
	if w.claim != nil {
		return w.claim(ctx, o)
	}
	return w.Backend.Claim(ctx, o)
}

func (w *wrapped) ExtendLease(ctx context.Context, id work.ID, wk string, until, now time.Time) (bool, error) {
	if w.extend != nil {
		return w.extend(ctx, id, wk, until, now)
	}
	return w.Backend.ExtendLease(ctx, id, wk, until, now)
}

func (w *wrapped) Complete(ctx context.Context, id work.ID, wk string, now time.Time) error {
	if w.finish != nil {
		return w.finish(ctx, id, wk, now)
	}
	return w.Backend.Complete(ctx, id, wk, now)
}

func (w *wrapped) Capabilities() work.Capabilities {
	if w.caps != nil {
		return *w.caps
	}
	return w.Backend.Capabilities()
}
