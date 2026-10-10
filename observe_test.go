package work_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tobibamidele/work"
)

type sink struct {
	mu       sync.Mutex
	counts   map[string]float64
	observed map[string][]float64
	gauges   map[string]float64
	labels   []work.Labels
}

func newSink() *sink {
	return &sink{counts: map[string]float64{}, observed: map[string][]float64{}, gauges: map[string]float64{}}
}

func (s *sink) Count(n string, l work.Labels, d float64) {
	s.mu.Lock()
	s.counts[n] += d
	s.labels = append(s.labels, l)
	s.mu.Unlock()
}
func (s *sink) Observe(n string, l work.Labels, v float64) {
	s.mu.Lock()
	s.observed[n] = append(s.observed[n], v)
	s.labels = append(s.labels, l)
	s.mu.Unlock()
}
func (s *sink) Gauge(n string, l work.Labels, v float64) {
	s.mu.Lock()
	s.gauges[n+"/"+l["queue"]] = v
	s.labels = append(s.labels, l)
	s.mu.Unlock()
}

func TestMetricsObserverAndMiddleware(t *testing.T) {
	s := newSink()
	e := newEnv(t, defPing("p", func(_ context.Context, p Ping) error {
		if p.N == 1 {
			return errors.New("fail")
		}
		return nil
	}))
	e.dispatch(Ping{N: 0})
	e.dispatch(Ping{N: 1}, work.MaxAttempts(2))
	cfg := work.WorkerConfig{Observer: work.MultiObserver(e.rec, work.MetricsObserver(s))}
	start(t, fastWorker(e, cfg, work.Use(work.MetricsMiddleware(s))))
	eventually(t, "done", func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.counts[work.MetricCompletedTotal] == 1 && s.counts[work.MetricFailedTotal] == 1
	})
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.counts[work.MetricStartedTotal] != 3 || s.counts[work.MetricRetriedTotal] != 1 {
		t.Fatalf("counts = %v", s.counts)
	}
	if len(s.observed[work.MetricDurationSeconds]) != 3 || len(s.observed[work.MetricQueueWaitSeconds]) != 3 {
		t.Fatalf("observed = %v", s.observed)
	}
	// metric labels are bounded: never IDs, payloads, emails
	for _, l := range s.labels {
		for k := range l {
			if k != "work_type" && k != "queue" && k != "status" {
				t.Fatalf("unbounded label %q", k)
			}
		}
	}
}

func TestQueueDepthGauges(t *testing.T) {
	s := newSink()
	e := newEnv(t, defPing("p", func(context.Context, Ping) error { return nil }))
	e.dispatch(Ping{N: 1}, work.Queue("other"), work.Delay(time.Hour))
	e.dispatch(Ping{N: 2}, work.Queue("other"), work.Delay(time.Hour))
	start(t, fastWorker(e, work.WorkerConfig{
		Queues: []work.QueueConfig{{Name: "other"}}, Metrics: s, StatsInterval: 10 * time.Millisecond,
	}))
	eventually(t, "gauge", func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.gauges[work.MetricAvailable+"/other"] == 2
	})
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.gauges[work.MetricQueueDepth+"/other"] != 2 || s.gauges[work.MetricRunning+"/other"] != 0 {
		t.Fatalf("gauges = %v", s.gauges)
	}
}

func TestLoggingNeverIncludesPayloads(t *testing.T) {
	var buf bytes.Buffer
	var mu sync.Mutex
	logger := slog.New(slog.NewTextHandler(&lockedWriter{&mu, &buf}, &slog.HandlerOptions{Level: slog.LevelDebug}))
	e := newEnv(t, invoiceDef(func(context.Context, Invoice) error { return errors.New("smtp down") }))
	e.dispatch(Invoice{InvoiceID: "SECRET-INVOICE-9", Email: "private@example.com"}, work.MaxAttempts(1))
	cfg := work.WorkerConfig{Observer: work.MultiObserver(e.rec, work.SlogObserver(logger)), Logger: logger}
	start(t, fastWorker(e, cfg, work.Use(work.LoggingMiddleware(logger))))
	eventually(t, "failed", func() bool { return e.rec.count(work.EventFailed) == 1 })
	time.Sleep(30 * time.Millisecond)
	mu.Lock()
	out := buf.String()
	mu.Unlock()
	for _, want := range []string{"work_id=", "work_type=send-invoice", "queue=default", "attempt=1", "worker_id=", "duration=", "status="} {
		if !strings.Contains(out, want) {
			t.Fatalf("log lacks %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "SECRET-INVOICE-9") || strings.Contains(out, "private@example.com") {
		t.Fatalf("payload leaked into logs:\n%s", out)
	}
}

func TestDebugPayloadLoggingIsOptIn(t *testing.T) {
	run := func(debug bool) string {
		var buf bytes.Buffer
		var mu sync.Mutex
		logger := slog.New(slog.NewTextHandler(&lockedWriter{&mu, &buf}, &slog.HandlerOptions{Level: slog.LevelDebug}))
		e := newEnv(t, invoiceDef(nil))
		ref := e.dispatch(Invoice{InvoiceID: "VISIBLE-IF-OPTED-IN", Email: "a@b.co"})
		stop := start(t, fastWorker(e, work.WorkerConfig{Logger: logger, DebugPayloads: debug}))
		e.waitStatus(ref.ID, work.StatusCompleted)
		_ = stop()
		mu.Lock()
		defer mu.Unlock()
		return buf.String()
	}
	if strings.Contains(run(false), "VISIBLE-IF-OPTED-IN") {
		t.Fatal("payloads must not be logged by default")
	}
	if !strings.Contains(run(true), "VISIBLE-IF-OPTED-IN") {
		t.Fatal("DebugPayloads should log payloads at debug level")
	}
}

type lockedWriter struct {
	mu *sync.Mutex
	b  *bytes.Buffer
}

func (w *lockedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.Write(p)
}

type spanRec struct {
	mu    sync.Mutex
	names []string
	attrs []map[string]string
	errs  []error
}
type testSpan struct {
	r *spanRec
}

func (s testSpan) End(err error) { s.r.mu.Lock(); s.r.errs = append(s.r.errs, err); s.r.mu.Unlock() }
func (r *spanRec) Start(ctx context.Context, name string, a map[string]string) (context.Context, work.Span) {
	r.mu.Lock()
	r.names = append(r.names, name)
	r.attrs = append(r.attrs, a)
	r.mu.Unlock()
	return ctx, testSpan{r}
}

func TestTracingMiddlewareAndPropagation(t *testing.T) {
	spans := &spanRec{}
	type ctxKey struct{}
	prop := tracePropagator{key: ctxKey{}}
	var seen atomicString
	reg := work.NewRegistry()
	reg.MustRegister(defPing("p", func(ctx context.Context, _ Ping) error {
		seen.set(ctx.Value(ctxKey{}))
		return errors.New("traced failure")
	}))
	b := newEnv(t).backend
	rt, _ := work.NewRuntime(b, reg, work.RuntimeConfig{Propagator: prop})
	ref, err := rt.Dispatch(context.WithValue(context.Background(), ctxKey{}, "trace-123"), Ping{N: 1}, work.MaxAttempts(1))
	if err != nil {
		t.Fatal(err)
	}
	w, err := work.NewWorker(b, reg, work.WorkerConfig{PollInterval: 5 * time.Millisecond, Propagator: prop}, work.Use(work.TracingMiddleware(spans)))
	if err != nil {
		t.Fatal(err)
	}
	start(t, w)
	eventually(t, "failed", func() bool { r, _ := b.Get(context.Background(), ref.ID); return r.Status == work.StatusFailed })
	if seen.get() != "trace-123" {
		t.Fatalf("handler context did not carry the dispatch trace: %v", seen.get())
	}
	spans.mu.Lock()
	defer spans.mu.Unlock()
	if len(spans.names) != 1 || spans.names[0] != "work.execute" || spans.errs[0] == nil {
		t.Fatalf("spans = %v, errs = %v", spans.names, spans.errs)
	}
	a := spans.attrs[0]
	if a["work.id"] != string(ref.ID) || a["work.type"] != "p" || a["work.queue"] != "default" || a["work.attempt"] != "1" || a["work.version"] != "1" {
		t.Fatalf("attrs = %v", a)
	}
	for k := range a {
		if strings.Contains(k, "payload") {
			t.Fatal("payload attribute")
		}
	}
}

type atomicString struct {
	mu sync.Mutex
	v  any
}

func (a *atomicString) set(v any) { a.mu.Lock(); a.v = v; a.mu.Unlock() }
func (a *atomicString) get() any  { a.mu.Lock(); defer a.mu.Unlock(); return a.v }

func TestObserverPanicsAreContained(t *testing.T) {
	e := newEnv(t, defPing("p", func(context.Context, Ping) error { return nil }))
	boom := work.ObserverFunc(func(context.Context, work.Event) { panic("observer bug") })
	ref := e.dispatch(Ping{N: 1})
	start(t, fastWorker(e, work.WorkerConfig{Observer: work.MultiObserver(boom, e.rec)}))
	e.waitStatus(ref.ID, work.StatusCompleted)
	eventually(t, "later observers still run", func() bool { return e.rec.count(work.EventCompleted) == 1 })
}
