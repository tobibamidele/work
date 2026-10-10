package work

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tobibamidele/work/internal/id"
)

// QueueConfig configures one queue a worker serves.
type QueueConfig struct {
	// Name of the queue.
	Name string
	// Concurrency is the number of handlers that may run at once for this
	// queue. Zero means WorkerConfig.Concurrency, or 10 when that is also
	// zero.
	Concurrency int
}

// WorkerConfig configures a Worker. Every field is optional.
//
// Concurrency precedence is deterministic: each queue has its own
// Concurrency; when WorkerConfig.Concurrency is also set it is an upper bound
// on the total number of handlers running across all queues, and it is the
// default for queues that set none.
type WorkerConfig struct {
	// Queues the worker claims from. Default: one queue named "default".
	// A worker never claims from a queue that is not listed.
	Queues []QueueConfig

	// Concurrency: see the precedence rules above.
	Concurrency int

	// LeaseDuration is how long a claimed item stays owned without a
	// heartbeat. Default 2m. Do not make it very short.
	LeaseDuration time.Duration

	// HeartbeatInterval is how often leases are extended while handlers run.
	// Default LeaseDuration/3. Cancellation requests for running work are
	// noticed at this cadence. It must be shorter than LeaseDuration.
	HeartbeatInterval time.Duration

	// PollInterval is the wait after finding no work. Default 500ms.
	PollInterval time.Duration

	// ShutdownTimeout is the grace period for in-flight handlers after the
	// Run context is cancelled. Default 30s.
	ShutdownTimeout time.Duration

	// ReapInterval is how often expired work that cannot be claimed again is
	// settled. Default 30s.
	ReapInterval time.Duration

	// WorkerID identifies this worker as lease owner. Default: generated,
	// unique per Worker.
	WorkerID string

	// Backoff is the retry policy for work whose definition has none.
	// Default ExponentialBackoff{}.
	Backoff Backoff

	// Clock is the time source. Default SystemClock.
	Clock Clock

	// Observer receives lifecycle events.
	Observer Observer

	// Propagator extracts trace context stored at dispatch.
	Propagator Propagator

	// Logger receives operational logs (backend errors, lease loss). Default
	// slog.Default().
	Logger *slog.Logger

	// Metrics, when set, receives the queue depth gauges (work_queue_depth,
	// work_available, work_running) every StatsInterval.
	Metrics MetricsSink

	// StatsInterval is the gauge reporting cadence. Default 15s.
	StatsInterval time.Duration

	// DebugPayloads opts in to logging payloads at DEBUG level. Payloads may
	// contain secrets or personal data; leave this off in production.
	DebugPayloads bool
}

// WorkerOption configures a Worker.
type WorkerOption func(*Worker)

// Use installs middleware around handler execution. The first is outermost:
// Use(A, B, C) runs A, B, C, handler, C, B, A.
func Use(mws ...Middleware) WorkerOption {
	return func(w *Worker) { w.mws = append(w.mws, mws...) }
}

// Worker claims durable work from a Backend and executes it with the handlers
// in a Registry.
type Worker struct {
	backend  Backend
	registry *Registry
	cfg      WorkerConfig
	mws      []Middleware
	queues   []*queueRunner
	gsem     chan struct{}
	started  atomic.Bool

	execBase context.Context
	execWG   sync.WaitGroup
	types    []string
}

type queueRunner struct {
	name string
	conc int
	sem  chan struct{}
	wake chan struct{}
}

// NewWorker validates the configuration and returns a Worker. Configuration
// errors (invalid or duplicate queues, a heartbeat longer than the lease, a
// nil backend or registry) are fatal and returned here, at startup.
func NewWorker(backend Backend, registry *Registry, cfg WorkerConfig, opts ...WorkerOption) (*Worker, error) {
	if backend == nil {
		return nil, configError("backend must not be nil")
	}
	if registry == nil {
		return nil, configError("registry must not be nil")
	}
	if cfg.Concurrency < 0 {
		return nil, configError("concurrency must not be negative")
	}
	if len(cfg.Queues) == 0 {
		cfg.Queues = []QueueConfig{{Name: DefaultQueueName}}
	}
	if cfg.LeaseDuration < 0 || cfg.HeartbeatInterval < 0 || cfg.PollInterval < 0 || cfg.ShutdownTimeout < 0 || cfg.ReapInterval < 0 || cfg.StatsInterval < 0 {
		return nil, configError("durations must not be negative")
	}
	if cfg.LeaseDuration == 0 {
		cfg.LeaseDuration = 2 * time.Minute
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = cfg.LeaseDuration / 3
	}
	if cfg.HeartbeatInterval >= cfg.LeaseDuration {
		return nil, configError("heartbeat interval (%s) must be shorter than the lease duration (%s)", cfg.HeartbeatInterval, cfg.LeaseDuration)
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 500 * time.Millisecond
	}
	if cfg.ShutdownTimeout == 0 {
		cfg.ShutdownTimeout = 30 * time.Second
	}
	if cfg.ReapInterval == 0 {
		cfg.ReapInterval = 30 * time.Second
	}
	if cfg.StatsInterval == 0 {
		cfg.StatsInterval = 15 * time.Second
	}
	if cfg.WorkerID == "" {
		cfg.WorkerID = id.Worker()
	}
	if cfg.Backoff == nil {
		cfg.Backoff = ExponentialBackoff{}
	}
	if cfg.Clock == nil {
		cfg.Clock = SystemClock
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	w := &Worker{backend: backend, registry: registry, cfg: cfg}
	defaultConc := cfg.Concurrency
	if defaultConc == 0 {
		defaultConc = 10
	}
	seen := map[string]bool{}
	for _, qc := range cfg.Queues {
		if !validQueueName(qc.Name) {
			return nil, configError("invalid queue name %q", qc.Name)
		}
		if seen[qc.Name] {
			return nil, configError("queue %q is configured twice", qc.Name)
		}
		seen[qc.Name] = true
		if qc.Concurrency < 0 {
			return nil, configError("queue %q: concurrency must not be negative", qc.Name)
		}
		c := qc.Concurrency
		if c == 0 {
			c = defaultConc
		}
		w.queues = append(w.queues, &queueRunner{
			name: qc.Name, conc: c,
			sem: make(chan struct{}, c), wake: make(chan struct{}, 1),
		})
	}
	if cfg.Concurrency > 0 {
		w.gsem = make(chan struct{}, cfg.Concurrency)
	}
	for _, o := range opts {
		o(w)
	}
	return w, nil
}

// Use appends middleware before Run starts. It panics if the worker is
// already running.
func (w *Worker) Use(mws ...Middleware) {
	if w.started.Load() {
		panic("work: Use called on a running worker")
	}
	w.mws = append(w.mws, mws...)
}

// WorkerID returns the lease owner identity of this worker.
func (w *Worker) WorkerID() string { return w.cfg.WorkerID }

// Run claims and executes work until ctx is cancelled. Handler errors never
// stop it; backend errors are retried with bounded exponential backoff.
//
// Shutdown, triggered by cancelling ctx (wire it to SIGINT/SIGTERM with
// signal.NotifyContext): stop claiming; wait up to ShutdownTimeout for
// running handlers; then cancel their contexts and give them a short window
// to return. Work still running after that keeps its lease and is recovered
// after the lease expires; Run then returns ErrShutdownTimeout. Handlers
// interrupted by the forced cancellation are released back to the queue
// without consuming an attempt.
func (w *Worker) Run(ctx context.Context) error {
	if !w.started.CompareAndSwap(false, true) {
		return configError("worker is already running or has run; create a new Worker")
	}
	w.registry.Freeze()
	w.types = w.registry.Names()
	if len(w.types) == 0 {
		return configError("registry has no definitions; a worker would never claim anything")
	}

	execBase, abort := context.WithCancel(context.WithoutCancel(ctx))
	defer abort()
	w.execBase = execBase

	var loops sync.WaitGroup
	for _, q := range w.queues {
		loops.Add(1)
		go func(q *queueRunner) {
			defer loops.Done()
			w.queueLoop(ctx, q)
		}(q)
	}
	loops.Add(1)
	go func() { defer loops.Done(); w.reapLoop(ctx) }()
	if w.cfg.Metrics != nil {
		loops.Add(1)
		go func() { defer loops.Done(); w.statsLoop(ctx) }()
	}

	<-ctx.Done()
	loops.Wait() // nothing new is claimed from here on

	idle := make(chan struct{})
	go func() { w.execWG.Wait(); close(idle) }()

	grace := time.NewTimer(w.cfg.ShutdownTimeout)
	defer grace.Stop()
	select {
	case <-idle:
		return nil
	case <-grace.C:
	}
	abort() // ask handlers to stop through their contexts
	final := time.NewTimer(forcedReturnWindow)
	defer final.Stop()
	select {
	case <-idle:
		return nil
	case <-final.C:
		return ErrShutdownTimeout
	}
}

// forcedReturnWindow is how long handlers get to return after their contexts
// were cancelled at the end of the shutdown grace period.
const forcedReturnWindow = 5 * time.Second

func (w *Worker) acquire(q *queueRunner, max int) int {
	got := 0
	for got < max {
		if w.gsem != nil {
			select {
			case w.gsem <- struct{}{}:
			default:
				return got
			}
		}
		select {
		case q.sem <- struct{}{}:
			got++
		default:
			if w.gsem != nil {
				<-w.gsem
			}
			return got
		}
	}
	return got
}

func (w *Worker) releaseSlot(q *queueRunner) {
	<-q.sem
	if w.gsem != nil {
		<-w.gsem
	}
	for _, o := range w.queues {
		select {
		case o.wake <- struct{}{}:
		default:
		}
	}
}

func (w *Worker) queueLoop(ctx context.Context, q *queueRunner) {
	ib := newInfraBackoff()
	for ctx.Err() == nil {
		n := w.acquire(q, q.conc)
		if n == 0 {
			select {
			case <-q.wake:
			case <-ctx.Done():
			case <-time.After(w.cfg.PollInterval):
			}
			continue
		}
		recs, err := w.backend.Claim(ctx, ClaimOptions{
			Queues: []string{q.name}, Types: w.types, WorkerID: w.cfg.WorkerID,
			Now: w.cfg.Clock.Now().UTC(), LeaseDuration: w.cfg.LeaseDuration, Limit: n,
		})
		for i := len(recs); i < n; i++ {
			w.releaseSlot(q)
		}
		switch {
		case errors.Is(err, ErrNoWork):
			w.sleep(ctx, w.cfg.PollInterval)
		case err != nil:
			if ctx.Err() != nil {
				return
			}
			d := ib.next()
			w.cfg.Logger.Error("work: claim failed; backing off", "queue", q.name, "worker_id", w.cfg.WorkerID, "error", err, "backoff", d)
			w.sleep(ctx, d)
		default:
			ib.reset()
			for _, rec := range recs {
				w.execWG.Add(1)
				go func(rec *Record) {
					defer w.execWG.Done()
					defer w.releaseSlot(q)
					w.execute(rec)
				}(rec)
			}
		}
	}
}

// sleep waits d (±10% jitter) or until ctx is done.
func (w *Worker) sleep(ctx context.Context, d time.Duration) {
	d = time.Duration(float64(d) * (0.9 + 0.2*rand.Float64()))
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
	}
}

func (w *Worker) reapLoop(ctx context.Context) {
	ib := newInfraBackoff()
	wait := w.cfg.ReapInterval
	for {
		w.sleep(ctx, wait)
		if ctx.Err() != nil {
			return
		}
		wait = w.cfg.ReapInterval
		for {
			recs, err := w.backend.Reap(ctx, w.cfg.Clock.Now().UTC(), 100)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				wait = ib.next()
				w.cfg.Logger.Error("work: reaping expired work failed", "worker_id", w.cfg.WorkerID, "error", err)
				break
			}
			ib.reset()
			for _, rec := range recs {
				ev := Event{Name: EventFailed, WorkID: rec.ID, Type: rec.Type, Version: rec.Version, Queue: rec.Queue,
					Attempt: rec.Attempts, Time: w.cfg.Clock.Now().UTC(), Err: errors.New(rec.LastError)}
				if rec.Status == StatusCancelled {
					ev.Name, ev.Err = EventCancelled, nil
				}
				safeObserve(ctx, w.cfg.Observer, ev)
			}
			if len(recs) < 100 {
				break
			}
		}
	}
}

func (w *Worker) statsLoop(ctx context.Context) {
	for {
		stats, err := w.backend.Stats(ctx)
		if err == nil {
			for _, q := range w.queues {
				var avail, running int64
				for _, s := range stats {
					if s.Queue != q.name {
						continue
					}
					switch s.Status {
					case StatusAvailable:
						avail = s.Count
					case StatusRunning:
						running = s.Count
					}
				}
				l := Labels{"queue": q.name}
				w.cfg.Metrics.Gauge(MetricQueueDepth, l, float64(avail))
				w.cfg.Metrics.Gauge(MetricAvailable, l, float64(avail))
				w.cfg.Metrics.Gauge(MetricRunning, l, float64(running))
			}
		} else if ctx.Err() == nil {
			w.cfg.Logger.Warn("work: reading queue stats failed", "error", err)
		}
		w.sleep(ctx, w.cfg.StatsInterval)
		if ctx.Err() != nil {
			return
		}
	}
}

func (w *Worker) String() string { return fmt.Sprintf("work.Worker(%s)", w.cfg.WorkerID) }
