package work

import (
	"context"
	"log/slog"
	"time"
)

// Labels are metric labels. Only bounded labels are ever used by this
// package: work_type, queue and status. Never add work IDs, user IDs or
// emails.
type Labels map[string]string

// MetricsSink receives metrics. Adapt it to Prometheus, OpenTelemetry metrics,
// StatsD or anything else. Implementations must be safe for concurrent use.
type MetricsSink interface {
	// Count adds delta to a monotonically increasing counter.
	Count(name string, labels Labels, delta float64)
	// Observe records one sample of a distribution (a histogram).
	Observe(name string, labels Labels, value float64)
	// Gauge sets a gauge.
	Gauge(name string, labels Labels, value float64)
}

// Metric names.
const (
	MetricEnqueuedTotal  = "work_enqueued_total"
	MetricStartedTotal   = "work_started_total"
	MetricCompletedTotal = "work_completed_total"
	MetricFailedTotal    = "work_failed_total"
	MetricRetriedTotal   = "work_retried_total"
	MetricCancelledTotal = "work_cancelled_total"
	MetricLeaseLost      = "work_lease_lost_total"

	MetricDurationSeconds  = "work_duration_seconds"
	MetricQueueWaitSeconds = "work_queue_wait_seconds"

	MetricQueueDepth = "work_queue_depth"
	MetricRunning    = "work_running"
	MetricAvailable  = "work_available"
)

// MetricsObserver turns lifecycle events into the counters and the queue-wait
// distribution. Execution duration is recorded by MetricsMiddleware, and the
// depth gauges by the worker (see WorkerConfig.Metrics).
func MetricsObserver(sink MetricsSink) Observer {
	return ObserverFunc(func(_ context.Context, e Event) {
		l := Labels{"work_type": e.Type, "queue": e.Queue}
		switch e.Name {
		case EventEnqueued:
			sink.Count(MetricEnqueuedTotal, l, 1)
		case EventStarted:
			sink.Count(MetricStartedTotal, l, 1)
			sink.Observe(MetricQueueWaitSeconds, l, e.QueueWait.Seconds())
		case EventCompleted:
			sink.Count(MetricCompletedTotal, l, 1)
		case EventFailed:
			sink.Count(MetricFailedTotal, l, 1)
		case EventRetrying:
			sink.Count(MetricRetriedTotal, l, 1)
		case EventCancelled:
			sink.Count(MetricCancelledTotal, l, 1)
		case EventLeaseLost:
			sink.Count(MetricLeaseLost, l, 1)
		}
	})
}

// SlogObserver logs lifecycle events with the default fields: work_id,
// work_type, queue, attempt, worker_id, duration and status. It never logs
// payloads.
func SlogObserver(logger *slog.Logger) Observer {
	if logger == nil {
		logger = slog.Default()
	}
	return ObserverFunc(func(ctx context.Context, e Event) {
		level := slog.LevelInfo
		switch e.Name {
		case EventFailed, EventLeaseLost:
			level = slog.LevelWarn
		case EventRetrying:
			level = slog.LevelWarn
		case EventEnqueued, EventStarted:
			level = slog.LevelDebug
		}
		attrs := []slog.Attr{
			slog.String("work_id", string(e.WorkID)),
			slog.String("work_type", e.Type),
			slog.Int("version", e.Version),
			slog.String("queue", e.Queue),
			slog.Int("attempt", e.Attempt),
			slog.String("worker_id", e.WorkerID),
			slog.Duration("duration", e.Duration),
			slog.String("status", string(e.Name)),
		}
		if !e.RetryAt.IsZero() {
			attrs = append(attrs, slog.Time("retry_at", e.RetryAt))
		}
		if e.Err != nil {
			attrs = append(attrs, slog.String("error", truncateUTF8(e.Err.Error(), 1024)))
		}
		logger.LogAttrs(ctx, level, string(e.Name), attrs...)
	})
}

// LoggingMiddleware logs the execution of each attempt: the default fields
// work_id, work_type, queue, attempt, worker_id, duration and status. It never
// logs payloads.
func LoggingMiddleware(logger *slog.Logger) Middleware {
	if logger == nil {
		logger = slog.Default()
	}
	return func(next Handler) Handler {
		return func(ctx context.Context) error {
			m, _ := FromContext(ctx)
			base := []any{
				"work_id", string(m.WorkID), "work_type", m.Type, "queue", m.Queue,
				"attempt", m.Attempt, "worker_id", m.WorkerID,
			}
			logger.DebugContext(ctx, "work handler started", base...)
			start := time.Now()
			err := next(ctx)
			attrs := append(base, "duration", time.Since(start))
			if err != nil {
				logger.WarnContext(ctx, "work handler failed", append(attrs, "status", "error", "error", truncateUTF8(err.Error(), 1024))...)
				return err
			}
			logger.InfoContext(ctx, "work handler finished", append(attrs, "status", "ok")...)
			return nil
		}
	}
}

// MetricsMiddleware records the handler execution time in
// work_duration_seconds, labelled work_type, queue and status (ok or error).
func MetricsMiddleware(sink MetricsSink) Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context) error {
			m, _ := FromContext(ctx)
			start := time.Now()
			err := next(ctx)
			status := "ok"
			if err != nil {
				status = "error"
			}
			sink.Observe(MetricDurationSeconds, Labels{"work_type": m.Type, "queue": m.Queue, "status": status}, time.Since(start).Seconds())
			return err
		}
	}
}

// Tracer starts spans. Adapt OpenTelemetry (or any tracer) to it; the core
// package takes no tracing dependency.
type Tracer interface {
	Start(ctx context.Context, name string, attributes map[string]string) (context.Context, Span)
}

// Span is one trace span.
type Span interface {
	// End finishes the span, recording err as its error when non-nil.
	End(err error)
}

// Propagator carries trace context from dispatch to execution through the
// record's metadata. Adapt OpenTelemetry's propagators to it.
type Propagator interface {
	// Inject writes the context's trace identifiers into carrier.
	Inject(ctx context.Context, carrier map[string]string)
	// Extract returns a context carrying the identifiers found in carrier.
	Extract(ctx context.Context, carrier map[string]string) context.Context
}

// TracingMiddleware wraps each attempt in a "work.execute" span with the
// attributes work.id, work.type, work.version, work.queue, work.attempt and
// work.worker_id. Payloads are never attached.
func TracingMiddleware(t Tracer) Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context) error {
			m, _ := FromContext(ctx)
			ctx, span := t.Start(ctx, "work.execute", map[string]string{
				"work.id":        string(m.WorkID),
				"work.type":      m.Type,
				"work.version":   itoa(m.Version),
				"work.queue":     m.Queue,
				"work.attempt":   itoa(m.Attempt),
				"work.worker_id": m.WorkerID,
			})
			err := next(ctx)
			span.End(err)
			return err
		}
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
