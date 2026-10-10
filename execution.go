package work

import (
	"context"
	"errors"
	"time"
)

// errCancelRequested is the cancellation cause delivered to a handler when
// its work was cancelled while running.
var errCancelRequested = errors.New("work: cancellation requested")

// execute runs one claimed record from decode to settlement.
func (w *Worker) execute(rec *Record) {
	now := w.cfg.Clock.Now().UTC()
	base := Event{
		WorkID: rec.ID, Type: rec.Type, Version: rec.Version, Queue: rec.Queue,
		Attempt: rec.Attempts, WorkerID: w.cfg.WorkerID,
	}
	emit := func(e Event) {
		e.WorkID, e.Type, e.Version, e.Queue, e.Attempt, e.WorkerID = base.WorkID, base.Type, base.Version, base.Queue, base.Attempt, base.WorkerID
		e.Time = w.cfg.Clock.Now().UTC()
		safeObserve(w.execBase, w.cfg.Observer, e)
	}
	wait := now.Sub(rec.AvailableAt)
	if wait < 0 {
		wait = 0
	}
	emit(Event{Name: EventStarted, QueueWait: wait})

	// 1. resolve and validate. A payload that cannot be validated fails
	// permanently: retrying malformed data would never succeed.
	def, err := w.registry.lookup(rec.Type, rec.Version)
	if err != nil {
		w.fail(rec, err, emit, 0)
		return
	}
	if def.fingerprint != "" && rec.SchemaFingerprint != "" && def.fingerprint != rec.SchemaFingerprint {
		w.cfg.Logger.Warn("work: schema fingerprint differs from the one stored at dispatch; version is the compatibility boundary",
			"work_id", string(rec.ID), "work_type", rec.Type, "version", rec.Version,
			"stored_fingerprint", rec.SchemaFingerprint, "current_fingerprint", def.fingerprint)
	}
	if w.cfg.DebugPayloads {
		w.cfg.Logger.Debug("work payload", "work_id", string(rec.ID), "work_type", rec.Type, "payload", string(rec.Payload))
	}
	decodeCtx, decodeCancel := context.WithTimeout(w.execBase, 30*time.Second)
	bound, err := def.decode(decodeCtx, w.registry.serializerFor(def), rec.Payload)
	decodeCancel()
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			w.releaseWork(rec, emit, err)
			return
		}
		w.fail(rec, err, emit, 0)
		return
	}

	// 2. handler context: metadata, trace context, timeout, cancellation.
	hctx, hcancel := context.WithCancelCause(w.execBase)
	defer hcancel(nil)
	if w.cfg.Propagator != nil && len(rec.Metadata) > 0 {
		hctx = w.cfg.Propagator.Extract(hctx, rec.Metadata)
	}
	meta := Metadata{
		WorkID: rec.ID, Type: rec.Type, Version: rec.Version, Queue: rec.Queue,
		Attempt: rec.Attempts, MaxAttempts: rec.MaxAttempts, WorkerID: w.cfg.WorkerID,
		IdempotencyKey: rec.IdempotencyKey, EnqueuedAt: rec.CreatedAt, Dispatch: rec.Metadata,
	}
	if meta.IdempotencyKey == "" {
		meta.IdempotencyKey = string(rec.ID)
	}
	if rec.Timeout > 0 {
		var tcancel context.CancelFunc
		hctx, tcancel = context.WithTimeout(hctx, rec.Timeout)
		defer tcancel()
		meta.Deadline, _ = hctx.Deadline()
	}
	hctx = withMetadata(hctx, meta)

	// 3. heartbeat for the duration of the handler.
	hb := &heartbeat{w: w, rec: rec, cancelHandler: hcancel, emit: emit}
	hb.start(w.execBase)

	started := time.Now()
	handler := Chain(bound, w.mws...)
	herr := safeCall(hctx, handler)
	elapsed := time.Since(started)

	hb.stop() // the heartbeat must be fully stopped before settling

	aborted := w.execBase.Err() != nil

	// 4. settle.
	switch {
	case hb.lost.Load():
		w.cfg.Logger.Warn("work: lease lost during execution; not acknowledging",
			"work_id", string(rec.ID), "work_type", rec.Type, "attempt", rec.Attempts, "worker_id", w.cfg.WorkerID)
	case herr == nil:
		w.settleOwnership(rec, emit, func(ctx context.Context) error {
			return w.backend.Complete(ctx, rec.ID, w.cfg.WorkerID, w.cfg.Clock.Now().UTC())
		}, Event{Name: EventCompleted, Duration: elapsed})
	case hb.cancelRequested.Load():
		cause := w.execError(rec, herr)
		w.settleOwnership(rec, emit, func(ctx context.Context) error {
			return w.backend.FinishCancel(ctx, rec.ID, w.cfg.WorkerID, cause, w.cfg.Clock.Now().UTC())
		}, Event{Name: EventCancelled, Duration: elapsed, Err: cause})
	case aborted:
		w.releaseWork(rec, emit, herr)
	default:
		w.retryOrFail(rec, def, herr, elapsed, emit)
	}
}

// safeCall runs h and converts a panic into a *PanicError, so a panicking
// handler never crashes the worker.
func safeCall(ctx context.Context, h Handler) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = newPanicError(r)
		}
	}()
	return h(ctx)
}

func (w *Worker) execError(rec *Record, err error) error {
	return &ExecutionError{
		WorkID: rec.ID, Type: rec.Type, Version: rec.Version, Queue: rec.Queue,
		Attempt: rec.Attempts, WorkerID: w.cfg.WorkerID, Err: err,
	}
}

func (w *Worker) retryOrFail(rec *Record, def *definition, herr error, elapsed time.Duration, emit func(Event)) {
	backoff := w.cfg.Backoff
	if def.backoff != nil {
		backoff = def.backoff
	}
	cause := w.execError(rec, herr)
	dec, delay := retryDecision(herr, rec.Attempts, rec.MaxAttempts, backoff)
	if dec == decideFail {
		w.fail(rec, cause, emit, elapsed)
		return
	}
	next := w.cfg.Clock.Now().UTC().Add(delay)
	w.settleOwnership(rec, emit, func(ctx context.Context) error {
		return w.backend.Retry(ctx, rec.ID, w.cfg.WorkerID, next, cause, w.cfg.Clock.Now().UTC())
	}, Event{Name: EventRetrying, Duration: elapsed, RetryAt: next, Err: cause})
}

// fail settles the work as permanently failed.
func (w *Worker) fail(rec *Record, err error, emit func(Event), elapsed time.Duration) {
	var cause error = err
	var ee *ExecutionError
	if !errors.As(err, &ee) {
		cause = w.execError(rec, err)
	}
	w.settleOwnership(rec, emit, func(ctx context.Context) error {
		return w.backend.Fail(ctx, rec.ID, w.cfg.WorkerID, cause, w.cfg.Clock.Now().UTC())
	}, Event{Name: EventFailed, Duration: elapsed, Err: cause})
}

// release hands interrupted work back without consuming an attempt.
func (w *Worker) releaseWork(rec *Record, emit func(Event), cause error) {
	w.settleOwnership(rec, emit, func(ctx context.Context) error {
		return w.backend.Release(ctx, rec.ID, w.cfg.WorkerID, w.cfg.Clock.Now().UTC())
	}, Event{Name: EventRetrying, RetryAt: w.cfg.Clock.Now().UTC(), Err: w.execError(rec, cause)})
}

// settleOwnership performs a backend settlement, retrying infrastructure
// errors with bounded backoff for at most one lease duration (after that
// another worker may legitimately own the work). An ownership error means the
// work is no longer ours: it is reported, never forced.
func (w *Worker) settleOwnership(rec *Record, emit func(Event), op func(context.Context) error, ok Event) {
	ctx := w.execBase
	// Settlement must survive shutdown: use a context that is never cancelled.
	ctx = context.WithoutCancel(ctx)
	ib := newInfraBackoff()
	deadline := time.Now().Add(w.cfg.LeaseDuration)
	for {
		err := op(ctx)
		switch {
		case err == nil:
			emit(ok)
			return
		case errors.Is(err, ErrLeaseLost):
			emit(Event{Name: EventLeaseLost, Err: err})
			w.cfg.Logger.Warn("work: lease lost before settlement",
				"work_id", string(rec.ID), "work_type", rec.Type, "attempt", rec.Attempts, "worker_id", w.cfg.WorkerID)
			return
		case IsOwnership(err) || errors.Is(err, ErrInvalidState):
			w.cfg.Logger.Warn("work: work changed state while it was running; not acknowledging",
				"work_id", string(rec.ID), "work_type", rec.Type, "error", err)
			return
		}
		if time.Now().After(deadline) {
			w.cfg.Logger.Error("work: could not settle work before its lease expired; it will be recovered and run again",
				"work_id", string(rec.ID), "work_type", rec.Type, "error", err)
			return
		}
		d := ib.next()
		w.cfg.Logger.Warn("work: settling work failed; retrying", "work_id", string(rec.ID), "error", err, "backoff", d)
		time.Sleep(d)
	}
}
