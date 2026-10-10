package work

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// heartbeat extends the lease of one running record until stopped. It also
// delivers cancellation requests and ownership loss to the handler.
type heartbeat struct {
	w             *Worker
	rec           *Record
	cancelHandler context.CancelCauseFunc
	emit          func(Event)

	lost            atomic.Bool
	cancelRequested atomic.Bool

	stopOnce sync.Once
	cancel   context.CancelFunc
	done     chan struct{}
}

func (h *heartbeat) start(parent context.Context) {
	ctx, cancel := context.WithCancel(context.WithoutCancel(parent))
	h.cancel = cancel
	h.done = make(chan struct{})
	go h.loop(ctx)
}

// stop ends the heartbeat and waits for its goroutine, so no extension can
// race with settlement.
func (h *heartbeat) stop() {
	h.stopOnce.Do(func() {
		h.cancel()
		<-h.done
	})
}

func (h *heartbeat) loop(ctx context.Context) {
	defer close(h.done)
	t := time.NewTicker(h.w.cfg.HeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		h.beat(ctx)
	}
}

func (h *heartbeat) beat(ctx context.Context) {
	cfg := &h.w.cfg
	now := cfg.Clock.Now().UTC()
	cancelReq, err := h.w.backend.ExtendLease(ctx, h.rec.ID, cfg.WorkerID, now.Add(cfg.LeaseDuration), now)
	switch {
	case err == nil:
		if cancelReq && h.cancelRequested.CompareAndSwap(false, true) {
			h.cancelHandler(errCancelRequested)
		}
	case ctx.Err() != nil:
		// stopping
	case errors.Is(err, ErrLeaseLost) || errors.Is(err, ErrNotFound):
		// Ownership is proven lost: the record is no longer running under
		// this worker. Only now is the handler told to stop.
		if h.lost.CompareAndSwap(false, true) {
			h.emit(Event{Name: EventLeaseLost, Err: ErrLeaseLost})
			h.cancelHandler(ErrLeaseLost)
		}
	case errors.Is(err, ErrCancelled):
		if h.cancelRequested.CompareAndSwap(false, true) {
			h.cancelHandler(errCancelRequested)
		}
	default:
		// Temporary failure: ownership is not disproved, so the handler keeps
		// running. Log it; the next beat tries again.
		cfg.Logger.Warn("work: lease heartbeat failed", "work_id", string(h.rec.ID), "worker_id", cfg.WorkerID, "error", err)
	}
}
