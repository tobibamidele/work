package work

import (
	"context"
	"errors"
)

// Get returns one record. The record includes its payload: authenticate and
// authorize callers before exposing it, and never rely on the ID alone as
// access control in a multi-tenant application.
func (rt *Runtime) Get(ctx context.Context, id ID) (*Record, error) {
	return rt.backend.Get(ctx, id)
}

// List returns records matching f, newest first by default.
func (rt *Runtime) List(ctx context.Context, f ListFilter) ([]*Record, error) {
	return rt.backend.List(ctx, f)
}

// Cancel cancels work. Available work is cancelled immediately. Running work
// cannot be killed: its handler's context is cancelled at the owning worker's
// next heartbeat and the final state is settled once the handler returns
// (completed if it finished anyway, cancelled otherwise).
func (rt *Runtime) Cancel(ctx context.Context, id ID) (CancelOutcome, error) {
	if !rt.caps.Cancellation {
		return 0, unsupported(rt.caps, "cancellation")
	}
	out, err := rt.backend.Cancel(ctx, id, rt.cfg.Clock.Now().UTC())
	if err != nil {
		return 0, err
	}
	if out == Cancelled {
		if rec, gerr := rt.backend.Get(ctx, id); gerr == nil {
			safeObserve(ctx, rt.cfg.Observer, Event{
				Name: EventCancelled, WorkID: rec.ID, Type: rec.Type, Version: rec.Version,
				Queue: rec.Queue, Attempt: rec.Attempts, Time: rt.cfg.Clock.Now().UTC(),
			})
		}
	}
	return out, nil
}

// Retry starts a new execution cycle for failed or cancelled work: attempts
// reset to zero and the work becomes available now. The previous error text
// is preserved on the record. Work in any other state returns ErrInvalidState.
func (rt *Runtime) Retry(ctx context.Context, id ID) error {
	if err := rt.backend.Requeue(ctx, id, rt.cfg.Clock.Now().UTC()); err != nil {
		return err
	}
	if rec, err := rt.backend.Get(ctx, id); err == nil {
		rt.Enqueued(ctx, rec, false)
	}
	return nil
}

// Failed lists failed work, newest first.
func (rt *Runtime) Failed(ctx context.Context, limit int) ([]*Record, error) {
	return rt.backend.List(ctx, ListFilter{Statuses: []Status{StatusFailed}, Limit: limit})
}

// Stats returns record counts per queue and status.
func (rt *Runtime) Stats(ctx context.Context) ([]QueueStat, error) { return rt.backend.Stats(ctx) }

// Get is Runtime.Get on the context's or default Runtime.
func Get(ctx context.Context, id ID) (*Record, error) {
	rt, err := resolve(ctx)
	if err != nil {
		return nil, err
	}
	return rt.Get(ctx, id)
}

// List is Runtime.List on the context's or default Runtime.
func List(ctx context.Context, f ListFilter) ([]*Record, error) {
	rt, err := resolve(ctx)
	if err != nil {
		return nil, err
	}
	return rt.List(ctx, f)
}

// Cancel is Runtime.Cancel on the context's or default Runtime.
func Cancel(ctx context.Context, id ID) (CancelOutcome, error) {
	rt, err := resolve(ctx)
	if err != nil {
		return 0, err
	}
	return rt.Cancel(ctx, id)
}

// Retry is Runtime.Retry on the context's or default Runtime.
func Retry(ctx context.Context, id ID) error {
	rt, err := resolve(ctx)
	if err != nil {
		return err
	}
	return rt.Retry(ctx, id)
}

var _ = errors.Is
