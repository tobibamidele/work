package database

import (
	"context"
	"fmt"

	"github.com/tobibamidele/lathe"
	"github.com/tobibamidele/work"
)

// txEnqueuer is implemented by backends that can enqueue inside an
// application transaction.
type txEnqueuer interface {
	EnqueueTx(ctx context.Context, tx *lathe.DB, rec *work.Record) error
}

// EnqueueTx inserts rec using tx, the *lathe.DB handed to a Lathe transaction
// callback. The insert is part of that transaction: it commits or rolls back
// with the caller's own writes, and the work is invisible to workers until the
// commit.
//
// tx must be bound to a transaction. A plain handle is refused with
// ErrNotInTransaction rather than silently inserting outside the transaction,
// which would break the atomicity transactional dispatch exists to provide.
func (b *Backend) EnqueueTx(ctx context.Context, tx *lathe.DB, rec *work.Record) error {
	if tx == nil || !tx.InTx() {
		return ErrNotInTransaction
	}
	return b.enqueue(ctx, tx, rec)
}

func txEnqueuerOf(rt *work.Runtime) (txEnqueuer, error) {
	te, ok := rt.Backend().(txEnqueuer)
	if !ok || !rt.Backend().Capabilities().Transactions {
		return nil, fmt.Errorf("%w: the runtime's backend cannot enqueue inside a transaction", work.ErrUnsupported)
	}
	return te, nil
}

// DispatchTx is Dispatch inside a Lathe transaction: it validates and
// serializes payload exactly like work.Dispatch, then inserts the work record
// with tx, the handle passed to the transaction callback.
//
//	err := client.Tx(ctx, func(tx *db.Client) error {
//	    order := db.Order{...}
//	    if err := tx.Orders.Create(ctx, &order); err != nil {
//	        return err
//	    }
//	    _, err := database.DispatchTx(ctx, rt, tx.DB, ProcessOrder{OrderID: order.ID})
//	    return err
//	})
//
// If the callback returns an error, neither the order nor the work exists.
// The work.enqueued event is emitted when the insert succeeds, which can be
// before the surrounding commit; use WithTx to have it emitted after commit.
func DispatchTx(ctx context.Context, rt *work.Runtime, tx *lathe.DB, payload any, opts ...work.DispatchOption) (work.Ref, error) {
	te, err := txEnqueuerOf(rt)
	if err != nil {
		return work.Ref{}, err
	}
	if tx == nil || !tx.InTx() {
		return work.Ref{}, ErrNotInTransaction
	}
	rec, err := rt.Prepare(ctx, payload, opts...)
	if err != nil {
		return work.Ref{}, err
	}
	if err := te.EnqueueTx(ctx, tx, rec); err != nil {
		return duplicateRef(err), err
	}
	rt.Enqueued(ctx, rec, true)
	return work.Ref{ID: rec.ID, Type: rec.Type, Version: rec.Version, Queue: rec.Queue, AvailableAt: rec.AvailableAt}, nil
}

func duplicateRef(err error) work.Ref {
	var de *work.DuplicateError
	if asDup(err, &de) {
		return work.Ref{ID: de.ExistingID, Queue: de.Queue}
	}
	return work.Ref{}
}

// Tx is a transaction that can dispatch work. It is what WithTx passes to your
// function: DB() is the handle for your own statements, Dispatch and Enqueue
// add work to the same transaction.
type Tx struct {
	ctx     context.Context
	rt      *work.Runtime
	te      txEnqueuer
	tx      *lathe.DB
	pending []*work.Record
}

// DB returns the transaction-bound handle. Use it (or a Lathe client built
// from it) for every statement that must be atomic with the dispatched work.
func (t *Tx) DB() *lathe.DB { return t.tx }

// Dispatch validates payload and inserts the work in this transaction. A
// Unique collision returns an error matching work.ErrDuplicate with the Ref of
// the active work; the transaction stays usable.
func (t *Tx) Dispatch(ctx context.Context, payload any, opts ...work.DispatchOption) (work.Ref, error) {
	rec, err := t.rt.Prepare(ctx, payload, opts...)
	if err != nil {
		return work.Ref{}, err
	}
	if err := t.te.EnqueueTx(ctx, t.tx, rec); err != nil {
		return duplicateRef(err), err
	}
	t.pending = append(t.pending, rec)
	return work.Ref{ID: rec.ID, Type: rec.Type, Version: rec.Version, Queue: rec.Queue, AvailableAt: rec.AvailableAt}, nil
}

// Enqueue is Dispatch with the transaction's context, for the common case of
// not needing the reference.
func (t *Tx) Enqueue(payload any, opts ...work.DispatchOption) error {
	_, err := t.Dispatch(t.ctx, payload, opts...)
	return err
}

// WithTx runs fn in a Lathe transaction on db. Work dispatched through the Tx
// is inserted in that transaction; it exists if and only if the transaction
// commits. The work.enqueued events are emitted after the commit succeeds.
//
//	err := database.WithTx(ctx, conn, rt, func(tx *database.Tx) error {
//	    if err := orders.Create(ctx, tx.DB(), &order); err != nil {
//	        return err // rolls back: no order, no work
//	    }
//	    return tx.Enqueue(ProcessOrder{OrderID: order.ID})
//	})
func WithTx(ctx context.Context, db *lathe.DB, rt *work.Runtime, fn func(tx *Tx) error) error {
	te, err := txEnqueuerOf(rt)
	if err != nil {
		return err
	}
	var pending []*work.Record
	err = db.Tx(ctx, func(raw *lathe.DB) error {
		t := &Tx{ctx: ctx, rt: rt, te: te, tx: raw}
		if err := fn(t); err != nil {
			return err
		}
		pending = t.pending
		return nil
	})
	if err != nil {
		return err
	}
	for _, rec := range pending {
		rt.Enqueued(ctx, rec, false)
	}
	return nil
}
