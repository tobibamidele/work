package integration

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tobibamidele/lathe"
	"github.com/tobibamidele/validate"
	"github.com/tobibamidele/work"
	"github.com/tobibamidele/work/database"
	"github.com/tobibamidele/work/memory"
)

type ProcessOrder struct {
	OrderID string `json:"order_id"`
}

var ProcessOrderSchema = validate.Object[ProcessOrder](validate.Field("order_id", validate.String().Min(1).Max(64)))

type txSys struct {
	*sys
	rec *eventLog
}

type eventLog struct {
	mu     sync.Mutex
	events []work.Event
	hook   func(work.Event)
}

func (l *eventLog) Observe(_ context.Context, e work.Event) {
	if l.hook != nil {
		l.hook(e)
	}
	l.mu.Lock()
	l.events = append(l.events, e)
	l.mu.Unlock()
}

func (l *eventLog) count(n work.EventName) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	c := 0
	for _, e := range l.events {
		if e.Name == n {
			c++
		}
	}
	return c
}

func newTxSys(t *testing.T, db *lathe.DB, handler func(context.Context, ProcessOrder) error) *txSys {
	t.Helper()
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE IF NOT EXISTS it_orders (id VARCHAR(64) PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, "DELETE FROM it_orders"); err != nil {
		t.Fatal(err)
	}
	if handler == nil {
		handler = func(context.Context, ProcessOrder) error { return nil }
	}
	b := newBackend(t, db)
	reg := work.NewRegistry()
	reg.MustRegister(work.Define("process-order", ProcessOrderSchema, handler))
	log := &eventLog{}
	rt, err := work.NewRuntime(b, reg, work.RuntimeConfig{Observer: log})
	if err != nil {
		t.Fatal(err)
	}
	return &txSys{sys: &sys{t: t, b: b, reg: reg, rt: rt, db: db}, rec: log}
}

func orderExists(t *testing.T, db *lathe.DB, id string) bool {
	t.Helper()
	rows, err := lathe.Raw[int64](context.Background(), db, "SELECT COUNT(*) FROM it_orders WHERE id = ?", id)
	if err != nil {
		t.Fatal(err)
	}
	return rows[0] == 1
}

func workCount(t *testing.T, s *sys) int {
	t.Helper()
	recs, err := s.b.List(context.Background(), work.ListFilter{})
	if err != nil {
		t.Fatal(err)
	}
	return len(recs)
}

func TestWithTxCommitsOrderAndWorkTogether(t *testing.T) {
	forEachDialect(t, func(t *testing.T, db *lathe.DB) {
		s := newTxSys(t, db, nil)
		ctx := context.Background()
		// at event time the work must already be committed and visible
		var visibleAtEvent atomic.Bool
		s.rec.hook = func(e work.Event) {
			if e.Name == work.EventEnqueued {
				if _, err := s.b.Get(ctx, e.WorkID); err == nil {
					visibleAtEvent.Store(true)
				}
			}
		}
		var ref work.Ref
		err := database.WithTx(ctx, db, s.rt, func(tx *database.Tx) error {
			if _, err := tx.DB().Exec(ctx, "INSERT INTO it_orders (id) VALUES (?)", "o-1"); err != nil {
				return err
			}
			var err error
			ref, err = tx.Dispatch(ctx, ProcessOrder{OrderID: "o-1"})
			if err != nil {
				return err
			}
			return tx.Enqueue(ProcessOrder{OrderID: "o-1b"})
		})
		if err != nil {
			t.Fatal(err)
		}
		if !orderExists(t, db, "o-1") {
			t.Fatal("order missing")
		}
		if rec := s.get(ref.ID); rec.Status != work.StatusAvailable || rec.Type != "process-order" {
			t.Fatalf("record = %+v", rec)
		}
		if workCount(t, s.sys) != 2 {
			t.Fatalf("work count = %d", workCount(t, s.sys))
		}
		if s.rec.count(work.EventEnqueued) != 2 || !visibleAtEvent.Load() {
			t.Fatalf("enqueued events = %d, visible at event time = %v", s.rec.count(work.EventEnqueued), visibleAtEvent.Load())
		}
	})
}

func TestWithTxRollbackLeavesNeitherOrderNorWork(t *testing.T) {
	forEachDialect(t, func(t *testing.T, db *lathe.DB) {
		s := newTxSys(t, db, nil)
		ctx := context.Background()
		boom := errors.New("payment declined")
		var ref work.Ref
		err := database.WithTx(ctx, db, s.rt, func(tx *database.Tx) error {
			if _, err := tx.DB().Exec(ctx, "INSERT INTO it_orders (id) VALUES (?)", "o-2"); err != nil {
				return err
			}
			var err error
			if ref, err = tx.Dispatch(ctx, ProcessOrder{OrderID: "o-2"}); err != nil {
				return err
			}
			return boom
		})
		if !errors.Is(err, boom) {
			t.Fatalf("err = %v", err)
		}
		if orderExists(t, db, "o-2") {
			t.Fatal("order must be rolled back")
		}
		if _, err := s.b.Get(ctx, ref.ID); !errors.Is(err, work.ErrNotFound) {
			t.Fatalf("work must be rolled back with the transaction, got %v", err)
		}
		if workCount(t, s.sys) != 0 || s.rec.count(work.EventEnqueued) != 0 {
			t.Fatal("no work and no enqueued event after a rollback")
		}
	})
}

func TestDispatchTxInsideALatheTransaction(t *testing.T) {
	forEachDialect(t, func(t *testing.T, db *lathe.DB) {
		s := newTxSys(t, db, nil)
		ctx := context.Background()
		err := db.Tx(ctx, func(tx *lathe.DB) error {
			if _, err := tx.Exec(ctx, "INSERT INTO it_orders (id) VALUES (?)", "o-3"); err != nil {
				return err
			}
			_, err := database.DispatchTx(ctx, s.rt, tx, ProcessOrder{OrderID: "o-3"})
			return err
		})
		if err != nil || workCount(t, s.sys) != 1 {
			t.Fatalf("err = %v, work = %d", err, workCount(t, s.sys))
		}
		// a failing callback takes the work with it
		err = db.Tx(ctx, func(tx *lathe.DB) error {
			if _, err := database.DispatchTx(ctx, s.rt, tx, ProcessOrder{OrderID: "o-4"}); err != nil {
				return err
			}
			return errors.New("abort")
		})
		if err == nil || workCount(t, s.sys) != 1 {
			t.Fatalf("err = %v, work = %d", err, workCount(t, s.sys))
		}
	})
}

func TestTransactionalDispatchRefusesAPlainHandle(t *testing.T) {
	forEachDialect(t, func(t *testing.T, db *lathe.DB) {
		s := newTxSys(t, db, nil)
		ctx := context.Background()
		_, err := database.DispatchTx(ctx, s.rt, db, ProcessOrder{OrderID: "x"})
		if !errors.Is(err, database.ErrNotInTransaction) {
			t.Fatalf("err = %v: using the plain handle would silently drop the atomicity", err)
		}
		err = s.b.EnqueueTx(ctx, db, &work.Record{})
		if !errors.Is(err, database.ErrNotInTransaction) {
			t.Fatalf("EnqueueTx: %v", err)
		}
		if workCount(t, s.sys) != 0 {
			t.Fatal("nothing may be written")
		}
	})
}

func TestUniqueConflictInsideATransactionDoesNotPoisonIt(t *testing.T) {
	// On PostgreSQL a failed statement aborts the whole transaction; the
	// backend claims keys without provoking errors, so the caller's
	// transaction keeps working.
	forEachDialect(t, func(t *testing.T, db *lathe.DB) {
		s := newTxSys(t, db, nil)
		ctx := context.Background()
		committed, err := s.rt.Dispatch(ctx, ProcessOrder{OrderID: "dup"}, work.Unique("order:dup"))
		if err != nil {
			t.Fatal(err)
		}
		var inTx, second work.Ref
		err = database.WithTx(ctx, db, s.rt, func(tx *database.Tx) error {
			if _, err := tx.DB().Exec(ctx, "INSERT INTO it_orders (id) VALUES (?)", "o-5"); err != nil {
				return err
			}
			var derr error
			inTx, derr = tx.Dispatch(ctx, ProcessOrder{OrderID: "dup"}, work.Unique("order:dup"))
			if !errors.Is(derr, work.ErrDuplicate) || inTx.ID != committed.ID {
				return fmt.Errorf("duplicate against committed work: %v, ref %v", derr, inTx)
			}
			// the transaction is still healthy: keep working in it
			if _, err := tx.DB().Exec(ctx, "INSERT INTO it_orders (id) VALUES (?)", "o-6"); err != nil {
				return fmt.Errorf("transaction was poisoned by the conflict: %w", err)
			}
			second, derr = tx.Dispatch(ctx, ProcessOrder{OrderID: "fresh"}, work.Unique("order:fresh"))
			if derr != nil {
				return derr
			}
			// duplicate of work added earlier in this same transaction
			again, derr := tx.Dispatch(ctx, ProcessOrder{OrderID: "fresh"}, work.Unique("order:fresh"))
			if !errors.Is(derr, work.ErrDuplicate) || again.ID != second.ID {
				return fmt.Errorf("in-transaction duplicate: %v, %v", derr, again)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if !orderExists(t, db, "o-5") || !orderExists(t, db, "o-6") {
			t.Fatal("both orders must be committed")
		}
		if workCount(t, s.sys) != 2 {
			t.Fatalf("work count = %d, want 2 (the committed one and the fresh one)", workCount(t, s.sys))
		}
	})
}

func TestInvalidPayloadInATransactionKeepsItUsable(t *testing.T) {
	forEachDialect(t, func(t *testing.T, db *lathe.DB) {
		s := newTxSys(t, db, nil)
		ctx := context.Background()
		err := database.WithTx(ctx, db, s.rt, func(tx *database.Tx) error {
			if _, err := tx.DB().Exec(ctx, "INSERT INTO it_orders (id) VALUES (?)", "o-7"); err != nil {
				return err
			}
			if _, err := tx.Dispatch(ctx, ProcessOrder{OrderID: ""}); !errors.Is(err, work.ErrInvalidPayload) {
				return fmt.Errorf("want ErrInvalidPayload, got %v", err)
			}
			if _, err := tx.Dispatch(ctx, struct{ X int }{1}); !errors.Is(err, work.ErrUnknownType) {
				return fmt.Errorf("want ErrUnknownType, got %v", err)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if !orderExists(t, db, "o-7") || workCount(t, s.sys) != 0 {
			t.Fatal("the order commits, no work was persisted")
		}
	})
}

func TestBackendsWithoutTransactionsRefuseTransactionalDispatch(t *testing.T) {
	reg := work.NewRegistry()
	reg.MustRegister(work.Define("process-order", ProcessOrderSchema, func(context.Context, ProcessOrder) error { return nil }))
	rt, _ := work.NewRuntime(memory.New(), reg, work.RuntimeConfig{})
	db := dialects[2].open(t)
	err := database.WithTx(context.Background(), db, rt, func(*database.Tx) error { return nil })
	if !errors.Is(err, work.ErrUnsupported) {
		t.Fatalf("WithTx: %v", err)
	}
	err = db.Tx(context.Background(), func(tx *lathe.DB) error {
		_, err := database.DispatchTx(context.Background(), rt, tx, ProcessOrder{OrderID: "x"})
		if !errors.Is(err, work.ErrUnsupported) {
			t.Errorf("DispatchTx: %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestWorkIsInvisibleToWorkersUntilCommit(t *testing.T) {
	// SQLite has a single writer, so a second connection cannot even look
	// while a write transaction is open; the guarantee holds there trivially.
	for _, d := range dialects[:2] {
		d := d
		t.Run(d.name, func(t *testing.T) {
			db := freshDB(t, d)
			s := newTxSys(t, db, nil)
			ctx := context.Background()
			err := database.WithTx(ctx, db, s.rt, func(tx *database.Tx) error {
				if err := tx.Enqueue(ProcessOrder{OrderID: "o-8"}); err != nil {
					return err
				}
				_, cerr := s.b.Claim(ctx, work.ClaimOptions{Queues: []string{"default"}, WorkerID: "w", Now: time.Now().UTC(), LeaseDuration: time.Minute, Limit: 5})
				if !errors.Is(cerr, work.ErrNoWork) {
					return fmt.Errorf("an uncommitted work item was claimable: %v", cerr)
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.b.Claim(ctx, work.ClaimOptions{Queues: []string{"default"}, WorkerID: "w", Now: time.Now().UTC(), LeaseDuration: time.Minute, Limit: 5}); err != nil {
				t.Fatalf("after the commit the work is claimable: %v", err)
			}
		})
	}
}

func TestHandlersAlwaysSeeTheCommittedOrder(t *testing.T) {
	forEachDialect(t, func(t *testing.T, db *lathe.DB) {
		var missing, handled atomic.Int32
		var s *txSys
		s = newTxSys(t, db, func(ctx context.Context, p ProcessOrder) error {
			rows, err := lathe.Raw[int64](ctx, db, "SELECT COUNT(*) FROM it_orders WHERE id = ?", p.OrderID)
			if err != nil {
				return err
			}
			if rows[0] != 1 {
				missing.Add(1)
				return errors.New("order not visible: the work ran before its transaction committed")
			}
			handled.Add(1)
			return nil
		})
		s.worker(work.WorkerConfig{Concurrency: 4})
		const n = 40
		var wg sync.WaitGroup
		for i := 0; i < n; i++ {
			i := i
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := database.WithTx(context.Background(), db, s.rt, func(tx *database.Tx) error {
					id := fmt.Sprintf("conc-%d", i)
					if _, err := tx.DB().Exec(context.Background(), "INSERT INTO it_orders (id) VALUES (?)", id); err != nil {
						return err
					}
					return tx.Enqueue(ProcessOrder{OrderID: id})
				})
				if err != nil {
					t.Errorf("tx %d: %v", i, err)
				}
			}()
		}
		wg.Wait()
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) && handled.Load() < n {
			time.Sleep(20 * time.Millisecond)
		}
		if missing.Load() != 0 || handled.Load() != n {
			t.Fatalf("handled = %d, ran-before-commit = %d", handled.Load(), missing.Load())
		}
	})
}
