package integration

import (
	"context"
	"errors"
	"io/fs"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tobibamidele/lathe"
	"github.com/tobibamidele/validate"
	"github.com/tobibamidele/work"
	"github.com/tobibamidele/work/database"
)

type fsLike = fs.FS

type Ping struct {
	N int `json:"n"`
}

var PingSchema = validate.Object[Ping](validate.Field("n", validate.Int().Min(0)))

type sys struct {
	t   *testing.T
	b   *database.Backend
	reg *work.Registry
	rt  *work.Runtime
	db  *lathe.DB
}

func newSys(t *testing.T, db *lathe.DB, fn func(context.Context, Ping) error) *sys {
	t.Helper()
	b := newBackend(t, db)
	reg := work.NewRegistry()
	reg.MustRegister(work.Define("ping", PingSchema, fn))
	rt, err := work.NewRuntime(b, reg, work.RuntimeConfig{})
	if err != nil {
		t.Fatal(err)
	}
	return &sys{t: t, b: b, reg: reg, rt: rt, db: db}
}

func (s *sys) worker(cfg work.WorkerConfig) func() error {
	s.t.Helper()
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 10 * time.Millisecond
	}
	if cfg.Backoff == nil {
		cfg.Backoff = work.FixedBackoff(10 * time.Millisecond)
	}
	if cfg.ReapInterval == 0 {
		cfg.ReapInterval = 50 * time.Millisecond
	}
	w, err := work.NewWorker(s.b, s.reg, cfg)
	if err != nil {
		s.t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	var once sync.Once
	var res error
	stop := func() error {
		once.Do(func() {
			cancel()
			select {
			case res = <-done:
			case <-time.After(20 * time.Second):
				s.t.Error("worker did not stop")
			}
		})
		return res
	}
	s.t.Cleanup(func() { _ = stop() })
	return stop
}

func (s *sys) get(id work.ID) *work.Record {
	s.t.Helper()
	r, err := s.b.Get(context.Background(), id)
	if err != nil {
		s.t.Fatal(err)
	}
	return r
}

func (s *sys) waitStatus(id work.ID, st work.Status) *work.Record {
	s.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if r := s.get(id); r.Status == st {
			return r
		}
		time.Sleep(10 * time.Millisecond)
	}
	s.t.Fatalf("work %s never reached %s (is %s)", id, st, s.get(id).Status)
	return nil
}

func TestWorkersRunEveryItemExactlyOnce(t *testing.T) {
	forEachDialect(t, func(t *testing.T, db *lathe.DB) {
		var mu sync.Mutex
		seen := map[int]int{}
		s := newSys(t, db, func(_ context.Context, p Ping) error {
			mu.Lock()
			seen[p.N]++
			mu.Unlock()
			return nil
		})
		const total = 120
		for i := 0; i < total; i++ {
			if _, err := s.rt.Dispatch(context.Background(), Ping{N: i}); err != nil {
				t.Fatal(err)
			}
		}
		for i := 0; i < 4; i++ {
			s.worker(work.WorkerConfig{Concurrency: 4})
		}
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) {
			recs, _ := s.b.List(context.Background(), work.ListFilter{Statuses: []work.Status{work.StatusCompleted}})
			if len(recs) == total {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		mu.Lock()
		defer mu.Unlock()
		if len(seen) != total {
			t.Fatalf("ran %d of %d items", len(seen), total)
		}
		for n, c := range seen {
			if c != 1 {
				t.Fatalf("item %d ran %d times", n, c)
			}
		}
	})
}

func TestRetriesAndPermanentFailure(t *testing.T) {
	forEachDialect(t, func(t *testing.T, db *lathe.DB) {
		var flaky atomic.Int32
		s := newSys(t, db, func(_ context.Context, p Ping) error {
			switch p.N {
			case 1:
				if flaky.Add(1) < 3 {
					return errors.New("flaky")
				}
				return nil
			case 2:
				return work.NonRetryable(errors.New("will never work"))
			}
			return errors.New("always")
		})
		ctx := context.Background()
		a, _ := s.rt.Dispatch(ctx, Ping{N: 1}, work.MaxAttempts(5))
		b, _ := s.rt.Dispatch(ctx, Ping{N: 2}, work.MaxAttempts(5))
		c, _ := s.rt.Dispatch(ctx, Ping{N: 3}, work.MaxAttempts(2))
		s.worker(work.WorkerConfig{})
		if r := s.waitStatus(a.ID, work.StatusCompleted); r.Attempts != 3 {
			t.Fatalf("attempts = %d", r.Attempts)
		}
		if r := s.waitStatus(b.ID, work.StatusFailed); r.Attempts != 1 {
			t.Fatalf("non-retryable attempts = %d", r.Attempts)
		}
		r := s.waitStatus(c.ID, work.StatusFailed)
		if r.Attempts != 2 || r.LastError == "" {
			t.Fatalf("exhausted = %+v", r)
		}
	})
}

func TestCrashRecoveryAndHeartbeats(t *testing.T) {
	forEachDialect(t, func(t *testing.T, db *lathe.DB) {
		var runs atomic.Int32
		s := newSys(t, db, func(_ context.Context, p Ping) error {
			runs.Add(1)
			if p.N == 2 {
				time.Sleep(700 * time.Millisecond) // longer than the lease
			}
			return nil
		})
		ctx := context.Background()
		crashed, _ := s.rt.Dispatch(ctx, Ping{N: 1})
		slow, _ := s.rt.Dispatch(ctx, Ping{N: 2}, work.Queue("slow"))
		// simulate a worker that claimed the first item and died
		if _, err := s.b.Claim(ctx, work.ClaimOptions{Queues: []string{"default"}, WorkerID: "dead", Now: time.Now().UTC(), LeaseDuration: 200 * time.Millisecond, Limit: 1}); err != nil {
			t.Fatal(err)
		}
		cfg := work.WorkerConfig{LeaseDuration: 300 * time.Millisecond, HeartbeatInterval: 60 * time.Millisecond,
			Queues: []work.QueueConfig{{Name: "default"}, {Name: "slow"}}}
		s.worker(cfg)
		s.worker(cfg)
		if r := s.waitStatus(crashed.ID, work.StatusCompleted); r.Attempts != 2 {
			t.Fatalf("recovered work attempts = %d", r.Attempts)
		}
		if r := s.waitStatus(slow.ID, work.StatusCompleted); r.Attempts != 1 {
			t.Fatalf("a heartbeating handler must keep its lease: attempts = %d", r.Attempts)
		}
		if runs.Load() != 2 {
			t.Fatalf("handler runs = %d, want 2", runs.Load())
		}
	})
}

func TestCooperativeCancellationOfRunningWork(t *testing.T) {
	forEachDialect(t, func(t *testing.T, db *lathe.DB) {
		started := make(chan struct{}, 1)
		s := newSys(t, db, func(ctx context.Context, _ Ping) error {
			started <- struct{}{}
			<-ctx.Done()
			return ctx.Err()
		})
		ctx := context.Background()
		ref, _ := s.rt.Dispatch(ctx, Ping{N: 1}, work.MaxAttempts(5))
		s.worker(work.WorkerConfig{LeaseDuration: 2 * time.Second, HeartbeatInterval: 50 * time.Millisecond})
		<-started
		out, err := s.rt.Cancel(ctx, ref.ID)
		if err != nil || out != work.CancelRequested {
			t.Fatalf("cancel = %v, %v", out, err)
		}
		r := s.waitStatus(ref.ID, work.StatusCancelled)
		if r.Attempts != 1 {
			t.Fatalf("attempts = %d", r.Attempts)
		}
	})
}

func TestUniquenessAndDelayAndPriorityThroughTheRuntime(t *testing.T) {
	forEachDialect(t, func(t *testing.T, db *lathe.DB) {
		var mu sync.Mutex
		var order []int
		s := newSys(t, db, func(_ context.Context, p Ping) error {
			mu.Lock()
			order = append(order, p.N)
			mu.Unlock()
			return nil
		})
		ctx := context.Background()
		first, err := s.rt.Dispatch(ctx, Ping{N: 1}, work.UniquePayload())
		if err != nil {
			t.Fatal(err)
		}
		dup, err := s.rt.Dispatch(ctx, Ping{N: 1}, work.UniquePayload())
		if !errors.Is(err, work.ErrDuplicate) || dup.ID != first.ID {
			t.Fatalf("dup = %v, %v", dup, err)
		}
		if _, err := s.rt.Dispatch(ctx, Ping{N: 5}, work.Priority(1)); err != nil {
			t.Fatal(err)
		}
		if _, err := s.rt.Dispatch(ctx, Ping{N: 6}, work.Priority(9)); err != nil {
			t.Fatal(err)
		}
		late, _ := s.rt.Dispatch(ctx, Ping{N: 7}, work.Delay(400*time.Millisecond), work.Priority(100))
		s.worker(work.WorkerConfig{Concurrency: 1})
		s.waitStatus(late.ID, work.StatusCompleted)
		mu.Lock()
		defer mu.Unlock()
		want := []int{6, 5, 1, 7}
		if len(order) != 4 {
			t.Fatalf("order = %v", order)
		}
		for i := range want {
			if order[i] != want[i] {
				t.Fatalf("order = %v, want %v", order, want)
			}
		}
		// the completed key is free again
		if _, err := s.rt.Dispatch(ctx, Ping{N: 1}, work.UniquePayload()); err != nil {
			t.Fatalf("completed work must not block the key: %v", err)
		}
	})
}

func TestQueueDepthStatsAndInspection(t *testing.T) {
	forEachDialect(t, func(t *testing.T, db *lathe.DB) {
		s := newSys(t, db, func(context.Context, Ping) error { return errors.New("nope") })
		ctx := context.Background()
		var refs []work.Ref
		for i := 0; i < 3; i++ {
			r, _ := s.rt.Dispatch(ctx, Ping{N: i}, work.MaxAttempts(1), work.Queue("q"))
			refs = append(refs, r)
		}
		s.rt.Dispatch(ctx, Ping{N: 9}, work.Delay(time.Hour))
		s.worker(work.WorkerConfig{Queues: []work.QueueConfig{{Name: "q"}}})
		for _, r := range refs {
			s.waitStatus(r.ID, work.StatusFailed)
		}
		stats, err := s.rt.Stats(ctx)
		if err != nil {
			t.Fatal(err)
		}
		got := map[string]int64{}
		for _, st := range stats {
			got[st.Queue+"/"+string(st.Status)] = st.Count
		}
		if got["q/failed"] != 3 || got["default/available"] != 1 {
			t.Fatalf("stats = %v", got)
		}
		failed, err := s.rt.Failed(ctx, 2)
		if err != nil || len(failed) != 2 {
			t.Fatalf("failed = %v, %v", failed, err)
		}
		if err := s.rt.Retry(ctx, refs[0].ID); err != nil {
			t.Fatal(err)
		}
	})
}

func TestNonJSONPayloadsAreRefusedExplicitly(t *testing.T) {
	forEachDialect(t, func(t *testing.T, db *lathe.DB) {
		b := newBackend(t, db)
		now := time.Now().UTC()
		err := b.Enqueue(context.Background(), &work.Record{
			ID: "0197a000-0000-7000-8000-000000000001", Type: "x", Version: 1, Queue: "q", Payload: []byte{0x80, 0x81},
			Status: work.StatusAvailable, MaxAttempts: 1, AvailableAt: now, CreatedAt: now, UpdatedAt: now,
		})
		if !errors.Is(err, work.ErrUnsupported) {
			t.Fatalf("err = %v, want ErrUnsupported", err)
		}
		err = b.Enqueue(context.Background(), &work.Record{
			ID: "not-a-uuid", Type: "x", Version: 1, Queue: "q", Payload: []byte(`{}`),
			Status: work.StatusAvailable, MaxAttempts: 1, AvailableAt: now, CreatedAt: now, UpdatedAt: now,
		})
		if err == nil {
			t.Fatal("a non-UUID id must be refused")
		}
	})
}

var _ = lathe.ErrNotFound
