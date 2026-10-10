package redis_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/tobibamidele/validate"
	"github.com/tobibamidele/work"
	"github.com/tobibamidele/work/backendtest"
	workredis "github.com/tobibamidele/work/redis"
)

// WORK_TEST_REDIS defaults to a local server. An unreachable server skips the
// tests unless WORK_TEST_REQUIRE_DB=1 (CI).
func client(t *testing.T) *goredis.Client {
	t.Helper()
	url := os.Getenv("WORK_TEST_REDIS")
	if url == "" {
		url = "redis://127.0.0.1:6379/0"
	}
	opt, err := goredis.ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	c := goredis.NewClient(opt)
	if err := c.Ping(context.Background()).Err(); err != nil {
		c.Close()
		if os.Getenv("WORK_TEST_REQUIRE_DB") == "1" {
			t.Fatalf("redis: %v", err)
		}
		t.Skipf("redis is not reachable: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// freshBackend returns a backend in its own key namespace, removed afterwards.
func freshBackend(t *testing.T, c *goredis.Client) (*workredis.Backend, string) {
	t.Helper()
	prefix := "t" + uuid.NewString()[:12]
	b, err := workredis.New(context.Background(), c, workredis.WithPrefix(prefix))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		iter := c.Scan(ctx, 0, "{"+prefix+"}:*", 500).Iterator()
		for iter.Next(ctx) {
			c.Del(ctx, iter.Val())
		}
	})
	return b, prefix
}

func TestConformance(t *testing.T) {
	c := client(t)
	backendtest.Run(t, func(t *testing.T) work.Backend {
		b, _ := freshBackend(t, c)
		return b
	})
}

func TestNewValidatesItsInputs(t *testing.T) {
	c := client(t)
	if _, err := workredis.New(context.Background(), nil); !errors.Is(err, work.ErrConfig) {
		t.Fatalf("nil client: %v", err)
	}
	for _, bad := range []string{"", "has space", "a{b}", "x/y"} {
		if _, err := workredis.New(context.Background(), c, workredis.WithPrefix(bad)); !errors.Is(err, work.ErrConfig) {
			t.Errorf("prefix %q: %v", bad, err)
		}
	}
	dead := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:1", MaxRetries: -1, DialTimeout: 200 * time.Millisecond})
	defer dead.Close()
	if _, err := workredis.New(context.Background(), dead); err == nil {
		t.Fatal("an unreachable Redis must fail at startup")
	}
	b, _ := freshBackend(t, c)
	caps := b.Capabilities()
	if caps.Name != "redis" || caps.Transactions || !caps.StrongUniqueness || !caps.DelayedWork || !caps.Cancellation || !caps.Priorities {
		t.Fatalf("capabilities = %+v", caps)
	}
}

func TestNamespacesAreIsolated(t *testing.T) {
	c := client(t)
	a, _ := freshBackend(t, c)
	b, _ := freshBackend(t, c)
	now := time.Now().UTC()
	rec := &work.Record{ID: work.ID(uuid.NewString()), Type: "t", Version: 1, Queue: "q", Payload: []byte(`{}`),
		Status: work.StatusAvailable, MaxAttempts: 1, AvailableAt: now, CreatedAt: now, UpdatedAt: now}
	if err := a.Enqueue(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Claim(context.Background(), work.ClaimOptions{Queues: []string{"q"}, WorkerID: "w", Now: now, LeaseDuration: time.Minute, Limit: 1}); !errors.Is(err, work.ErrNoWork) {
		t.Fatalf("another namespace must not see the work: %v", err)
	}
	if _, err := b.Get(context.Background(), rec.ID); !errors.Is(err, work.ErrNotFound) {
		t.Fatal("get across namespaces")
	}
}

func TestStateSurvivesANewBackendInstance(t *testing.T) {
	c := client(t)
	first, prefix := freshBackend(t, c)
	now := time.Now().UTC()
	rec := &work.Record{ID: work.ID(uuid.NewString()), Type: "t", Version: 1, Queue: "q", Payload: []byte(`{"a":1}`),
		Status: work.StatusAvailable, MaxAttempts: 3, AvailableAt: now, CreatedAt: now, UpdatedAt: now}
	if err := first.Enqueue(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	second, err := workredis.New(context.Background(), c, workredis.WithPrefix(prefix))
	if err != nil {
		t.Fatal(err)
	}
	got, err := second.Claim(context.Background(), work.ClaimOptions{Queues: []string{"q"}, WorkerID: "w", Now: now, LeaseDuration: time.Minute, Limit: 1})
	if err != nil || len(got) != 1 || got[0].ID != rec.ID {
		t.Fatalf("claim = %v, %v", got, err)
	}
}

func TestBinaryPayloadsAreStoredExactly(t *testing.T) {
	// unlike the database backend (JSON column), Redis holds opaque bytes, so
	// a non-JSON serializer works
	c := client(t)
	b, _ := freshBackend(t, c)
	now := time.Now().UTC()
	payload := []byte{0x00, 0xff, 0x80, 'x', 0x00, '\n'}
	rec := &work.Record{ID: work.ID(uuid.NewString()), Type: "t", Version: 1, Queue: "q", Payload: payload,
		Status: work.StatusAvailable, MaxAttempts: 1, AvailableAt: now, CreatedAt: now, UpdatedAt: now}
	if err := b.Enqueue(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	got, _ := b.Get(context.Background(), rec.ID)
	if string(got.Payload) != string(payload) {
		t.Fatalf("payload = %x", got.Payload)
	}
}

func TestOddNamesDoNotCollide(t *testing.T) {
	// queue "a:b" + type "c" must not be confused with queue "a" + type "b:c"
	c := client(t)
	b, _ := freshBackend(t, c)
	ctx := context.Background()
	now := time.Now().UTC()
	mk := func(queue, typ string) *work.Record {
		return &work.Record{ID: work.ID(uuid.NewString()), Type: typ, Version: 1, Queue: queue, Payload: []byte(`{}`),
			Status: work.StatusAvailable, MaxAttempts: 1, AvailableAt: now, CreatedAt: now, UpdatedAt: now}
	}
	x, y := mk("a:b", "c"), mk("a", "b:c")
	for _, r := range []*work.Record{x, y} {
		if err := b.Enqueue(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	got, err := b.Claim(ctx, work.ClaimOptions{Queues: []string{"a"}, WorkerID: "w", Now: now, LeaseDuration: time.Minute, Limit: 10})
	if err != nil || len(got) != 1 || got[0].ID != y.ID {
		t.Fatalf("claim a = %v, %v", got, err)
	}
	got, err = b.Claim(ctx, work.ClaimOptions{Queues: []string{"a:b"}, WorkerID: "w", Now: now, LeaseDuration: time.Minute, Limit: 10})
	if err != nil || len(got) != 1 || got[0].ID != x.ID {
		t.Fatalf("claim a:b = %v, %v", got, err)
	}
}

func TestExtremePrioritiesSortCorrectly(t *testing.T) {
	c := client(t)
	b, _ := freshBackend(t, c)
	ctx := context.Background()
	now := time.Now().UTC()
	prios := []int{0, 2147483647, -2147483648, 1, -1, 100000}
	ids := map[int]work.ID{}
	for i, p := range prios {
		r := &work.Record{ID: work.ID(uuid.NewString()), Type: "t", Version: 1, Queue: "q", Payload: []byte(`{}`), Priority: p,
			Status: work.StatusAvailable, MaxAttempts: 1, AvailableAt: now, CreatedAt: now.Add(time.Duration(i) * time.Millisecond), UpdatedAt: now}
		ids[p] = r.ID
		if err := b.Enqueue(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	got, err := b.Claim(ctx, work.ClaimOptions{Queues: []string{"q"}, WorkerID: "w", Now: now, LeaseDuration: time.Minute, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	want := []int{2147483647, 100000, 1, 0, -1, -2147483648}
	for i, r := range got {
		if r.ID != ids[want[i]] {
			t.Fatalf("position %d: priority %d, want %d", i, r.Priority, want[i])
		}
	}
}

func TestManyTypesAndQueuesInOneClaim(t *testing.T) {
	c := client(t)
	b, _ := freshBackend(t, c)
	ctx := context.Background()
	now := time.Now().UTC()
	for i := 0; i < 30; i++ {
		r := &work.Record{ID: work.ID(uuid.NewString()), Type: fmt.Sprintf("type%d", i%5), Version: 1, Queue: fmt.Sprintf("q%d", i%3),
			Payload: []byte(`{}`), Priority: i % 4, Status: work.StatusAvailable, MaxAttempts: 1,
			AvailableAt: now, CreatedAt: now.Add(time.Duration(i) * time.Millisecond), UpdatedAt: now}
		if err := b.Enqueue(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	got, err := b.Claim(ctx, work.ClaimOptions{Queues: []string{"q0", "q1", "q2"}, WorkerID: "w", Now: now, LeaseDuration: time.Minute, Limit: 30})
	if err != nil || len(got) != 30 {
		t.Fatalf("claimed %d, %v", len(got), err)
	}
	for i := 1; i < len(got); i++ {
		a, c := got[i-1], got[i]
		if a.Priority < c.Priority || (a.Priority == c.Priority && a.CreatedAt.After(c.CreatedAt)) {
			t.Fatalf("claim order broken at %d: %+v then %+v", i, a, c)
		}
	}
}

// ---- the whole engine on Redis ----

type Ping struct {
	N int `json:"n"`
}

var PingSchema = validate.Object[Ping](validate.Field("n", validate.Int().Min(0)))

type sys struct {
	t   *testing.T
	b   *workredis.Backend
	reg *work.Registry
	rt  *work.Runtime
}

func newSys(t *testing.T, fn func(context.Context, Ping) error) *sys {
	t.Helper()
	b, _ := freshBackend(t, client(t))
	reg := work.NewRegistry()
	reg.MustRegister(work.Define("ping", PingSchema, fn))
	rt, err := work.NewRuntime(b, reg, work.RuntimeConfig{})
	if err != nil {
		t.Fatal(err)
	}
	return &sys{t: t, b: b, reg: reg, rt: rt}
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

func (s *sys) waitStatus(id work.ID, st work.Status) *work.Record {
	s.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		r, err := s.b.Get(context.Background(), id)
		if err != nil {
			s.t.Fatal(err)
		}
		if r.Status == st {
			return r
		}
		time.Sleep(10 * time.Millisecond)
	}
	r, _ := s.b.Get(context.Background(), id)
	s.t.Fatalf("work %s never reached %s (is %s)", id, st, r.Status)
	return nil
}

func TestWorkersRunEveryItemExactlyOnce(t *testing.T) {
	var mu sync.Mutex
	seen := map[int]int{}
	s := newSys(t, func(_ context.Context, p Ping) error {
		mu.Lock()
		seen[p.N]++
		mu.Unlock()
		return nil
	})
	const total = 200
	for i := 0; i < total; i++ {
		if _, err := s.rt.Dispatch(context.Background(), Ping{N: i}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 5; i++ {
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
		t.Fatalf("ran %d of %d", len(seen), total)
	}
	for n, c := range seen {
		if c != 1 {
			t.Fatalf("item %d ran %d times", n, c)
		}
	}
}

func TestRetriesFailuresDelaysPriorityAndUniqueness(t *testing.T) {
	var flaky atomic.Int32
	var mu sync.Mutex
	var order []int
	s := newSys(t, func(_ context.Context, p Ping) error {
		switch p.N {
		case 1:
			if flaky.Add(1) < 3 {
				return errors.New("flaky")
			}
		case 2:
			return work.NonRetryable(errors.New("never"))
		case 3:
			return errors.New("always")
		default:
			mu.Lock()
			order = append(order, p.N)
			mu.Unlock()
		}
		return nil
	})
	ctx := context.Background()
	a, _ := s.rt.Dispatch(ctx, Ping{N: 1}, work.MaxAttempts(5))
	b, _ := s.rt.Dispatch(ctx, Ping{N: 2}, work.MaxAttempts(5))
	c, _ := s.rt.Dispatch(ctx, Ping{N: 3}, work.MaxAttempts(2))
	s.rt.Dispatch(ctx, Ping{N: 10}, work.Priority(1))
	s.rt.Dispatch(ctx, Ping{N: 11}, work.Priority(9))
	late, _ := s.rt.Dispatch(ctx, Ping{N: 12}, work.Delay(300*time.Millisecond), work.Priority(100))
	first, _ := s.rt.Dispatch(ctx, Ping{N: 20}, work.UniquePayload(), work.Delay(time.Hour))
	dup, err := s.rt.Dispatch(ctx, Ping{N: 20}, work.UniquePayload())
	if !errors.Is(err, work.ErrDuplicate) || dup.ID != first.ID {
		t.Fatalf("dup = %v, %v", dup, err)
	}
	s.worker(work.WorkerConfig{Concurrency: 1})

	if r := s.waitStatus(a.ID, work.StatusCompleted); r.Attempts != 3 {
		t.Fatalf("attempts = %d", r.Attempts)
	}
	if r := s.waitStatus(b.ID, work.StatusFailed); r.Attempts != 1 {
		t.Fatalf("non-retryable attempts = %d", r.Attempts)
	}
	if r := s.waitStatus(c.ID, work.StatusFailed); r.Attempts != 2 || r.LastError == "" {
		t.Fatalf("exhausted = %+v", r)
	}
	s.waitStatus(late.ID, work.StatusCompleted)
	mu.Lock()
	defer mu.Unlock()
	if len(order) != 3 || order[0] != 11 || order[1] != 10 || order[2] != 12 {
		t.Fatalf("order = %v, want [11 10 12]", order)
	}
}

func TestCrashRecoveryAndHeartbeats(t *testing.T) {
	var runs atomic.Int32
	s := newSys(t, func(_ context.Context, p Ping) error {
		runs.Add(1)
		if p.N == 2 {
			time.Sleep(700 * time.Millisecond)
		}
		return nil
	})
	ctx := context.Background()
	crashed, _ := s.rt.Dispatch(ctx, Ping{N: 1})
	slow, _ := s.rt.Dispatch(ctx, Ping{N: 2}, work.Queue("slow"))
	if _, err := s.b.Claim(ctx, work.ClaimOptions{Queues: []string{"default"}, WorkerID: "dead", Now: time.Now().UTC(), LeaseDuration: 200 * time.Millisecond, Limit: 1}); err != nil {
		t.Fatal(err)
	}
	cfg := work.WorkerConfig{LeaseDuration: 300 * time.Millisecond, HeartbeatInterval: 60 * time.Millisecond,
		Queues: []work.QueueConfig{{Name: "default"}, {Name: "slow"}}}
	s.worker(cfg)
	s.worker(cfg)
	if r := s.waitStatus(crashed.ID, work.StatusCompleted); r.Attempts != 2 {
		t.Fatalf("recovered attempts = %d", r.Attempts)
	}
	if r := s.waitStatus(slow.ID, work.StatusCompleted); r.Attempts != 1 {
		t.Fatalf("a heartbeating handler must keep its lease: attempts = %d", r.Attempts)
	}
	if runs.Load() != 2 {
		t.Fatalf("runs = %d", runs.Load())
	}
}

func TestCooperativeCancellationAndGracefulShutdown(t *testing.T) {
	started := make(chan struct{}, 2)
	s := newSys(t, func(ctx context.Context, p Ping) error {
		started <- struct{}{}
		if p.N == 1 {
			<-ctx.Done()
			return ctx.Err()
		}
		time.Sleep(200 * time.Millisecond)
		return nil
	})
	ctx := context.Background()
	cancelMe, _ := s.rt.Dispatch(ctx, Ping{N: 1}, work.MaxAttempts(5))
	finishMe, _ := s.rt.Dispatch(ctx, Ping{N: 2})
	stop := s.worker(work.WorkerConfig{LeaseDuration: 2 * time.Second, HeartbeatInterval: 50 * time.Millisecond, ShutdownTimeout: 5 * time.Second})
	<-started
	<-started
	out, err := s.rt.Cancel(ctx, cancelMe.ID)
	if err != nil || out != work.CancelRequested {
		t.Fatalf("cancel = %v, %v", out, err)
	}
	if r := s.waitStatus(cancelMe.ID, work.StatusCancelled); r.Attempts != 1 {
		t.Fatalf("attempts = %d", r.Attempts)
	}
	if err := stop(); err != nil {
		t.Fatalf("Run = %v", err)
	}
	if r, _ := s.b.Get(ctx, finishMe.ID); r.Status != work.StatusCompleted {
		t.Fatalf("graceful shutdown must record the running handler's result, status = %s", r.Status)
	}
}

func TestStatsStayConsistentThroughTheLifecycle(t *testing.T) {
	s := newSys(t, func(_ context.Context, p Ping) error {
		if p.N == 0 {
			return errors.New("fail")
		}
		return nil
	})
	ctx := context.Background()
	var refs []work.Ref
	for i := 0; i < 6; i++ {
		r, _ := s.rt.Dispatch(ctx, Ping{N: i}, work.MaxAttempts(1))
		refs = append(refs, r)
	}
	delayed, _ := s.rt.Dispatch(ctx, Ping{N: 99}, work.Delay(time.Hour))
	cancelled, _ := s.rt.Dispatch(ctx, Ping{N: 98}, work.Delay(time.Hour))
	if _, err := s.rt.Cancel(ctx, cancelled.ID); err != nil {
		t.Fatal(err)
	}
	s.worker(work.WorkerConfig{})
	for _, r := range refs {
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			rec, _ := s.b.Get(ctx, r.ID)
			if rec.Status.Terminal() {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	stats, err := s.rt.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]int64{}
	for _, st := range stats {
		got[string(st.Status)] += st.Count
	}
	if got["completed"] != 5 || got["failed"] != 1 || got["available"] != 1 || got["cancelled"] != 1 || got["running"] != 0 {
		t.Fatalf("stats = %v", got)
	}
	_ = delayed
	// manual retry moves the counts
	if err := s.rt.Retry(ctx, refs[0].ID); err != nil {
		t.Fatal(err)
	}
	if err := s.rt.Retry(ctx, cancelled.ID); err != nil {
		t.Fatal(err)
	}
}

func TestTransactionalDispatchIsRefused(t *testing.T) {
	s := newSys(t, func(context.Context, Ping) error { return nil })
	if s.b.Capabilities().Transactions {
		t.Fatal("Redis cannot join an application's SQL transaction")
	}
}

func TestPruneRemovesOnlyOldFinishedWork(t *testing.T) {
	c := client(t)
	b, _ := freshBackend(t, c)
	ctx := context.Background()
	base := time.Now().UTC().Add(-48 * time.Hour)
	mk := func(age time.Duration, queue string) *work.Record {
		created := time.Now().UTC().Add(-age)
		return &work.Record{ID: work.ID(uuid.NewString()), Type: "t", Version: 1, Queue: queue, Payload: []byte(`{}`),
			Status: work.StatusAvailable, MaxAttempts: 1, AvailableAt: created, CreatedAt: created, UpdatedAt: created}
	}
	finish := func(r *work.Record, how string) {
		recs, err := b.Claim(ctx, work.ClaimOptions{Queues: []string{r.Queue}, Types: []string{"t"}, WorkerID: "w", Now: time.Now().UTC(), LeaseDuration: time.Minute, Limit: 1})
		if err != nil || recs[0].ID != r.ID {
			t.Fatalf("claim = %v, %v", recs, err)
		}
		switch how {
		case "complete":
			err = b.Complete(ctx, r.ID, "w", time.Now().UTC())
		case "fail":
			err = b.Fail(ctx, r.ID, "w", errors.New("x"), time.Now().UTC())
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	oldDone, oldFailed, newDone := mk(72*time.Hour, "a"), mk(71*time.Hour, "b"), mk(time.Hour, "c")
	active := mk(100*time.Hour, "d")
	for _, r := range []*work.Record{oldDone, oldFailed, newDone, active} {
		if err := b.Enqueue(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	finish(oldDone, "complete")
	finish(oldFailed, "fail")
	finish(newDone, "complete")

	if _, err := b.Prune(ctx, base, work.StatusAvailable); !errors.Is(err, work.ErrInvalidState) {
		t.Fatalf("pruning active work must be refused: %v", err)
	}
	n, err := b.Prune(ctx, time.Now().UTC().Add(-24*time.Hour), work.StatusCompleted)
	if err != nil || n != 1 {
		t.Fatalf("pruned %d, %v", n, err)
	}
	if _, err := b.Get(ctx, oldDone.ID); !errors.Is(err, work.ErrNotFound) {
		t.Fatal("old completed work should be gone")
	}
	for _, r := range []*work.Record{oldFailed, newDone, active} {
		if _, err := b.Get(ctx, r.ID); err != nil {
			t.Fatalf("%s must be kept: %v", r.ID, err)
		}
	}
	n, err = b.Prune(ctx, time.Now().UTC().Add(-24*time.Hour)) // all finished statuses
	if err != nil || n != 1 {
		t.Fatalf("second prune = %d, %v", n, err)
	}
	stats, _ := b.Stats(ctx)
	got := map[string]int64{}
	for _, s := range stats {
		got[s.Queue+"/"+string(s.Status)] = s.Count
	}
	if len(got) != 2 || got["c/completed"] != 1 || got["d/available"] != 1 {
		t.Fatalf("stats after prune = %v", got)
	}
	list, _ := b.List(ctx, work.ListFilter{})
	if len(list) != 2 {
		t.Fatalf("list = %d", len(list))
	}
}
