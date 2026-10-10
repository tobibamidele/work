package work_test

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/tobibamidele/work"
)

// indentSerializer is a custom Serializer: pretty-printed JSON.
type indentSerializer struct{ used *int }

func (s indentSerializer) Marshal(v any) ([]byte, error) {
	*s.used++
	return json.MarshalIndent(v, "", " ")
}
func (s indentSerializer) Unmarshal(data []byte, target any) error {
	return work.JSONSerializer{}.Unmarshal(data, target)
}

func TestCustomSerializersAreUsedEndToEnd(t *testing.T) {
	used := 0
	reg := work.NewRegistry(work.WithSerializer(indentSerializer{&used}))
	var got Ping
	reg.MustRegister(work.Define("p", PingSchema, func(_ context.Context, p Ping) error { got = p; return nil }))
	if _, ok := reg.Serializer().(indentSerializer); !ok {
		t.Fatal("registry serializer")
	}
	e := newEnv(t)
	e.reg = reg
	rt, _ := work.NewRuntime(e.backend, reg, work.RuntimeConfig{})
	ref, err := rt.Dispatch(context.Background(), Ping{N: 41})
	if err != nil {
		t.Fatal(err)
	}
	rec, _ := e.backend.Get(context.Background(), ref.ID)
	if !bytes.Contains(rec.Payload, []byte("\n")) || used == 0 {
		t.Fatalf("custom serializer not used: %q (calls %d)", rec.Payload, used)
	}
	e.rt = rt
	start(t, fastWorker(e, work.WorkerConfig{}))
	e.waitStatus(ref.ID, work.StatusCompleted)
	if got.N != 41 {
		t.Fatalf("got = %+v", got)
	}

	// a definition-level serializer overrides the registry's
	used2 := 0
	reg2 := work.NewRegistry()
	reg2.MustRegister(work.Define("p", PingSchema, func(context.Context, Ping) error { return nil },
		work.DefinitionSerializer(indentSerializer{&used2}), work.DefaultPriority(4)))
	rt2, _ := work.NewRuntime(e.backend, reg2, work.RuntimeConfig{})
	if _, err := rt2.Dispatch(context.Background(), Ping{N: 1}); err != nil || used2 == 0 {
		t.Fatalf("definition serializer: %v (calls %d)", err, used2)
	}
	def, _ := reg2.Lookup("p", 1)
	if def.PayloadType().Name() != "Ping" {
		t.Fatalf("payload type = %v", def.PayloadType())
	}
}

type fakeClock struct{ t time.Time }

func (c *fakeClock) Now() time.Time { return c.t }

func TestRuntimeUsesTheInjectedClock(t *testing.T) {
	clk := &fakeClock{t: time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)}
	e := newEnv(t, defPing("p", func(context.Context, Ping) error { return nil }))
	rt, _ := work.NewRuntime(e.backend, e.reg, work.RuntimeConfig{Clock: clk})
	if rt.Clock() != clk || rt.Backend() != e.backend || rt.Registry() != e.reg {
		t.Fatal("accessors")
	}
	ref, _ := rt.Dispatch(context.Background(), Ping{N: 1}, work.Delay(time.Hour))
	rec, _ := e.backend.Get(context.Background(), ref.ID)
	if !rec.CreatedAt.Equal(clk.t) || !rec.AvailableAt.Equal(clk.t.Add(time.Hour)) {
		t.Fatalf("created %v available %v", rec.CreatedAt, rec.AvailableAt)
	}
	if work.SystemClock.Now().Location() != time.UTC {
		t.Fatal("SystemClock is UTC")
	}
	if !work.ClockFunc(func() time.Time { return clk.t }).Now().Equal(clk.t) {
		t.Fatal("ClockFunc")
	}
}

func TestPackageLevelCancelAndRetry(t *testing.T) {
	e := newEnv(t, defPing("p", func(context.Context, Ping) error { return nil }))
	ctx := work.NewContext(context.Background(), e.rt)
	ref, _ := work.Dispatch(ctx, Ping{N: 1}, work.Delay(time.Hour))
	if out, err := work.Cancel(ctx, ref.ID); err != nil || out != work.Cancelled {
		t.Fatalf("cancel = %v, %v", out, err)
	}
	if err := work.Retry(ctx, ref.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := work.Cancel(context.Background(), ref.ID); err != work.ErrNoRuntime {
		t.Fatalf("err = %v", err)
	}
	if err := work.Retry(context.Background(), ref.ID); err != work.ErrNoRuntime {
		t.Fatalf("err = %v", err)
	}
	if _, err := work.Get(context.Background(), ref.ID); err != work.ErrNoRuntime {
		t.Fatalf("err = %v", err)
	}
	if _, err := work.List(context.Background(), work.ListFilter{}); err != work.ErrNoRuntime {
		t.Fatalf("err = %v", err)
	}
	if _, err := work.DispatchAt(context.Background(), Ping{}, time.Now()); err != work.ErrNoRuntime {
		t.Fatalf("err = %v", err)
	}
}

func TestStatusHelpers(t *testing.T) {
	for _, s := range work.AllStatuses {
		if !s.Valid() {
			t.Fatalf("%s should be valid", s)
		}
	}
	if work.Status("retrying").Valid() {
		t.Fatal("there is deliberately no retrying status")
	}
	active := map[work.Status]bool{work.StatusAvailable: true, work.StatusRunning: true}
	terminal := map[work.Status]bool{work.StatusCompleted: true, work.StatusFailed: true, work.StatusCancelled: true}
	for _, s := range work.AllStatuses {
		if s.Active() != active[s] || s.Terminal() != terminal[s] {
			t.Fatalf("%s: active=%v terminal=%v", s, s.Active(), s.Terminal())
		}
	}
	if work.Cancelled.String() != "cancelled" || work.CancelRequested.String() != "cancel_requested" || work.CancelOutcome(0).String() != "unknown" {
		t.Fatal("CancelOutcome.String")
	}
	r := &work.Record{Payload: []byte("a"), Metadata: map[string]string{"k": "v"}}
	c := r.Clone()
	c.Payload[0] = 'z'
	c.Metadata["k"] = "changed"
	if r.Payload[0] != 'a' || r.Metadata["k"] != "v" {
		t.Fatal("Clone must be deep")
	}
	if (*work.Record)(nil).Clone() != nil {
		t.Fatal("nil clone")
	}
	exec := &work.ExecutionError{WorkID: "i", Type: "t", Version: 1, Attempt: 2, WorkerID: "w", Err: context.Canceled}
	mustContain(t, exec.Error(), "attempt 2", "canceled")
	_ = (&work.DuplicateError{Queue: "q", UniqueKey: "k", ExistingID: "i"}).Error()
}
