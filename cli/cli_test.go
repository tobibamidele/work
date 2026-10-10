package cli_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/tobibamidele/validate"
	"github.com/tobibamidele/work"
	"github.com/tobibamidele/work/cli"
	"github.com/tobibamidele/work/memory"
)

type P struct {
	Secret string `json:"secret"`
}

func setup(t *testing.T) (*work.Runtime, *memory.Backend) {
	t.Helper()
	reg := work.NewRegistry()
	reg.MustRegister(work.Define("p", validate.Object[P](validate.Field("secret", validate.String())),
		func(context.Context, P) error { return nil }))
	b := memory.New()
	rt, err := work.NewRuntime(b, reg, work.RuntimeConfig{})
	if err != nil {
		t.Fatal(err)
	}
	return rt, b
}

func run(t *testing.T, rt *work.Runtime, args ...string) (string, error) {
	t.Helper()
	var buf bytes.Buffer
	err := cli.Run(context.Background(), rt, args, &buf)
	return buf.String(), err
}

func failWork(t *testing.T, rt *work.Runtime, b *memory.Backend, ref work.Ref) {
	t.Helper()
	ctx := context.Background()
	recs, err := b.Claim(ctx, work.ClaimOptions{Queues: []string{ref.Queue}, WorkerID: "w", Now: time.Now().UTC(), LeaseDuration: time.Minute, Limit: 1})
	if err != nil || len(recs) != 1 {
		t.Fatal(err)
	}
	if err := b.Fail(ctx, ref.ID, "w", errors.New("smtp refused\nsecond line"), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
}

func TestInspectHidesPayloadsUnlessAsked(t *testing.T) {
	rt, _ := setup(t)
	ref, _ := rt.Dispatch(context.Background(), P{Secret: "hunter2"}, work.Unique("k"), work.IdempotencyKey("idem"), work.Timeout(time.Minute))
	out, err := run(t, rt, "inspect", string(ref.ID))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out, "hunter2") {
		t.Fatalf("payload shown without --payload:\n%s", out)
	}
	for _, want := range []string{string(ref.ID), "p (v1)", "available", "0 of 3", "unique key", "idempotency key", "timeout"} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q:\n%s", want, out)
		}
	}
	out, err = run(t, rt, "inspect", string(ref.ID), "--payload")
	if err != nil || !strings.Contains(out, "hunter2") {
		t.Fatalf("--payload: %v\n%s", err, out)
	}
	out, err = run(t, rt, "inspect", "--payload", string(ref.ID))
	if err != nil || !strings.Contains(out, "hunter2") {
		t.Fatalf("flags before the id: %v\n%s", err, out)
	}
}

func TestFailedRetryCancelAndStats(t *testing.T) {
	rt, b := setup(t)
	ctx := context.Background()
	ref, _ := rt.Dispatch(ctx, P{Secret: "x"})
	failWork(t, rt, b, ref)

	out, err := run(t, rt, "failed")
	if err != nil || !strings.Contains(out, string(ref.ID)) || !strings.Contains(out, "smtp refused") || strings.Contains(out, "second line") {
		t.Fatalf("failed list: %v\n%s", err, out)
	}
	out, _ = run(t, rt, "inspect", string(ref.ID))
	if !strings.Contains(out, "last error:") || !strings.Contains(out, "second line") {
		t.Fatalf("inspect shows the whole error:\n%s", out)
	}
	if out, err = run(t, rt, "list", "--status", "failed", "--queue", "default", "--type", "p"); err != nil || !strings.Contains(out, string(ref.ID)) {
		t.Fatalf("list: %v\n%s", err, out)
	}
	if out, _ = run(t, rt, "list", "--status", "completed"); !strings.Contains(out, "no work found") {
		t.Fatalf("empty list:\n%s", out)
	}
	if _, err = run(t, rt, "list", "--status", "bogus"); err == nil {
		t.Fatal("unknown status")
	}
	if out, err = run(t, rt, "stats"); err != nil || !strings.Contains(out, "failed") {
		t.Fatalf("stats: %v\n%s", err, out)
	}

	if out, err = run(t, rt, "retry", string(ref.ID)); err != nil || !strings.Contains(out, "available again") {
		t.Fatalf("retry: %v\n%s", err, out)
	}
	if rec, _ := rt.Get(ctx, ref.ID); rec.Status != work.StatusAvailable || rec.Attempts != 0 {
		t.Fatalf("record = %+v", rec)
	}
	if _, err = run(t, rt, "retry", string(ref.ID)); err == nil || !strings.Contains(err.Error(), "only failed or cancelled") {
		t.Fatalf("retrying available work: %v", err)
	}
	if out, err = run(t, rt, "cancel", string(ref.ID)); err != nil || !strings.Contains(out, "cancelled") {
		t.Fatalf("cancel: %v\n%s", err, out)
	}
}

func TestCancelRunningAndErrorsAreActionable(t *testing.T) {
	rt, b := setup(t)
	ctx := context.Background()
	ref, _ := rt.Dispatch(ctx, P{Secret: "x"})
	if _, err := b.Claim(ctx, work.ClaimOptions{Queues: []string{"default"}, WorkerID: "w", Now: time.Now().UTC(), LeaseDuration: time.Minute, Limit: 1}); err != nil {
		t.Fatal(err)
	}
	out, err := run(t, rt, "cancel", string(ref.ID))
	if err != nil || !strings.Contains(out, "asked to stop") {
		t.Fatalf("cancel running: %v\n%s", err, out)
	}
	if _, err = run(t, rt, "inspect", "missing"); err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("missing: %v", err)
	}
	if err = b.Complete(ctx, ref.ID, "w", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if _, err = run(t, rt, "cancel", string(ref.ID)); err == nil || !strings.Contains(err.Error(), "already completed") {
		t.Fatalf("completed: %v", err)
	}
}

func TestUsage(t *testing.T) {
	rt, _ := setup(t)
	if out, err := run(t, rt); !errors.Is(err, cli.ErrUsage) || !strings.Contains(out, "usage:") {
		t.Fatalf("no args: %v", err)
	}
	if out, err := run(t, rt, "help"); err != nil || !strings.Contains(out, "inspect") {
		t.Fatalf("help: %v", err)
	}
	for _, args := range [][]string{{"bogus"}, {"retry"}, {"retry", "a", "b"}, {"cancel", "--x"}, {"inspect"}, {"inspect", "a", "--nope"}, {"list", "extra"}, {"list", "--limit", "0"}} {
		if _, err := run(t, rt, args...); !errors.Is(err, cli.ErrUsage) {
			t.Errorf("%v: err = %v, want ErrUsage", args, err)
		}
	}
}
