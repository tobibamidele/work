package work_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/tobibamidele/validate"
	"github.com/tobibamidele/work"
)

func invoiceDef(fn func(context.Context, Invoice) error, opts ...work.DefinitionOption) work.Definition {
	if fn == nil {
		fn = func(context.Context, Invoice) error { return nil }
	}
	return work.Define("send-invoice", InvoiceSchema, fn, opts...)
}

func TestDispatchPersistsValidatedNormalizedPayload(t *testing.T) {
	e := newEnv(t, invoiceDef(nil, work.DefaultQueue("emails"), work.DefaultMaxAttempts(7)))
	ref := e.dispatch(Invoice{InvoiceID: "  INV-1  ", Email: "  Billing@Example.COM "})
	rec := e.get(ref.ID)
	var stored map[string]any
	if err := json.Unmarshal(rec.Payload, &stored); err != nil {
		t.Fatal(err)
	}
	if stored["invoice_id"] != "INV-1" || stored["email"] != "billing@example.com" {
		t.Fatalf("the normalized value must be persisted, got %s", rec.Payload)
	}
	if rec.Type != "send-invoice" || rec.Version != 1 || rec.Queue != "emails" || rec.MaxAttempts != 7 || rec.Status != work.StatusAvailable {
		t.Fatalf("record = %+v", rec)
	}
	if rec.SchemaFingerprint == "" {
		t.Fatal("the schema fingerprint should be stored when Validate exposes one")
	}
	if ref.ID != rec.ID || ref.Type != "send-invoice" || ref.Queue != "emails" || ref.String() != string(rec.ID) {
		t.Fatalf("ref = %+v", ref)
	}
}

func TestInvalidPayloadIsRejectedAndNothingIsPersisted(t *testing.T) {
	e := newEnv(t, invoiceDef(nil))
	_, err := e.rt.Dispatch(context.Background(), Invoice{InvoiceID: "", Email: "not-an-email"})
	wantIs(t, err, work.ErrInvalidPayload)
	if !validate.IsValidationError(err) {
		t.Fatalf("the Validate error must stay reachable, got %T", err)
	}
	var pe *work.PayloadError
	if !errors.As(err, &pe) || pe.Phase != work.PhaseDispatch || pe.Type != "send-invoice" {
		t.Fatalf("payload error = %#v", err)
	}
	recs, _ := e.backend.List(context.Background(), work.ListFilter{})
	if len(recs) != 0 {
		t.Fatalf("an invalid payload must not be persisted, found %d records", len(recs))
	}
	if e.rec.count(work.EventEnqueued) != 0 {
		t.Fatal("no enqueued event for rejected work")
	}
}

func TestFieldsMissingFromTheSchemaFailLoudly(t *testing.T) {
	type Loose struct {
		A string `json:"a"`
		B string `json:"b"` // not in the schema: would be silently dropped
	}
	s := validate.Object[Loose](validate.Field("a", validate.String()))
	e := newEnv(t, work.Define("loose", s, func(context.Context, Loose) error { return nil }))
	_, err := e.rt.Dispatch(context.Background(), Loose{A: "x", B: "y"})
	wantIs(t, err, work.ErrInvalidPayload)
	mustContain(t, err.Error(), `"b"`, "not declared in the schema")
}

func TestJSONTagsThatDisagreeWithTheSchemaFailLoudly(t *testing.T) {
	type Untagged struct{ InvoiceID string } // encodes as "InvoiceID"
	s := validate.Object[Untagged](validate.Field("invoice_id", validate.String()))
	e := newEnv(t, work.Define("untagged", s, func(context.Context, Untagged) error { return nil }))
	_, err := e.rt.Dispatch(context.Background(), Untagged{InvoiceID: "x"})
	wantIs(t, err, work.ErrInvalidPayload)
	mustContain(t, err.Error(), "InvoiceID")
}

func TestNestedUndeclaredFieldIsReportedWithItsPath(t *testing.T) {
	type Inner struct {
		X int `json:"x"`
		Y int `json:"y"`
	}
	type Outer struct {
		In Inner `json:"in"`
	}
	inner := validate.Object[Inner](validate.Field("x", validate.Int()))
	outer := validate.Object[Outer](validate.Field("in", inner))
	e := newEnv(t, work.Define("nested", outer, func(context.Context, Outer) error { return nil }))
	_, err := e.rt.Dispatch(context.Background(), Outer{In: Inner{X: 1, Y: 2}})
	wantIs(t, err, work.ErrInvalidPayload)
	mustContain(t, err.Error(), `"in.y"`)
}

func TestUnknownPayloadTypeIsRejected(t *testing.T) {
	e := newEnv(t, invoiceDef(nil))
	_, err := e.rt.Dispatch(context.Background(), Ping{N: 1})
	wantIs(t, err, work.ErrUnknownType)
	_, err = e.rt.Dispatch(context.Background(), nil)
	wantIs(t, err, work.ErrInvalidPayload)
	var nilPtr *Invoice
	_, err = e.rt.Dispatch(context.Background(), nilPtr)
	wantIs(t, err, work.ErrInvalidPayload)
}

func TestPointerPayloadsAreAccepted(t *testing.T) {
	e := newEnv(t, invoiceDef(nil))
	ref := e.dispatch(&Invoice{InvoiceID: "p", Email: "a@b.co"})
	if e.get(ref.ID).Type != "send-invoice" {
		t.Fatal("pointer payload should resolve to the value definition")
	}
}

func TestDispatchOptionsAreAppliedAndValidated(t *testing.T) {
	e := newEnv(t, invoiceDef(nil))
	inv := Invoice{InvoiceID: "1", Email: "a@b.co"}

	ref := e.dispatch(inv, work.Queue("billing"), work.MaxAttempts(9), work.Priority(5),
		work.Timeout(90*time.Second), work.IdempotencyKey("idem"), work.DispatchMetadata("tenant", "t1"), work.Delay(time.Hour))
	rec := e.get(ref.ID)
	if rec.Queue != "billing" || rec.MaxAttempts != 9 || rec.Priority != 5 || rec.Timeout != 90*time.Second ||
		rec.IdempotencyKey != "idem" || rec.Metadata["tenant"] != "t1" {
		t.Fatalf("record = %+v", rec)
	}
	if d := time.Until(rec.AvailableAt); d < 59*time.Minute || d > 61*time.Minute {
		t.Fatalf("delay not applied, available in %s", d)
	}

	at := time.Now().Add(3 * time.Hour).UTC().Truncate(time.Second)
	ref, err := e.rt.DispatchAt(context.Background(), inv, at)
	if err != nil {
		t.Fatal(err)
	}
	if !e.get(ref.ID).AvailableAt.Equal(at) {
		t.Fatalf("DispatchAt: available_at = %v, want %v", e.get(ref.ID).AvailableAt, at)
	}

	bad := map[string][]work.DispatchOption{
		"negative delay":     {work.Delay(-time.Second)},
		"zero max attempts":  {work.MaxAttempts(0)},
		"bad queue":          {work.Queue("has space")},
		"empty queue":        {work.Queue("")},
		"empty unique":       {work.Unique("")},
		"negative timeout":   {work.Timeout(-1)},
		"delay and at":       {work.Delay(time.Second), work.At(time.Now().Add(time.Hour))},
		"zero at":            {work.At(time.Time{})},
		"empty metadata key": {work.DispatchMetadata("", "v")},
	}
	for name, opts := range bad {
		if _, err := e.rt.Dispatch(context.Background(), inv, opts...); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
}

func TestUniqueKeysThroughDispatch(t *testing.T) {
	e := newEnv(t, invoiceDef(nil))
	inv := Invoice{InvoiceID: "1", Email: "a@b.co"}
	first := e.dispatch(inv, work.Unique("invoice:1"))
	ref, err := e.rt.Dispatch(context.Background(), inv, work.Unique("invoice:1"))
	wantIs(t, err, work.ErrDuplicate)
	if ref.ID != first.ID {
		t.Fatalf("the duplicate's Ref must identify the active work: %s != %s", ref.ID, first.ID)
	}
	var de *work.DuplicateError
	if !errors.As(err, &de) || de.UniqueKey != "invoice:1" {
		t.Fatalf("err = %#v", err)
	}
	e.dispatch(inv, work.Unique("invoice:1"), work.Queue("other")) // scoped per queue
	e.dispatch(inv, work.Unique("invoice:2"))
	e.dispatch(inv) // no key, never blocked
	e.dispatch(inv)
}

func TestUniquePayloadDerivesTheKeyFromTheNormalizedPayload(t *testing.T) {
	e := newEnv(t, invoiceDef(nil))
	a := e.dispatch(Invoice{InvoiceID: "1", Email: "A@B.CO"}, work.UniquePayload())
	_, err := e.rt.Dispatch(context.Background(), Invoice{InvoiceID: " 1 ", Email: "a@b.co"}, work.UniquePayload())
	wantIs(t, err, work.ErrDuplicate) // equal after normalization
	e.dispatch(Invoice{InvoiceID: "2", Email: "a@b.co"}, work.UniquePayload())
	if got := e.get(a.ID).UniqueKey; len(got) == 0 || len(got) > 255 {
		t.Fatalf("derived key = %q", got)
	}
}

func TestDispatchRefusesUnsupportedCapabilities(t *testing.T) {
	caps := work.Capabilities{Name: "tiny"}
	e := newEnvWith(t, &wrapped{Backend: newEnv(t).backend, caps: &caps}, invoiceDef(nil))
	inv := Invoice{InvoiceID: "1", Email: "a@b.co"}
	for name, opt := range map[string]work.DispatchOption{
		"delay": work.Delay(time.Minute), "priority": work.Priority(1), "unique": work.Unique("k"),
	} {
		if _, err := e.rt.Dispatch(context.Background(), inv, opt); !errors.Is(err, work.ErrUnsupported) {
			t.Errorf("%s: err = %v, want ErrUnsupported", name, err)
		}
	}
	if _, err := e.rt.Cancel(context.Background(), "x"); !errors.Is(err, work.ErrUnsupported) {
		t.Errorf("cancel: err = %v", err)
	}
	e.dispatch(inv) // plain dispatch still works
}

func TestPackageLevelDispatchResolvesTheRuntime(t *testing.T) {
	e := newEnv(t, invoiceDef(nil))
	inv := Invoice{InvoiceID: "1", Email: "a@b.co"}

	work.SetDefault(nil)
	if _, err := work.Dispatch(context.Background(), inv); !errors.Is(err, work.ErrNoRuntime) {
		t.Fatalf("err = %v, want ErrNoRuntime", err)
	}
	ctx := work.NewContext(context.Background(), e.rt)
	ref, err := work.Dispatch(ctx, inv)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := work.Get(ctx, ref.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := work.DispatchAt(ctx, inv, time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}

	work.SetDefault(e.rt)
	defer work.SetDefault(nil)
	if work.Default() != e.rt {
		t.Fatal("Default")
	}
	if _, err := work.Dispatch(context.Background(), inv); err != nil {
		t.Fatalf("default runtime: %v", err)
	}
	list, err := work.List(context.Background(), work.ListFilter{})
	if err != nil || len(list) != 3 {
		t.Fatalf("list = %d, %v", len(list), err)
	}
}

func TestTraceContextIsPropagatedThroughMetadata(t *testing.T) {
	type key struct{}
	prop := tracePropagator{key: key{}}
	reg := work.NewRegistry()
	reg.MustRegister(invoiceDef(nil))
	rt, err := work.NewRuntime(newEnv(t).backend, reg, work.RuntimeConfig{Propagator: prop})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.WithValue(context.Background(), key{}, "trace-abc")
	ref, err := rt.Dispatch(ctx, Invoice{InvoiceID: "1", Email: "a@b.co"})
	if err != nil {
		t.Fatal(err)
	}
	rec, _ := rt.Get(ctx, ref.ID)
	if rec.Metadata["traceparent"] != "trace-abc" {
		t.Fatalf("metadata = %v", rec.Metadata)
	}
}

type tracePropagator struct{ key any }

func (p tracePropagator) Inject(ctx context.Context, c map[string]string) {
	if v, ok := ctx.Value(p.key).(string); ok {
		c["traceparent"] = v
	}
}
func (p tracePropagator) Extract(ctx context.Context, c map[string]string) context.Context {
	return context.WithValue(ctx, p.key, c["traceparent"])
}
