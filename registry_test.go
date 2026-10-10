package work_test

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/tobibamidele/validate"
	"github.com/tobibamidele/work"
)

type ptrWork struct {
	N int `json:"n"`
}

var ptrCalls atomic.Int64

func (p *ptrWork) Handle(context.Context) error { ptrCalls.Add(int64(p.N)); return nil }

type noHandle struct{}

type validatable struct {
	N int `json:"n"`
}

func (v validatable) Handle(context.Context) error { return nil }
func (v validatable) Validate() error {
	if v.N < 0 {
		return errors.New("n must not be negative")
	}
	return nil
}

func TestRegistryRejectsBadDefinitions(t *testing.T) {
	cases := map[string]work.Definition{
		"import path as name":  work.Define("github.com/acme/jobs.SendInvoice", InvoiceSchema, func(context.Context, Invoice) error { return nil }),
		"empty name":           work.Define("", InvoiceSchema, func(context.Context, Invoice) error { return nil }),
		"spaces":               work.Define("send invoice", InvoiceSchema, func(context.Context, Invoice) error { return nil }),
		"nil handler":          work.Define[Invoice]("x", InvoiceSchema, nil),
		"nil schema":           work.Define[Invoice]("x", nil, func(context.Context, Invoice) error { return nil }),
		"version zero":         work.Define("x", InvoiceSchema, func(context.Context, Invoice) error { return nil }, work.Version(0)),
		"bad default queue":    work.Define("x", InvoiceSchema, func(context.Context, Invoice) error { return nil }, work.DefaultQueue("a b")),
		"no Handle method":     work.Declare[noHandle]("nohandle"),
		"schema of other type": work.Declare[ptrWork]("wrong", work.Schema(InvoiceSchema)),
	}
	for name, def := range cases {
		err := work.NewRegistry().Register(def)
		if !errors.Is(err, work.ErrConfig) {
			t.Errorf("%s: err = %v, want ErrConfig", name, err)
		}
	}
	if work.ValidName("send-invoice") != true || work.ValidName("billing.SendInvoice") != true || work.ValidName("a/b") {
		t.Fatal("ValidName")
	}
}

func TestRegistryDuplicatesAndVersions(t *testing.T) {
	type InvoiceV2 struct {
		InvoiceID string `json:"invoice_id"`
		Email     string `json:"email"`
		Currency  string `json:"currency"`
	}
	v2 := validate.Object[InvoiceV2](
		validate.Field("invoice_id", validate.String()), validate.Field("email", validate.String()),
		validate.Field("currency", validate.String().Min(3).Max(3)))
	h := func(context.Context, Invoice) error { return nil }

	reg := work.NewRegistry()
	if err := reg.Register(work.Define("send-invoice", InvoiceSchema, h), work.Define("send-invoice", v2, func(context.Context, InvoiceV2) error { return nil }, work.Version(2))); err != nil {
		t.Fatalf("two versions of one name must coexist: %v", err)
	}
	if got := reg.Versions("send-invoice"); len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("versions = %v", got)
	}
	if names := reg.Names(); len(names) != 1 || names[0] != "send-invoice" {
		t.Fatalf("names = %v", names)
	}
	if def, err := reg.Lookup("send-invoice", 2); err != nil || def.Version() != 2 || def.Name() != "send-invoice" || def.Fingerprint() == "" {
		t.Fatalf("lookup = %v, %v", def, err)
	}
	_, err := reg.Lookup("send-invoice", 3)
	wantIs(t, err, work.ErrUnsupportedVersion)
	var ve *work.VersionError
	if !errors.As(err, &ve) || len(ve.Supported) != 2 {
		t.Fatalf("version error = %#v", err)
	}
	_, err = reg.Lookup("nope", 1)
	wantIs(t, err, work.ErrUnknownType)

	// same name+version twice, and the same Go type under two names
	wantIs(t, reg.Register(work.Define("send-invoice", InvoiceSchema, h)), work.ErrConfig)
	wantIs(t, reg.Register(work.Define("other-name", InvoiceSchema, h)), work.ErrConfig)

	// a failed Register registers nothing from that call
	reg2 := work.NewRegistry()
	err = reg2.Register(work.Define("good", PingSchema, func(context.Context, Ping) error { return nil }),
		work.Define("", InvoiceSchema, h))
	wantIs(t, err, work.ErrConfig)
	if len(reg2.Names()) != 0 {
		t.Fatal("a failed call must be atomic")
	}
}

func TestRegistryFreezesWhenAWorkerStarts(t *testing.T) {
	e := newEnv(t, defPing("ping", func(context.Context, Ping) error { return nil }))
	stop := start(t, fastWorker(e, work.WorkerConfig{}))
	eventually(t, "freeze", func() bool {
		return errors.Is(e.reg.Register(invoiceDef(nil)), work.ErrRegistryFrozen)
	})
	_ = stop()
}

func TestDeclareUsesHandleMethodsOfEitherReceiver(t *testing.T) {
	ptrCalls.Store(0)
	e := newEnv(t, work.Declare[ptrWork]("ptr-work"))
	e.dispatch(ptrWork{N: 2})
	e.dispatch(&ptrWork{N: 3})
	start(t, fastWorker(e, work.WorkerConfig{}))
	eventually(t, "both handled", func() bool { return e.rec.count(work.EventCompleted) == 2 })
	if ptrCalls.Load() != 5 {
		t.Fatalf("calls = %d", ptrCalls.Load())
	}
}

func TestSchemaLessPayloadsUseValidateMethods(t *testing.T) {
	e := newEnv(t, work.Declare[validatable]("validatable"))
	e.dispatch(validatable{N: 1})
	_, err := e.rt.Dispatch(context.Background(), validatable{N: -1})
	wantIs(t, err, work.ErrInvalidPayload)
	mustContain(t, err.Error(), "must not be negative")
}

func TestDeclareWithSchemaAndHandleMethod(t *testing.T) {
	e := newEnv(t, work.Declare[Ping2]("ping-decl", work.Schema(Ping2Schema)))
	if _, err := e.rt.Dispatch(context.Background(), Ping2{N: -1}); err == nil {
		t.Fatal("schema constraints apply to Declare too")
	}
}

type Ping2 struct {
	N int `json:"n"`
}

var Ping2Schema = validate.Object[Ping2](validate.Field("n", validate.Int().Min(0)))

func (p Ping2) Handle(context.Context) error { return nil }

func TestSerializerBigIntegersSurvive(t *testing.T) {
	s := work.JSONSerializer{}
	var tree any
	if err := s.Unmarshal([]byte(`{"id":9007199254740993}`), &tree); err != nil {
		t.Fatal(err)
	}
	out, _ := s.Marshal(tree)
	if !bytes.Contains(out, []byte("9007199254740993")) {
		t.Fatalf("precision lost: %s", out)
	}
	if err := s.Unmarshal([]byte(`{"a":1} {"b":2}`), &tree); err == nil {
		t.Fatal("trailing data must be rejected")
	}
	if work.ContentTypeOf(s) != "application/json" {
		t.Fatal("content type")
	}
}

type bigPayload struct {
	ID int64 `json:"id"`
}

func TestLargeIntegerPayloadRoundTripsThroughValidation(t *testing.T) {
	var got int64
	s := validate.Object[bigPayload](validate.Field("id", validate.Int64().Min(0)))
	e := newEnv(t, work.Define("big", s, func(_ context.Context, p bigPayload) error { got = p.ID; return nil }))
	ref := e.dispatch(bigPayload{ID: 9007199254740993})
	start(t, fastWorker(e, work.WorkerConfig{}))
	e.waitStatus(ref.ID, work.StatusCompleted)
	if got != 9007199254740993 {
		t.Fatalf("id = %d: integers above 2^53 must survive dispatch and execution exactly", got)
	}
}

func TestMiddlewareChainOrder(t *testing.T) {
	var order []string
	mk := func(name string) work.Middleware {
		return func(next work.Handler) work.Handler {
			return func(ctx context.Context) error {
				order = append(order, name+">")
				err := next(ctx)
				order = append(order, "<"+name)
				return err
			}
		}
	}
	h := work.Chain(func(context.Context) error { order = append(order, "handler"); return nil }, mk("A"), nil, mk("B"), mk("C"))
	if err := h(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := []string{"A>", "B>", "C>", "handler", "<C", "<B", "<A"}
	if len(order) != len(want) {
		t.Fatalf("order = %v", order)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order = %v, want %v", order, want)
		}
	}
}
