package work

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"time"

	"github.com/tobibamidele/validate"
)

// Work is the ergonomic contract for a unit of durable work: ordinary Go with
// the idiomatic handler signature.
//
//	type SendInvoice struct {
//	    InvoiceID string `json:"invoice_id"`
//	}
//
//	func (j SendInvoice) Handle(ctx context.Context) error { ... }
//
// A type becomes dispatchable once it is declared with Declare (or defined
// explicitly with Define) and registered. Payloads are serialized with
// encoding/json by default, so json tags must produce the field names of the
// payload schema.
type Work interface {
	Handle(ctx context.Context) error
}

// Validatable can be implemented by payloads that have no schema. Validate is
// then called at dispatch and again before execution.
type Validatable interface {
	Validate() error
}

// Definition is the runtime description of one work type at one version: its
// stable name, payload type, schema, handler and serializer. Create one with
// Define or Declare and register it with Registry.Register.
type Definition interface {
	// Name is the stable identifier persisted with every record.
	Name() string
	// Version is the payload contract version.
	Version() int
	// PayloadType is the Go type dispatched for this definition.
	PayloadType() reflect.Type
	// Fingerprint is Validate's schema fingerprint, or "" when unavailable.
	Fingerprint() string

	definition() *definition
}

// definition is the type-erased form the engine works with. The closures
// capture the payload type T, so neither dispatch nor execution needs
// reflection to move a value through validation, serialization and the
// handler.
type definition struct {
	name        string
	version     int
	typ         reflect.Type
	fingerprint string
	err         error // definition-time error, reported by Register

	serializer Serializer // nil: the registry default

	queue       string
	maxAttempts int
	priority    int
	timeout     time.Duration
	backoff     Backoff

	// encode validates and normalizes payload (already of type T) and returns
	// the bytes to persist.
	encode func(ctx context.Context, ser Serializer, payload any) ([]byte, error)
	// decode deserializes and validates stored bytes and binds the handler.
	decode func(ctx context.Context, ser Serializer, data []byte) (Handler, error)
}

func (d *definition) Name() string              { return d.name }
func (d *definition) Version() int              { return d.version }
func (d *definition) PayloadType() reflect.Type { return d.typ }
func (d *definition) Fingerprint() string       { return d.fingerprint }
func (d *definition) definition() *definition   { return d }

var nameRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,254}$`)

// ValidName reports whether name is an acceptable stable work type name. Go
// import paths and fully qualified type names are rejected because identity
// must survive refactors.
func ValidName(name string) bool { return nameRE.MatchString(name) }

// DefinitionOption configures a Definition.
type DefinitionOption func(*defConfig)

type defConfig struct {
	version     int
	queue       string
	maxAttempts int
	priority    int
	timeout     time.Duration
	backoff     Backoff
	serializer  Serializer
	schema      any
}

// Version sets the payload contract version. Default 1. Bump it when the
// payload changes incompatibly; both versions can be registered side by side
// (with distinct Go types) while old work drains.
func Version(v int) DefinitionOption { return func(c *defConfig) { c.version = v } }

// DefaultQueue sets the queue used when Dispatch gets no Queue option.
func DefaultQueue(q string) DefinitionOption { return func(c *defConfig) { c.queue = q } }

// DefaultMaxAttempts sets the attempt limit used when Dispatch gets no
// MaxAttempts option.
func DefaultMaxAttempts(n int) DefinitionOption { return func(c *defConfig) { c.maxAttempts = n } }

// DefaultPriority sets the priority used when Dispatch gets no Priority.
func DefaultPriority(p int) DefinitionOption { return func(c *defConfig) { c.priority = p } }

// DefaultTimeout sets the execution timeout used when Dispatch gets no
// Timeout option.
func DefaultTimeout(d time.Duration) DefinitionOption { return func(c *defConfig) { c.timeout = d } }

// DefaultBackoff sets the retry backoff for this work type, overriding the
// worker's. A backoff is code, not data, so it is configured per definition
// and per worker rather than per dispatch.
func DefaultBackoff(b Backoff) DefinitionOption { return func(c *defConfig) { c.backoff = b } }

// DefinitionSerializer overrides the registry's serializer for this type.
func DefinitionSerializer(s Serializer) DefinitionOption {
	return func(c *defConfig) { c.serializer = s }
}

// Schema attaches the Validate schema of the payload to a Declare. The type
// parameter is checked against the declared payload type when the definition
// is built; a mismatch is reported by Registry.Register.
func Schema[T any](s validate.Schema[T]) DefinitionOption {
	return func(c *defConfig) { c.schema = s }
}

func newConfig(opts []DefinitionOption) defConfig {
	c := defConfig{version: 1}
	for _, o := range opts {
		if o != nil {
			o(&c)
		}
	}
	return c
}

func configError(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrConfig, fmt.Sprintf(format, args...))
}

func (c defConfig) check(name string) error {
	switch {
	case !ValidName(name):
		return configError("work type name %q is invalid: use a short stable identifier such as \"send-invoice\" (letters, digits, . _ : -), never a Go import path or type name", name)
	case c.version < 1:
		return configError("work type %q: version must be >= 1, got %d", name, c.version)
	case c.maxAttempts < 0:
		return configError("work type %q: default max attempts must not be negative", name)
	case c.timeout < 0:
		return configError("work type %q: default timeout must not be negative", name)
	case c.queue != "" && !validQueueName(c.queue):
		return configError("work type %q: invalid default queue %q", name, c.queue)
	}
	return nil
}

func (c defConfig) apply(d *definition) {
	d.version = c.version
	d.queue = c.queue
	d.maxAttempts = c.maxAttempts
	d.priority = c.priority
	d.timeout = c.timeout
	d.backoff = c.backoff
	d.serializer = c.serializer
}

// Define builds a Definition from an explicit schema and handler function.
// This is the form for advanced use; most code uses Declare with a Handle
// method.
//
//	work.Define("send-welcome-email", SendWelcomeEmailSchema,
//	    func(ctx context.Context, p SendWelcomeEmail) error { ... })
//
// The schema is the payload contract. It is applied at dispatch (the
// validated, normalized value is what gets persisted) and again before the
// handler runs.
func Define[T any](name string, schema validate.Schema[T], handler func(ctx context.Context, payload T) error, opts ...DefinitionOption) Definition {
	cfg := newConfig(opts)
	d := &definition{name: name, typ: reflect.TypeOf((*T)(nil)).Elem()}
	cfg.apply(d)
	switch {
	case cfg.check(name) != nil:
		d.err = cfg.check(name)
	case schema == nil:
		d.err = configError("work type %q: Define needs a schema; use Declare for payloads without one", name)
	case handler == nil:
		d.err = configError("work type %q: handler must not be nil", name)
	default:
		bind := func(v T) Handler { return func(ctx context.Context) error { return handler(ctx, v) } }
		build(d, schema, bind)
	}
	return d
}

// Declare builds a Definition for a payload type T that implements Work (its
// Handle method is the handler, with a value or pointer receiver).
//
//	work.Declare[SendInvoice]("send-invoice", work.Schema(SendInvoiceSchema))
//
// Without a Schema option the payload is validated structurally (it must
// serialize and deserialize) and by its Validate method if it has one.
func Declare[T any](name string, opts ...DefinitionOption) Definition {
	cfg := newConfig(opts)
	d := &definition{name: name, typ: reflect.TypeOf((*T)(nil)).Elem()}
	cfg.apply(d)
	if err := cfg.check(name); err != nil {
		d.err = err
		return d
	}
	var zero T
	_, byValue := any(zero).(Work)
	_, byPointer := any(&zero).(Work)
	if !byValue && !byPointer {
		d.err = configError("work type %q: %s must have a Handle(context.Context) error method", name, d.typ)
		return d
	}
	bind := func(v T) Handler {
		if w, ok := any(v).(Work); ok {
			return w.Handle
		}
		return any(&v).(Work).Handle
	}
	var schema validate.Schema[T]
	if cfg.schema != nil {
		s, ok := cfg.schema.(validate.Schema[T])
		if !ok {
			d.err = configError("work type %q: the schema is %T, which does not produce %s", name, cfg.schema, d.typ)
			return d
		}
		schema = s
	}
	build(d, schema, bind)
	return d
}

// build installs the typed encode/decode closures. schema may be nil.
func build[T any](d *definition, schema validate.Schema[T], bind func(T) Handler) {
	if schema != nil {
		if fp, ok := validate.Fingerprint(schema); ok {
			d.fingerprint = fp
		}
	}
	name, version := d.name, d.version
	payloadErr := func(phase Phase, err error) error {
		return &PayloadError{Phase: phase, Type: name, Version: version, Err: err}
	}

	d.encode = func(ctx context.Context, ser Serializer, payload any) ([]byte, error) {
		v, ok := payload.(T)
		if !ok {
			return nil, payloadErr(PhaseDispatch, fmt.Errorf("expected %s, got %T", d.typ, payload))
		}
		raw, err := ser.Marshal(v)
		if err != nil {
			return nil, payloadErr(PhaseDispatch, fmt.Errorf("serialize: %w", err))
		}
		if schema == nil {
			if err := checkValidatable(v); err != nil {
				return nil, payloadErr(PhaseDispatch, err)
			}
			// Prove the worker will be able to read what is stored.
			var back T
			if err := ser.Unmarshal(raw, &back); err != nil {
				return nil, payloadErr(PhaseDispatch, fmt.Errorf("payload does not survive serialization: %w", err))
			}
			return raw, nil
		}

		tree, err := toTree(ser, raw)
		if err != nil {
			return nil, payloadErr(PhaseDispatch, err)
		}
		// Fields the schema does not declare would be silently dropped by
		// validation; fail loudly instead (usually a missing json tag).
		if info, ok := validate.InspectObject(schema); ok {
			if err := checkDeclared(tree, info, nil); err != nil {
				return nil, payloadErr(PhaseDispatch, err)
			}
		}
		norm, err := validate.ParseContext(ctx, schema, tree)
		if err != nil {
			return nil, schemaError(PhaseDispatch, name, version, err)
		}
		stored, err := ser.Marshal(norm)
		if err != nil {
			return nil, payloadErr(PhaseDispatch, fmt.Errorf("serialize normalized payload: %w", err))
		}
		// What is stored must itself satisfy the schema, or the worker would
		// reject it as corrupted. This catches non-idempotent normalizers and
		// json tags that disagree with the schema's field names.
		tree2, err := toTree(ser, stored)
		if err != nil {
			return nil, payloadErr(PhaseDispatch, err)
		}
		if _, err := validate.ParseContext(ctx, schema, tree2); err != nil {
			return nil, schemaError(PhaseDispatch, name, version, fmt.Errorf("normalized payload does not satisfy its own schema (check that json tags match the schema's field names): %w", err))
		}
		return stored, nil
	}

	d.decode = func(ctx context.Context, ser Serializer, data []byte) (Handler, error) {
		var v T
		if schema == nil {
			if err := ser.Unmarshal(data, &v); err != nil {
				return nil, payloadErr(PhaseExecute, fmt.Errorf("deserialize: %w", err))
			}
			if err := checkValidatable(v); err != nil {
				return nil, payloadErr(PhaseExecute, err)
			}
			return bind(v), nil
		}
		tree, err := toTree(ser, data)
		if err != nil {
			return nil, payloadErr(PhaseExecute, err)
		}
		v, err = validate.ParseContext(ctx, schema, tree)
		if err != nil {
			return nil, schemaError(PhaseExecute, name, version, err)
		}
		return bind(v), nil
	}
}

func checkValidatable(v any) error {
	if val, ok := v.(Validatable); ok {
		return val.Validate()
	}
	return nil
}

// schemaError classifies a validation failure. Context cancellation is not a
// payload problem and is passed through.
func schemaError(phase Phase, name string, version int, err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return &PayloadError{Phase: phase, Type: name, Version: version, Err: err}
}

func toTree(ser Serializer, data []byte) (any, error) {
	var tree any
	if err := ser.Unmarshal(data, &tree); err != nil {
		return nil, fmt.Errorf("deserialize: %w", err)
	}
	return tree, nil
}

// checkDeclared reports payload keys that the schema does not declare.
func checkDeclared(tree any, info validate.ObjectInfo, path []string) error {
	m, ok := tree.(map[string]any)
	if !ok {
		return nil
	}
	declared := make(map[string]*validate.ObjectInfo, len(info.Fields))
	for i := range info.Fields {
		declared[info.Fields[i].Name] = info.Fields[i].Nested
	}
	for k, v := range m {
		nested, ok := declared[k]
		if !ok {
			p := append(append([]string(nil), path...), k)
			return fmt.Errorf("payload field %q is not declared in the schema and would be dropped; add it to the schema or fix the json tag", joinPath(p))
		}
		if nested != nil {
			if err := checkDeclared(v, *nested, append(append([]string(nil), path...), k)); err != nil {
				return err
			}
		}
	}
	return nil
}

func joinPath(p []string) string {
	out := ""
	for i, s := range p {
		if i > 0 {
			out += "."
		}
		out += s
	}
	return out
}
