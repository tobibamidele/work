package work

import (
	"fmt"
	"reflect"
	"sort"
	"sync"
)

type defKey struct {
	name    string
	version int
}

// Registry maps stable work identity (type name + version) to Definitions.
// It is application-owned: there is no global registry.
//
// Register everything during startup. Starting a Worker freezes the registry;
// later registration returns ErrRegistryFrozen.
type Registry struct {
	mu     sync.RWMutex
	frozen bool
	byKey  map[defKey]*definition
	byType map[reflect.Type]*definition
	byName map[string][]int
	ser    Serializer
}

// RegistryOption configures NewRegistry.
type RegistryOption func(*Registry)

// WithSerializer sets the default serializer for definitions that do not
// bring their own. The default is JSONSerializer.
func WithSerializer(s Serializer) RegistryOption {
	return func(r *Registry) {
		if s != nil {
			r.ser = s
		}
	}
}

// NewRegistry creates an empty registry.
func NewRegistry(opts ...RegistryOption) *Registry {
	r := &Registry{
		byKey:  map[defKey]*definition{},
		byType: map[reflect.Type]*definition{},
		byName: map[string][]int{},
		ser:    JSONSerializer{},
	}
	for _, o := range opts {
		o(r)
	}
	return r
}

// Register adds definitions. It fails, registering nothing from that call,
// on any configuration error: an invalid definition, a duplicate name and
// version, or a payload type that is already registered.
func (r *Registry) Register(defs ...Definition) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.frozen {
		return fmt.Errorf("%w: register definitions before starting workers", ErrRegistryFrozen)
	}
	staged := make([]*definition, 0, len(defs))
	seenKey := map[defKey]bool{}
	seenType := map[reflect.Type]bool{}
	for _, def := range defs {
		if def == nil {
			return configError("nil definition")
		}
		d := def.definition()
		if d.err != nil {
			return d.err
		}
		k := defKey{d.name, d.version}
		if _, dup := r.byKey[k]; dup || seenKey[k] {
			return configError("work type %q version %d is registered twice", d.name, d.version)
		}
		if prev, dup := r.byType[d.typ]; dup || seenType[d.typ] {
			name := d.name
			if prev != nil {
				name = prev.name
			}
			return configError("payload type %s is already registered as %q; each version of a work type needs its own Go type", d.typ, name)
		}
		seenKey[k], seenType[d.typ] = true, true
		staged = append(staged, d)
	}
	for _, d := range staged {
		r.byKey[defKey{d.name, d.version}] = d
		r.byType[d.typ] = d
		r.byName[d.name] = append(r.byName[d.name], d.version)
		sort.Ints(r.byName[d.name])
	}
	return nil
}

// MustRegister is Register that panics on error, for use in startup code.
func (r *Registry) MustRegister(defs ...Definition) {
	if err := r.Register(defs...); err != nil {
		panic(err)
	}
}

// Freeze forbids further registration. Workers call it when they start.
func (r *Registry) Freeze() {
	r.mu.Lock()
	r.frozen = true
	r.mu.Unlock()
}

// Lookup resolves a persisted (name, version) to its definition. It returns
// ErrUnknownType for an unregistered name and a *VersionError (matching
// ErrUnsupportedVersion) for a registered name at an unregistered version.
func (r *Registry) Lookup(name string, version int) (Definition, error) {
	d, err := r.lookup(name, version)
	if err != nil {
		return nil, err
	}
	return d, nil
}

func (r *Registry) lookup(name string, version int) (*definition, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if d, ok := r.byKey[defKey{name, version}]; ok {
		return d, nil
	}
	if versions, ok := r.byName[name]; ok {
		return nil, &VersionError{Type: name, Version: version, Supported: append([]int(nil), versions...)}
	}
	return nil, fmt.Errorf("%w: %q is not registered", ErrUnknownType, name)
}

// lookupValue finds the definition for a payload value, following one level of
// pointer so Dispatch(&x) works for value-registered types. It returns the
// value to encode.
func (r *Registry) lookupValue(payload any) (*definition, any, error) {
	if payload == nil {
		return nil, nil, fmt.Errorf("%w: payload is nil", ErrInvalidPayload)
	}
	t := reflect.TypeOf(payload)
	r.mu.RLock()
	d, ok := r.byType[t]
	r.mu.RUnlock()
	if ok {
		return d, payload, nil
	}
	if t.Kind() == reflect.Pointer {
		rv := reflect.ValueOf(payload)
		if rv.IsNil() {
			return nil, nil, fmt.Errorf("%w: payload is a nil %s", ErrInvalidPayload, t)
		}
		r.mu.RLock()
		d, ok = r.byType[t.Elem()]
		r.mu.RUnlock()
		if ok {
			return d, rv.Elem().Interface(), nil
		}
	}
	return nil, nil, fmt.Errorf("%w: no definition registered for payload type %s", ErrUnknownType, t)
}

// Names returns the registered work type names, sorted. Workers use them to
// claim only work they can execute.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.byName))
	for n := range r.byName {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// Versions returns the registered versions of name, ascending.
func (r *Registry) Versions(name string) []int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return append([]int(nil), r.byName[name]...)
}

func (r *Registry) serializerFor(d *definition) Serializer {
	if d.serializer != nil {
		return d.serializer
	}
	return r.ser
}

// Serializer returns the registry's default serializer.
func (r *Registry) Serializer() Serializer { return r.ser }
