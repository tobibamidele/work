package work

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
)

// Serializer converts payloads to and from bytes. The serialization format is
// not work identity: identity is type + version.
//
// Unmarshal into a *any must produce a generic tree the payload schema can
// validate, with numbers preserved exactly (the JSON serializer decodes them
// as json.Number).
type Serializer interface {
	Marshal(value any) ([]byte, error)
	Unmarshal(data []byte, target any) error
}

// JSONSerializer is the default Serializer. It is human readable, easy to
// inspect in the database and broadly compatible.
//
// It decodes numbers as json.Number, so integers above 2^53 survive a round
// trip exactly, and it rejects data after the first JSON value.
type JSONSerializer struct{}

// Marshal implements Serializer.
func (JSONSerializer) Marshal(value any) ([]byte, error) { return json.Marshal(value) }

// Unmarshal implements Serializer.
func (JSONSerializer) Unmarshal(data []byte, target any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(target); err != nil {
		return err
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return errors.New("work: trailing data after JSON value")
	}
	return nil
}

// ContentType reports the serializer's MIME type. Backends that can only
// store a particular format use it to refuse payloads they cannot hold.
func (JSONSerializer) ContentType() string { return "application/json" }

// contentTyper is implemented by serializers that declare their format.
type contentTyper interface{ ContentType() string }

// ContentTypeOf returns the declared content type of s, or "" if undeclared.
func ContentTypeOf(s Serializer) string {
	if ct, ok := s.(contentTyper); ok {
		return ct.ContentType()
	}
	return ""
}
