package work

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"regexp"
	"time"
)

var queueRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,254}$`)

func validQueueName(q string) bool { return queueRE.MatchString(q) }

// DefaultQueueName is the queue used when none is chosen.
const DefaultQueueName = "default"

// Limits on dispatch options.
const (
	maxUniqueKeyLen = 255
	maxMetadataSize = 4 << 10
)

// DispatchOption customizes one Dispatch call.
type DispatchOption func(*dispatchConfig)

type dispatchConfig struct {
	queue          string
	delay          time.Duration
	hasDelay       bool
	at             time.Time
	maxAttempts    int
	hasMaxAttempts bool
	priority       int
	hasPriority    bool
	unique         string
	uniquePayload  bool
	idempotency    string
	timeout        time.Duration
	hasTimeout     bool
	metadata       map[string]string
	errs           []error
}

func (c *dispatchConfig) fail(format string, args ...any) {
	c.errs = append(c.errs, fmt.Errorf("work: invalid dispatch option: "+format, args...))
}

// Queue sends the work to a named queue instead of the definition's default.
func Queue(name string) DispatchOption {
	return func(c *dispatchConfig) {
		if !validQueueName(name) {
			c.fail("queue name %q (use letters, digits and . _ : -, at most 255 characters)", name)
			return
		}
		c.queue = name
	}
}

// Delay makes the work claimable only after d has elapsed. It conflicts with
// At and DispatchAt.
func Delay(d time.Duration) DispatchOption {
	return func(c *dispatchConfig) {
		if d < 0 {
			c.fail("delay must not be negative, got %s", d)
			return
		}
		c.delay, c.hasDelay = d, true
	}
}

// At makes the work claimable only from t on. It is the option form of the
// DispatchAt function.
func At(t time.Time) DispatchOption {
	return func(c *dispatchConfig) {
		if t.IsZero() {
			c.fail("At needs a non-zero time")
			return
		}
		c.at = t.UTC()
	}
}

// MaxAttempts limits how many times the work is attempted, including the
// first. It must be at least 1.
func MaxAttempts(n int) DispatchOption {
	return func(c *dispatchConfig) {
		if n < 1 || n > math.MaxInt32 {
			c.fail("max attempts must be between 1 and %d, got %d", math.MaxInt32, n)
			return
		}
		c.maxAttempts, c.hasMaxAttempts = n, true
	}
}

// Priority sets a scheduling preference: higher values are claimed first.
// The default is 0. It is a preference within a queue, not a global ordering
// guarantee.
func Priority(p int) DispatchOption {
	return func(c *dispatchConfig) {
		if p < math.MinInt32 || p > math.MaxInt32 {
			c.fail("priority %d is out of range", p)
			return
		}
		c.priority, c.hasPriority = p, true
	}
}

// Unique prevents another equivalent work item from being scheduled while one
// with the same key (in the same queue) is active, that is available or
// running. Completed, failed and cancelled work never blocks new dispatch.
// A blocked Dispatch returns an error matching ErrDuplicate together with the
// Ref of the work that holds the key.
func Unique(key string) DispatchOption {
	return func(c *dispatchConfig) {
		if key == "" || len(key) > maxUniqueKeyLen {
			c.fail("unique key must be 1 to %d bytes, got %d", maxUniqueKeyLen, len(key))
			return
		}
		c.unique = key
	}
}

// UniquePayload is Unique with a key derived from the work type, version and
// the normalized payload, so two dispatches of equal payloads are duplicates.
func UniquePayload() DispatchOption {
	return func(c *dispatchConfig) { c.uniquePayload = true }
}

// IdempotencyKey sets the key handlers should pass to external systems so a
// duplicate execution is harmless. It defaults to the work ID.
func IdempotencyKey(key string) DispatchOption {
	return func(c *dispatchConfig) {
		if len(key) > maxUniqueKeyLen {
			c.fail("idempotency key must be at most %d bytes", maxUniqueKeyLen)
			return
		}
		c.idempotency = key
	}
}

// Timeout bounds one execution attempt. The handler's context gets this
// deadline; the handler must honor its context for the timeout to take effect.
func Timeout(d time.Duration) DispatchOption {
	return func(c *dispatchConfig) {
		if d < 0 {
			c.fail("timeout must not be negative, got %s", d)
			return
		}
		c.timeout, c.hasTimeout = d, true
	}
}

// DispatchMetadata attaches a key/value pair that handlers can read from
// FromContext(ctx).Dispatch. Do not put secrets or personal data in it.
func DispatchMetadata(key, value string) DispatchOption {
	return func(c *dispatchConfig) {
		if key == "" {
			c.fail("metadata key must not be empty")
			return
		}
		if c.metadata == nil {
			c.metadata = map[string]string{}
		}
		c.metadata[key] = value
	}
}

func newDispatchConfig(opts []DispatchOption) (*dispatchConfig, error) {
	c := &dispatchConfig{}
	for _, o := range opts {
		if o != nil {
			o(c)
		}
	}
	if c.hasDelay && !c.at.IsZero() {
		c.fail("Delay and At/DispatchAt are mutually exclusive")
	}
	size := 0
	for k, v := range c.metadata {
		size += len(k) + len(v)
	}
	if size > maxMetadataSize {
		c.fail("metadata is %d bytes, the limit is %d", size, maxMetadataSize)
	}
	if len(c.errs) > 0 {
		return nil, c.errs[0]
	}
	return c, nil
}

// payloadUniqueKey derives the key UniquePayload uses.
func payloadUniqueKey(name string, version int, payload []byte) string {
	sum := sha256.Sum256(payload)
	return fmt.Sprintf("%s:v%d:%s", name, version, hex.EncodeToString(sum[:]))
}
