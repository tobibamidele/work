package work

import (
	"errors"
	"fmt"
	"runtime/debug"
	"time"
)

// Sentinel errors. Test for them with errors.Is; never compare error strings.
var (
	// ErrNoWork is returned by Backend.Claim when nothing is claimable. It is
	// a normal condition, not an operational failure.
	ErrNoWork = errors.New("work: no work available")

	// ErrNotFound is returned when a work ID does not exist.
	ErrNotFound = errors.New("work: not found")

	// ErrAlreadyCompleted is returned when an operation targets work that has
	// already completed successfully.
	ErrAlreadyCompleted = errors.New("work: already completed")

	// ErrCancelled is returned when an operation targets work that was
	// cancelled, for example a worker trying to complete cancelled work.
	ErrCancelled = errors.New("work: cancelled")

	// ErrLeaseLost is the ownership error: the worker no longer holds the
	// lease it believed it held, so it must not acknowledge the work.
	ErrLeaseLost = errors.New("work: lease lost")

	// ErrInvalidState is returned when a transition is not allowed from the
	// work's current status, for example retrying work that is still running.
	ErrInvalidState = errors.New("work: invalid state for this operation")

	// ErrDuplicate is returned by Dispatch when work with the same unique key
	// is already active. The returned Ref identifies the existing work.
	ErrDuplicate = errors.New("work: duplicate unique key")

	// ErrUnknownType is returned when a work type is not registered.
	ErrUnknownType = errors.New("work: unknown work type")

	// ErrUnsupportedVersion is returned when a work type is registered but not
	// at the version a record carries.
	ErrUnsupportedVersion = errors.New("work: unsupported work version")

	// ErrInvalidPayload is returned when a payload fails validation or cannot
	// be serialized, at dispatch or at execution.
	ErrInvalidPayload = errors.New("work: invalid payload")

	// ErrUnsupported is returned when a backend cannot provide a requested
	// capability. Backends fail explicitly instead of silently weakening
	// semantics.
	ErrUnsupported = errors.New("work: unsupported by this backend")

	// ErrNoRuntime is returned by the package-level Dispatch functions when no
	// Runtime is attached to the context and no default Runtime is set.
	ErrNoRuntime = errors.New("work: no runtime in context and no default runtime set")

	// ErrConfig marks configuration errors. They are fatal and are reported at
	// startup, never during normal execution.
	ErrConfig = errors.New("work: invalid configuration")

	// ErrRegistryFrozen is returned when registering after workers started.
	ErrRegistryFrozen = errors.New("work: registry is frozen")

	// ErrShutdownTimeout is returned by Worker.Run when handlers did not stop
	// after the grace period and the forced-cancellation window.
	ErrShutdownTimeout = errors.New("work: shutdown timed out with handlers still running")
)

// DuplicateError is returned (wrapping ErrDuplicate) when a unique key
// collides with active work. ExistingID is the work that holds the key.
type DuplicateError struct {
	Queue      string
	UniqueKey  string
	ExistingID ID
}

func (e *DuplicateError) Error() string {
	return fmt.Sprintf("work: duplicate unique key %q in queue %q (active work %s)", e.UniqueKey, e.Queue, e.ExistingID)
}

// Is makes errors.Is(err, ErrDuplicate) true.
func (e *DuplicateError) Is(target error) bool { return target == ErrDuplicate }

// Phase says where a payload problem was detected.
type Phase string

const (
	// PhaseDispatch is validation before persistence.
	PhaseDispatch Phase = "dispatch"
	// PhaseExecute is validation of stored bytes before the handler runs.
	PhaseExecute Phase = "execute"
)

// PayloadError reports a payload that failed serialization or validation. It
// wraps the underlying error (a *validate.ValidationError for schema
// failures), so errors.As reaches it, and it matches ErrInvalidPayload.
type PayloadError struct {
	Phase   Phase
	Type    string
	Version int
	Err     error
}

func (e *PayloadError) Error() string {
	return fmt.Sprintf("work: invalid payload for %s v%d (%s): %v", e.Type, e.Version, e.Phase, e.Err)
}
func (e *PayloadError) Unwrap() []error { return []error{ErrInvalidPayload, e.Err} }

// VersionError reports an unsupported version of a known work type.
type VersionError struct {
	Type      string
	Version   int
	Supported []int
}

func (e *VersionError) Error() string {
	return fmt.Sprintf("work: %s version %d is not supported by this worker (supported: %v)", e.Type, e.Version, e.Supported)
}
func (e *VersionError) Is(target error) bool { return target == ErrUnsupportedVersion }

// NonRetryableError marks an error as permanent: the work fails immediately
// even if attempts remain.
type NonRetryableError struct{ Err error }

func (e *NonRetryableError) Error() string {
	if e.Err == nil {
		return "work: non-retryable failure"
	}
	return "non-retryable: " + e.Err.Error()
}
func (e *NonRetryableError) Unwrap() error { return e.Err }

// NonRetryable wraps err so the work fails permanently without further
// attempts. A nil err yields a generic permanent failure.
func NonRetryable(err error) error { return &NonRetryableError{Err: err} }

// IsNonRetryable reports whether err is, or wraps, a NonRetryableError.
func IsNonRetryable(err error) bool {
	var nr *NonRetryableError
	return errors.As(err, &nr)
}

// RetryAfterError requests a retry after an explicit delay, instead of the
// backoff policy's delay. It is still subject to MaxAttempts.
type RetryAfterError struct {
	Delay time.Duration
	Err   error
}

func (e *RetryAfterError) Error() string {
	if e.Err == nil {
		return fmt.Sprintf("work: retry after %s", e.Delay)
	}
	return fmt.Sprintf("retry after %s: %v", e.Delay, e.Err)
}
func (e *RetryAfterError) Unwrap() error { return e.Err }

// RetryAfter returns an error asking for a retry after d.
func RetryAfter(d time.Duration) error { return &RetryAfterError{Delay: d} }

// RetryAfterBecause is RetryAfter that also carries the cause.
func RetryAfterBecause(d time.Duration, err error) error { return &RetryAfterError{Delay: d, Err: err} }

// PanicError is the error recorded when a handler panics.
type PanicError struct {
	Value any
	Stack []byte
}

func (e *PanicError) Error() string { return fmt.Sprintf("panic: %v", e.Value) }

func newPanicError(v any) *PanicError { return &PanicError{Value: v, Stack: debug.Stack()} }

// ExecutionError is the structured error the engine builds for a failed
// attempt. It is what backends store and observers receive.
type ExecutionError struct {
	WorkID   ID
	Type     string
	Version  int
	Queue    string
	Attempt  int
	WorkerID string
	Err      error
}

func (e *ExecutionError) Error() string {
	return fmt.Sprintf("work %s (%s v%d) attempt %d on %s: %v", e.WorkID, e.Type, e.Version, e.Attempt, e.WorkerID, e.Err)
}
func (e *ExecutionError) Unwrap() error { return e.Err }

// IsOwnership reports whether err means the caller no longer owns the work.
func IsOwnership(err error) bool {
	return errors.Is(err, ErrLeaseLost) || errors.Is(err, ErrCancelled) || errors.Is(err, ErrAlreadyCompleted) || errors.Is(err, ErrNotFound)
}

func asDuplicate(err error) (*DuplicateError, bool) {
	var de *DuplicateError
	if errors.As(err, &de) {
		return de, true
	}
	return nil, false
}
