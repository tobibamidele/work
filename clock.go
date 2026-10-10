package work

import "time"

// Clock is the time source. The engine passes times explicitly to backends,
// so a single Clock makes whole systems deterministic in tests.
type Clock interface {
	Now() time.Time
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now().UTC() }

// SystemClock is the real wall clock, in UTC.
var SystemClock Clock = systemClock{}

// ClockFunc adapts a function to Clock.
type ClockFunc func() time.Time

// Now implements Clock.
func (f ClockFunc) Now() time.Time { return f() }
