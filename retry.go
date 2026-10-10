package work

import (
	"errors"
	"math"
	"math/rand/v2"
	"time"
)

// Backoff computes the delay before the next execution attempt of failed
// work. attempt is the number of attempts made so far: 1 after the first
// failure.
//
// This is the work retry policy. It is intentionally separate from the
// infrastructure backoff used when the backend itself is unavailable.
type Backoff interface {
	Next(attempt int) time.Duration
}

// BackoffFunc adapts a function to Backoff.
type BackoffFunc func(attempt int) time.Duration

// Next implements Backoff.
func (f BackoffFunc) Next(attempt int) time.Duration { return f(attempt) }

// ExponentialBackoff grows the delay exponentially and adds jitter. The zero
// value is usable: 1s initial delay, factor 5, 10m cap and ±20% jitter, which
// yields roughly 1s, 5s, 25s, 2m, 10m.
type ExponentialBackoff struct {
	// Initial is the delay after the first failure. Default 1s.
	Initial time.Duration
	// Maximum caps the delay before jitter. Default 10m.
	Maximum time.Duration
	// Multiplier is the growth factor per attempt. Default 5.
	Multiplier float64
	// Jitter is the fraction of the delay randomized in both directions, in
	// [0, 1]. Default 0.2. Use a negative value to disable jitter.
	Jitter float64
	// Rand returns a number in [0, 1). Default math/rand/v2. Tests inject a
	// fixed source.
	Rand func() float64
}

// Next implements Backoff.
func (b ExponentialBackoff) Next(attempt int) time.Duration {
	initial := b.Initial
	if initial <= 0 {
		initial = time.Second
	}
	maxD := b.Maximum
	if maxD <= 0 {
		maxD = 10 * time.Minute
	}
	mult := b.Multiplier
	if mult <= 1 {
		mult = 5
	}
	jitter := b.Jitter
	switch {
	case jitter == 0:
		jitter = 0.2
	case jitter < 0:
		jitter = 0
	case jitter > 1:
		jitter = 1
	}
	if attempt < 1 {
		attempt = 1
	}
	d := float64(initial) * math.Pow(mult, float64(attempt-1))
	if d > float64(maxD) || math.IsInf(d, 0) || math.IsNaN(d) {
		d = float64(maxD)
	}
	if jitter > 0 {
		r := b.Rand
		if r == nil {
			r = rand.Float64
		}
		// scale in [1-jitter, 1+jitter]
		d *= 1 + jitter*(2*r()-1)
	}
	if d < 0 {
		d = 0
	}
	return time.Duration(d)
}

// FixedBackoff waits the same duration before every retry.
type FixedBackoff time.Duration

// Next implements Backoff.
func (f FixedBackoff) Next(int) time.Duration { return time.Duration(f) }

// decision is the engine's verdict for a failed attempt.
type decision int

const (
	decideRetry decision = iota + 1
	decideFail
)

// retryDecision applies the retry policy. attempt is rec.Attempts (the attempt
// that just failed), max is rec.MaxAttempts.
//
//   - a NonRetryable error always fails permanently
//   - exhausted attempts fail permanently
//   - RetryAfter uses its own delay
//   - everything else uses the backoff
func retryDecision(err error, attempt, max int, b Backoff) (decision, time.Duration) {
	if IsNonRetryable(err) {
		return decideFail, 0
	}
	if attempt >= max {
		return decideFail, 0
	}
	var ra *RetryAfterError
	if errors.As(err, &ra) {
		d := ra.Delay
		if d < 0 {
			d = 0
		}
		return decideRetry, d
	}
	return decideRetry, b.Next(attempt)
}

// infraBackoff is the bounded exponential schedule used when the backend is
// unavailable: 100ms, 500ms, 1s, 2s, 5s, then 5s with jitter. It never grows
// without bound and is not related to the work retry policy.
type infraBackoff struct {
	step int
	rnd  func() float64
}

var infraSchedule = []time.Duration{
	100 * time.Millisecond, 500 * time.Millisecond, time.Second, 2 * time.Second, 5 * time.Second,
}

func newInfraBackoff() *infraBackoff { return &infraBackoff{rnd: rand.Float64} }

func (b *infraBackoff) next() time.Duration {
	i := b.step
	if i >= len(infraSchedule) {
		i = len(infraSchedule) - 1
	} else {
		b.step++
	}
	d := infraSchedule[i]
	// ±10% jitter so many workers do not retry in lockstep.
	return time.Duration(float64(d) * (0.9 + 0.2*b.rnd()))
}

func (b *infraBackoff) reset() { b.step = 0 }
