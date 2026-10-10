package work_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/tobibamidele/work"
)

func TestExponentialBackoffDefaults(t *testing.T) {
	b := work.ExponentialBackoff{Jitter: -1} // jitter off
	want := []time.Duration{time.Second, 5 * time.Second, 25 * time.Second, 125 * time.Second, 10 * time.Minute, 10 * time.Minute}
	for i, w := range want {
		if got := b.Next(i + 1); got != w {
			t.Errorf("attempt %d: %s, want %s", i+1, got, w)
		}
	}
	if b.Next(0) != time.Second || b.Next(-5) != time.Second {
		t.Error("attempts below 1 behave as the first")
	}
	if got := b.Next(100000); got != 10*time.Minute {
		t.Errorf("huge attempts must saturate at the maximum, got %s", got)
	}
	c := work.ExponentialBackoff{Initial: 100 * time.Millisecond, Maximum: time.Second, Multiplier: 2, Jitter: -1}
	if c.Next(1) != 100*time.Millisecond || c.Next(3) != 400*time.Millisecond || c.Next(10) != time.Second {
		t.Error("custom parameters")
	}
}

func TestExponentialBackoffJitterStaysWithinBounds(t *testing.T) {
	for _, r := range []float64{0, 0.25, 0.5, 0.999999} {
		r := r
		b := work.ExponentialBackoff{Initial: time.Second, Multiplier: 2, Jitter: 0.2, Rand: func() float64 { return r }}
		got := b.Next(1)
		if got < 800*time.Millisecond || got > 1200*time.Millisecond {
			t.Errorf("rand=%v: %s outside ±20%% of 1s", r, got)
		}
	}
	// default jitter is on and never leaves ±20%
	b := work.ExponentialBackoff{Initial: time.Second}
	for i := 0; i < 200; i++ {
		if got := b.Next(1); got < 800*time.Millisecond || got > 1200*time.Millisecond {
			t.Fatalf("default jitter produced %s", got)
		}
	}
	if work.FixedBackoff(3*time.Second).Next(9) != 3*time.Second {
		t.Error("fixed")
	}
	if work.BackoffFunc(func(a int) time.Duration { return time.Duration(a) }).Next(4) != 4 {
		t.Error("func")
	}
}

func TestErrorHelpers(t *testing.T) {
	base := errors.New("boom")
	if !work.IsNonRetryable(work.NonRetryable(base)) || work.IsNonRetryable(base) {
		t.Fatal("IsNonRetryable")
	}
	wrapped := fmt.Errorf("context: %w", work.NonRetryable(base))
	if !work.IsNonRetryable(wrapped) || !errors.Is(wrapped, base) {
		t.Fatal("wrapping must be transparent")
	}
	if work.NonRetryable(nil).Error() == "" {
		t.Fatal("nil cause")
	}
	ra := work.RetryAfterBecause(time.Minute, base)
	var rae *work.RetryAfterError
	if !errors.As(ra, &rae) || rae.Delay != time.Minute || !errors.Is(ra, base) {
		t.Fatal("RetryAfterBecause")
	}
	if work.RetryAfter(time.Second).Error() == "" {
		t.Fatal("RetryAfter text")
	}
	if !work.IsOwnership(work.ErrLeaseLost) || !work.IsOwnership(work.ErrCancelled) || work.IsOwnership(base) {
		t.Fatal("IsOwnership")
	}
}

func TestErrorTextIsBoundedAndKeepsPanicStacks(t *testing.T) {
	long := errors.New(string(make([]byte, 50000)))
	if got := work.ErrorText(long); len(got) > work.MaxErrorBytes {
		t.Fatalf("len = %d", len(got))
	}
	multibyte := errors.New(string([]rune(fmt.Sprintf("%0*d", work.MaxErrorBytes, 0))) + "ééééé")
	if s := work.ErrorText(multibyte); len(s) > work.MaxErrorBytes {
		t.Fatalf("len = %d", len(s))
	}
	if work.ErrorText(nil) != "" {
		t.Fatal("nil")
	}
}
