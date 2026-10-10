package work

import (
	"context"
	"time"
)

// EventName names a lifecycle event.
type EventName string

// Lifecycle events.
const (
	EventEnqueued  EventName = "work.enqueued"
	EventStarted   EventName = "work.started"
	EventRetrying  EventName = "work.retrying"
	EventCompleted EventName = "work.completed"
	EventFailed    EventName = "work.failed"
	EventCancelled EventName = "work.cancelled"
	EventLeaseLost EventName = "work.lease_lost"
)

// Event describes one lifecycle transition. It never carries the payload.
type Event struct {
	Name     EventName
	WorkID   ID
	Type     string
	Version  int
	Queue    string
	Attempt  int
	WorkerID string
	Time     time.Time

	// Duration is the execution time for completed, failed, retrying and
	// cancelled events, and the time spent waiting for lease-lost events.
	Duration time.Duration

	// QueueWait is, for started events, how long the work waited between
	// becoming available and being claimed.
	QueueWait time.Duration

	// RetryAt is when a retrying work item becomes available again.
	RetryAt time.Time

	// Transactional marks an enqueue made inside an application transaction:
	// the event is emitted when the insert succeeds, which may precede the
	// commit.
	Transactional bool

	Err error
}

// Observer receives lifecycle events. Implementations must be fast and safe
// for concurrent use; a panic in an observer is recovered and ignored.
type Observer interface {
	Observe(ctx context.Context, e Event)
}

// ObserverFunc adapts a function to Observer.
type ObserverFunc func(ctx context.Context, e Event)

// Observe implements Observer.
func (f ObserverFunc) Observe(ctx context.Context, e Event) { f(ctx, e) }

type multiObserver []Observer

func (m multiObserver) Observe(ctx context.Context, e Event) {
	for _, o := range m {
		safeObserve(ctx, o, e)
	}
}

// MultiObserver fans events out to several observers, in order.
func MultiObserver(obs ...Observer) Observer {
	out := make(multiObserver, 0, len(obs))
	for _, o := range obs {
		if o != nil {
			out = append(out, o)
		}
	}
	return out
}

func safeObserve(ctx context.Context, o Observer, e Event) {
	if o == nil {
		return
	}
	defer func() { _ = recover() }()
	o.Observe(ctx, e)
}
