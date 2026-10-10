package work

import "context"

// Handler is the function a Middleware wraps: the execution of one attempt of
// one work item.
type Handler func(ctx context.Context) error

// Middleware wraps handler execution. It must not claim or acknowledge work:
// the engine owns the lifecycle.
//
// Ordering: for Use(A, B, C) the call order is A, B, C, handler, C, B, A. The
// first middleware is the outermost.
type Middleware func(next Handler) Handler

// Chain composes middleware around h. The first middleware is outermost.
func Chain(h Handler, mws ...Middleware) Handler {
	for i := len(mws) - 1; i >= 0; i-- {
		if mws[i] == nil {
			continue
		}
		h = mws[i](h)
	}
	return h
}
