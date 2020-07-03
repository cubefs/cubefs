package concurrencylimiter

import (
	"context"
	"sync/atomic"
)

// A limiter allows goroutines to run with bounded concurrency.
type limiter struct {
	// ch limits concurrency by having a bounded capacity. To run a goroutine must
	// write to ch. This takes up a spot in the channel. After stopping, the
	// goroutine must read from ch, which frees up a spot in the channel for
	// another goroutine to run.
	ch chan struct{}
}

// limiterKey is the context key used for limiter structs.
type limiterKey struct{}

// With attaches a new limiter to the context with the given limit.
func With(ctx context.Context, limit int) context.Context {
	return context.WithValue(ctx, limiterKey{}, &limiter{
		ch: make(chan struct{}, limit),
	})
}

// holderKey is the context key used for holder structs.
type holderKey struct{}

// Holder describes the status of a goroutine that has called Acquire. The
// holder struct is used to refer back to the limiter and to temporarily release
// the goroutine's token during calls to block.
//
// It is impossible to enforce that every goroutine makes it own call to Acquire,
// so the holder gracefully handles the case where multiple concurrent goroutines
// call block: Only one of them will release the token, and the other goroutines
// will run independently. That's not ideal, but an alternative design that
// lets the other goroutines hang might cause surprise breakages when a context
// is shared between goroutines.
type holder struct {
	l *limiter

	// status tracks if the holder currently has an in item in l.ch. Before
	// modifying l.ch, first status must be modified using an atomic operation.
	// This is the concurrency control.
	status int64
}

const (
	acquired = iota
	blocked
	released
)

// release gives up the holder's spot in ch.
func (h *holder) release() {
	// If we currently are acquired, release the token. Otherwise, we are either
	// blocked or already released.
	if atomic.SwapInt64(&h.status, released) == acquired {
		<-h.l.ch
	}
}

// block temporarily gives up the holder's spot in ch while running f.
func (h *holder) block(f func()) {
	// If we are currently acquired, temporarily release the token. Otherwise,
	// we are either blocked or released.
	if atomic.CompareAndSwapInt64(&h.status, acquired, blocked) {
		<-h.l.ch

		// Before returning from f() we must reacquire.
		defer func() {
			// If we are still blocked, re-acquire. Otherwise, we just got got released
			// (and that release used our token we gave up), and should no longer try to
			// re-acquire.
			if atomic.CompareAndSwapInt64(&h.status, blocked, acquired) {
				h.l.ch <- struct{}{}
			}
		}()
	}

	f()
}

// A ReleaseFunc releases a concurrency limiter token.
type ReleaseFunc func()

// Acquire acquires a concurrency limiter token. If the context is canceled or
// if there is no concurrency limit associated with the context it succeeds
// immediately and returns a no-op release function.
//
// The returned release function is idempotent.
func Acquire(ctx context.Context) (context.Context, ReleaseFunc) {
	l, ok := ctx.Value(limiterKey{}).(*limiter)
	if !ok {
		return ctx, func() {}
	}

	select {
	case l.ch <- struct{}{}:
	case <-ctx.Done():
		return ctx, func() {}
	}

	h := &holder{
		l:      l,
		status: acquired,
	}
	ctx = context.WithValue(ctx, holderKey{}, h)

	return ctx, h.release
}

// TemporarilyRelease temporarily releases a concurrency limiter token (if any)
// while calling a long-running but no-resource-using function f.
func TemporarilyRelease(ctx context.Context, f func()) {
	if h, ok := ctx.Value(holderKey{}).(*holder); ok {
		h.block(f)
	} else {
		f()
	}
}
