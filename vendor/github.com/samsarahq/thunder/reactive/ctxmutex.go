package reactive

import (
	"context"
	"sync"
)

type ctxMutex struct {
	sync.Once

	// queue has buffer size of 1 and serializes pending Lock callers.
	queue chan struct{}
}

func (m *ctxMutex) Lock(ctx context.Context) error {
	m.Once.Do(func() {
		m.queue = make(chan struct{}, 1)
	})

	select {
	case <-ctx.Done():
		return ctx.Err()
	case m.queue <- struct{}{}:
		return nil
	}
}

func (m *ctxMutex) Unlock() {
	m.Once.Do(func() {
		m.queue = make(chan struct{}, 1)
	})

	select {
	case <-m.queue:
	default:
		panic("Unlock called before Lock")
	}
}
