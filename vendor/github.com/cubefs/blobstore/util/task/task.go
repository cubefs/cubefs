package task

import (
	"context"
)

type semaphore struct {
	ready chan struct{}
}

func newSemaphore(n int) *semaphore {
	s := &semaphore{
		ready: make(chan struct{}, n),
	}
	for i := 0; i < n; i++ {
		s.ready <- struct{}{}
	}
	return s
}

func (s *semaphore) Wait() <-chan struct{} {
	return s.ready
}
func (s *semaphore) Signal() {
	s.ready <- struct{}{}
}

// Run executes a list of tasks in parallel, returns the first error encountered or nil if all tasks pass.
func Run(ctx context.Context, tasks ...func() error) error {
	n := len(tasks)
	s := newSemaphore(n)
	done := make(chan error, 1)

	for _, task := range tasks {
		<-s.Wait()
		go func(f func() error) {
			err := f()
			if err == nil {
				s.Signal()
				return
			}

			select {
			case done <- err:
			default:
			}
		}(task)
	}

	for i := 0; i < n; i++ {
		select {
		case err := <-done:
			return err
		case <-ctx.Done():
			return ctx.Err()
		case <-s.Wait():
		}
	}

	return nil
}
