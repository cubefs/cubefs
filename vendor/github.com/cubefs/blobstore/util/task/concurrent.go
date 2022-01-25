package task

import "context"

var (
	// C alias of Concurrent
	C = Concurrent
	// CC alias of ConcurrentContext
	CC = ConcurrentContext
)

// Concurrent is tasks run concurrently.
func Concurrent(f func(index int, arg interface{}), args []interface{}) {
	ConcurrentContext(context.Background(), f, args)
}

// ConcurrentContext is tasks run concurrently with context.
// How to make []interface{} see: https://golang.org/doc/faq#convert_slice_of_interface
func ConcurrentContext(ctx context.Context, f func(index int, arg interface{}), args []interface{}) {
	tasks := make([]func() error, len(args))
	for i := 0; i < len(args); i++ {
		index, arg := i, args[i]
		tasks[i] = func() error {
			f(index, arg)
			return nil
		}
	}
	Run(ctx, tasks...)
}
