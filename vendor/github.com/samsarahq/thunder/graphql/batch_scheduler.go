package graphql

import (
	"sync"
)

// NewImmediateGoroutineScheduler creates a new batch execution scheduler that
// executes all Units immediately in their own goroutine.
func NewImmediateGoroutineScheduler() WorkScheduler {
	return &immediateGoroutineScheduler{}
}

type immediateGoroutineScheduler struct{}

func (q *immediateGoroutineScheduler) Run(resolver UnitResolver, initialUnits ...*WorkUnit) {
	r := &immediateGoroutineSchedulerRunner{}
	r.runEnqueue(resolver, initialUnits...)

	r.wg.Wait()
}

type immediateGoroutineSchedulerRunner struct {
	wg sync.WaitGroup
}

func (r *immediateGoroutineSchedulerRunner) runEnqueue(resolver UnitResolver, units ...*WorkUnit) {
	for _, unit := range units {
		r.wg.Add(1)
		go func(u *WorkUnit) {
			defer r.wg.Done()
			units := resolver(u)
			r.runEnqueue(resolver, units...)
		}(unit)
	}
}
