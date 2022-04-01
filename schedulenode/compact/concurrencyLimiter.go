package compact

import (
	"sync"
)

type concurrencyLimiter struct {
	limit       int32
	runningNum  int32
	blockingNum int32
	cond        *sync.Cond
	mu          *sync.RWMutex
}

// NewConcurrencyLimiter Create a concurrency limiter. The limit is the number of concurrency limits. You can dynamically adjust the limit through Reset().
// Each time you call Get() to get a resource, create a goroutine, and release the resource through Release() after completing the task.
func NewConcurrencyLimiter(limit int32) *concurrencyLimiter {
	mu := new(sync.RWMutex)
	return &concurrencyLimiter{
		limit: limit,
		cond:  sync.NewCond(mu),
		mu:    mu,
	}
}

func (c *concurrencyLimiter) Reset(limit int32) {
	if limit <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	oldLimit := c.limit
	c.limit = limit
	blockingNum := c.blockingNum
	// wakeup blocked tasks
	if limit-oldLimit > 0 && blockingNum > 0 {
		for i := int32(0); i < limit-oldLimit && blockingNum > 0; i++ {
			c.cond.Signal()
			blockingNum--
		}
	}
}

func (c *concurrencyLimiter) Add() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.runningNum < c.limit {
		c.runningNum++
		return
	}
	c.blockingNum++
	for !(c.runningNum < c.limit) {
		c.cond.Wait()
	}
	c.runningNum++
	c.blockingNum--
}

func (c *concurrencyLimiter) Done() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.blockingNum > 0 {
		c.runningNum--
		c.cond.Signal()
		return
	}
	c.runningNum--
}

func (c *concurrencyLimiter) Limit() int32 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.limit
}
