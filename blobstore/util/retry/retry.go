// Copyright 2022 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package retry

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrRetryFailed all retry attempts failed.
	ErrRetryFailed = errors.New("retry: all retry attempts failed")
	// ErrRetryNext retry next on interrupt.
	ErrRetryNext = errors.New("retry: retry next on interrupt")
)

// Retryer is an interface retry on a specific function.
type Retryer interface {
	// Reset the delay as beginning.
	Reset()
	// On performs a retry on function, until it doesn't return any error.
	On(func() error) error
	// OnContext On or context done.
	OnContext(context.Context, func() error) error
	// RuptOn performs a retry on function, until it doesn't return any error or interrupt.
	RuptOn(func() (bool, error)) error
	// RuptOnContext RuptOn or context done.
	RuptOnContext(context.Context, func() (bool, error)) error
}

// retry non-thread-safer implements Retryer
type retry struct {
	attempts  int
	reset     func()
	nextDelay func() uint32
}

type internalContext struct{}

func (internalContext) Deadline() (deadline time.Time, ok bool) { return }
func (internalContext) Done() <-chan struct{}                   { return nil }
func (internalContext) Err() error                              { return nil }
func (internalContext) Value(key interface{}) interface{}       { return nil }

func (r *retry) Reset() {
	if r.reset != nil {
		r.reset()
	}
}

// On implements Retryer.On.
func (r *retry) On(caller func() error) error {
	return r.OnContext(internalContext{}, caller)
}

// OnContext implements Retryer.OnContext.
func (r *retry) OnContext(ctx context.Context, caller func() error) error {
	var lastErr error
	var timer *time.Timer
	var ctxDone bool

	attempt := 1
	for attempt <= r.attempts {
		if lastErr = caller(); lastErr == nil {
			return nil
		}

		// do not wait on last useless delay
		if attempt >= r.attempts {
			break
		}
		attempt++

		if timer, ctxDone = r.waitContext(ctx, timer); ctxDone {
			break
		}
	}
	if timer != nil {
		timer.Stop()
	}
	return lastErr
}

// RuptOn implements Retryer.RuptOn.
func (r *retry) RuptOn(caller func() (bool, error)) error {
	return r.RuptOnContext(internalContext{}, caller)
}

// RuptOnContext implements Retryer.RuptOnContext.
func (r *retry) RuptOnContext(ctx context.Context, caller func() (bool, error)) error {
	var lastErr error
	var timer *time.Timer
	var ctxDone bool

	attempt := 1
	for attempt <= r.attempts {
		interrupted, err := caller()
		if err == nil {
			return nil
		}
		// return last error of method, if interrupted
		if err != ErrRetryNext {
			lastErr = err
		}
		if interrupted {
			break
		}

		// do not wait on last useless delay
		if attempt >= r.attempts {
			break
		}
		attempt++

		if timer, ctxDone = r.waitContext(ctx, timer); ctxDone {
			break
		}
	}
	if timer != nil {
		timer.Stop()
	}
	return lastErr
}

func (r *retry) waitContext(ctx context.Context, timer *time.Timer) (*time.Timer, bool) {
	var wait <-chan time.Time

	next := time.Duration(r.nextDelay()) * time.Millisecond
	if _, internal := ctx.(internalContext); internal {
		if next > 0 {
			time.Sleep(next)
		}
		return timer, false
	}

	if next > 0 {
		if timer == nil {
			timer = time.NewTimer(next)
		} else {
			timer.Reset(next)
		}
		wait = timer.C
	}

	if wait != nil {
		select {
		case <-wait:
			return timer, false
		case <-ctx.Done():
			return timer, true
		}
	}
	select {
	case <-ctx.Done():
		return timer, true
	default:
		return timer, false
	}
}

// Timed returns a retry with fixed interval delay.
func Timed(attempts int, delay uint32) Retryer {
	return &retry{
		attempts: attempts,
		nextDelay: func() uint32 {
			return delay
		},
	}
}

// ExponentialBackoff returns a retry with exponential delay.
func ExponentialBackoff(attempts int, expDelay uint32) Retryer {
	next := expDelay
	return &retry{
		attempts: attempts,
		reset: func() {
			next = expDelay
		},
		nextDelay: func() uint32 {
			r := next
			next += expDelay
			return r
		},
	}
}
