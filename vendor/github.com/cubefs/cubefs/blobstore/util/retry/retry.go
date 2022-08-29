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
	// On performs a retry on function, until it doesn't return any error.
	On(func() error) error
	// RuptOn performs a retry on function, until it doesn't return any error or interrupt.
	RuptOn(func() (bool, error)) error
}

type retry struct {
	attempts  int
	nextDelay func() uint32
}

// On implements Retryer.On.
func (r *retry) On(caller func() error) error {
	var lastErr error
	attempt := 1
	for attempt <= r.attempts {
		if lastErr = caller(); lastErr == nil {
			return nil
		}

		// do not wait on last useless delay
		if attempt >= r.attempts {
			break
		}
		time.Sleep(time.Duration(r.nextDelay()) * time.Millisecond)
		attempt++
	}
	return lastErr
}

// RuptOn implements Retryer.RuptOn.
func (r *retry) RuptOn(caller func() (bool, error)) error {
	var lastErr error
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
		time.Sleep(time.Duration(r.nextDelay()) * time.Millisecond)
		attempt++
	}
	return lastErr
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
		nextDelay: func() uint32 {
			r := next
			next += expDelay
			return r
		},
	}
}
