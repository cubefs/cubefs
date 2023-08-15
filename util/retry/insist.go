// Copyright 2023 The CubeFS Authors.
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
	"math"
	"time"
)

var (
	DefaultDelay    = 100 * time.Millisecond
	DefaultMaxDelay = 3 * time.Minute
	MaxBackOffN     = 62 - math.Floor(math.Log2(float64(DefaultDelay)))
)

type CallFunc func() error
type DelayFUnc func(uint) time.Duration

// Insist successfully on call or return err when done with context.
func Insist(ctx context.Context, delay, maxDelay time.Duration, call CallFunc, nextDelay DelayFUnc) error {
	if err := call(); err == nil {
		return nil
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	var n uint
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			if err := call(); err == nil {
				return nil
			}
			n++
			d := nextDelay(n)
			if d > maxDelay {
				d = maxDelay
			}
			timer.Reset(d)
		}
	}
}

func InsistWithDefaultExponential(ctx context.Context, call CallFunc) error {
	return Insist(ctx, DefaultDelay, DefaultMaxDelay, call, func(n uint) time.Duration {
		if n > uint(MaxBackOffN) {
			n = uint(MaxBackOffN)
		}
		return DefaultDelay << n
	})
}
