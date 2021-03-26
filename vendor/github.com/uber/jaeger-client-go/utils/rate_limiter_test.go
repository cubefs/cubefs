// Copyright (c) 2017 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package utils

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRateLimiter(t *testing.T) {
	rl := NewRateLimiter(2.0, 2.0)
	// stop time
	ts := time.Now()
	rl.lastTick = ts
	rl.timeNow = func() time.Time {
		return ts
	}
	assert.True(t, rl.CheckCredit(1.0))
	assert.True(t, rl.CheckCredit(1.0))
	assert.False(t, rl.CheckCredit(1.0))
	// move time 250ms forward, not enough credits to pay for 1.0 item
	rl.timeNow = func() time.Time {
		return ts.Add(time.Second / 4)
	}
	assert.False(t, rl.CheckCredit(1.0))
	// move time 500ms forward, now enough credits to pay for 1.0 item
	rl.timeNow = func() time.Time {
		return ts.Add(time.Second/4 + time.Second/2)
	}
	assert.True(t, rl.CheckCredit(1.0))
	assert.False(t, rl.CheckCredit(1.0))
	// move time 5s forward, enough to accumulate credits for 10 messages, but it should still be capped at 2
	rl.lastTick = ts
	rl.timeNow = func() time.Time {
		return ts.Add(5 * time.Second)
	}
	assert.True(t, rl.CheckCredit(1.0))
	assert.True(t, rl.CheckCredit(1.0))
	assert.False(t, rl.CheckCredit(1.0))
	assert.False(t, rl.CheckCredit(1.0))
	assert.False(t, rl.CheckCredit(1.0))
}

func TestRateLimiterMaxBalance(t *testing.T) {
	rl := NewRateLimiter(0.1, 1.0)
	// stop time
	ts := time.Now()
	rl.lastTick = ts
	rl.timeNow = func() time.Time {
		return ts
	}
	assert.True(t, rl.CheckCredit(1.0), "on initialization, should have enough credits for 1 message")

	// move time 20s forward, enough to accumulate credits for 2 messages, but it should still be capped at 1
	rl.timeNow = func() time.Time {
		return ts.Add(time.Second * 20)
	}
	assert.True(t, rl.CheckCredit(1.0))
	assert.False(t, rl.CheckCredit(1.0))
}

func TestRateLimiterReconfigure(t *testing.T) {
	rl := NewRateLimiter(1, 1.0)
	assertBalance := func(expected float64) {
		const delta = 0.0000001 // just some precision for comparing floats
		assert.InDelta(t, expected, rl.balance, delta)
	}
	// stop time
	ts := time.Now()
	rl.lastTick = ts
	rl.timeNow = func() time.Time {
		return ts
	}
	assert.True(t, rl.CheckCredit(1.0), "first message must succeed")
	assert.False(t, rl.CheckCredit(1.0), "second message must be rejected")
	assertBalance(0.0)

	// move half-second forward
	rl.timeNow = func() time.Time {
		return ts.Add(time.Second / 2)
	}
	rl.updateBalance()
	assertBalance(0.5) // 50% of max

	rl.Update(2, 4)
	assertBalance(2) // 50% of max
	assert.EqualValues(t, 2, rl.creditsPerSecond)
	assert.EqualValues(t, 4, rl.maxBalance)
}
