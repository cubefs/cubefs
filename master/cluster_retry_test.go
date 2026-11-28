// Copyright 2026 The CubeFS Authors.
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

package master

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// success on first try, no sleep
func TestRetryPersistDataPartitionOp_SuccessOnFirstTry(t *testing.T) {
	calls := 0
	start := time.Now()
	err := retryPersistDataPartitionOp(1, "testOp", 3, time.Millisecond, func() error {
		calls++
		return nil
	})
	elapsed := time.Since(start)

	require.NoError(t, err)
	require.Equal(t, 1, calls)
	require.Less(t, elapsed, 5*time.Millisecond)
}

// success after retry
func TestRetryPersistDataPartitionOp_SuccessOnRetry(t *testing.T) {
	calls := 0
	err := retryPersistDataPartitionOp(2, "testOp", 3, time.Millisecond, func() error {
		calls++
		if calls < 3 {
			return errors.New("transient raft error")
		}
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, 3, calls)
}

// all retries failed, check error message
func TestRetryPersistDataPartitionOp_AllFail(t *testing.T) {
	calls := 0
	underlying := errors.New("raft down")
	err := retryPersistDataPartitionOp(3, "testOp", 3, time.Millisecond, func() error {
		calls++
		return underlying
	})

	require.Error(t, err)
	require.Equal(t, 3, calls)
	require.True(t, strings.Contains(err.Error(), "failed after 3 retries"))
	require.True(t, strings.Contains(err.Error(), underlying.Error()))
	require.True(t, strings.Contains(err.Error(), "dp(3)"))
}

// check linear backoff time
func TestRetryPersistDataPartitionOp_LinearBackoff(t *testing.T) {
	base := 20 * time.Millisecond
	start := time.Now()
	err := retryPersistDataPartitionOp(4, "testOp", 3, base, func() error {
		return errors.New("boom")
	})
	elapsed := time.Since(start)

	require.Error(t, err)
	require.GreaterOrEqual(t, elapsed, 3*base)
	require.Less(t, elapsed, 6*base)
}
