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

package base

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func MockEmptyVolTaskLocker() {
	VolTaskLockerInst().mu.Lock()
	defer VolTaskLockerInst().mu.Unlock()
	VolTaskLockerInst().taskMap = make(map[uint32]struct{})
}

func TestVolTaskLocker(t *testing.T) {
	MockEmptyVolTaskLocker()
	ctx := context.Background()

	mu := VolTaskLockerInst()
	mu2 := VolTaskLockerInst()
	require.Equal(t, mu, mu2)
	err := mu.TryLock(ctx, 1)
	require.NoError(t, err)
	err = mu.TryLock(ctx, 1)
	require.EqualError(t, err, ErrVidTaskConflict.Error())
	mu.Unlock(ctx, 1)
	err = mu.TryLock(ctx, 1)
	require.NoError(t, err)
}

// TestTaskLocker_ConcurrentTryLock verifies that only one goroutine acquires the lock
// when many goroutines race on the same vid simultaneously.
func TestTaskLocker_ConcurrentTryLock(t *testing.T) {
	locker := &TaskLocker{taskMap: make(map[uint32]struct{})}
	ctx := context.Background()

	const (
		goroutines = 100
		vid        = uint32(42)
	)

	var (
		mu         sync.Mutex
		successCnt int
		start      = make(chan struct{})
		wg         sync.WaitGroup
	)
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			<-start
			if err := locker.TryLock(ctx, vid); err == nil {
				mu.Lock()
				successCnt++
				mu.Unlock()
			}
		}()
	}
	close(start)
	wg.Wait()

	require.Equal(t, 1, successCnt)
}

// TestTaskLocker_UnlockNeverLocked ensures Unlock on an unlocked vid does not panic.
func TestTaskLocker_UnlockNeverLocked(t *testing.T) {
	locker := &TaskLocker{taskMap: make(map[uint32]struct{})}
	ctx := context.Background()
	require.NotPanics(t, func() {
		locker.Unlock(ctx, 9999)
	})
}

// TestTaskLocker_VolAndShardLockerIndependent verifies that VolTaskLocker and
// ShardTaskLocker are completely independent: the same vid may be locked in both
// simultaneously without conflict.
func TestTaskLocker_VolAndShardLockerIndependent(t *testing.T) {
	MockEmptyVolTaskLocker()
	ShardTaskLockerInst().mu.Lock()
	ShardTaskLockerInst().taskMap = make(map[uint32]struct{})
	ShardTaskLockerInst().mu.Unlock()

	ctx := context.Background()
	const vid = uint32(1234)

	require.NoError(t, VolTaskLockerInst().TryLock(ctx, vid))
	require.NoError(t, ShardTaskLockerInst().TryLock(ctx, vid))

	require.Error(t, VolTaskLockerInst().TryLock(ctx, vid))
	require.Error(t, ShardTaskLockerInst().TryLock(ctx, vid))

	VolTaskLockerInst().Unlock(ctx, vid)
	ShardTaskLockerInst().Unlock(ctx, vid)
}

func TestShardTaskLockerInst(t *testing.T) {
	ctx := context.Background()

	// singleton: two calls return same instance
	locker1 := ShardTaskLockerInst()
	locker2 := ShardTaskLockerInst()
	require.Equal(t, locker1, locker2)

	// TryLock and Unlock
	err := locker1.TryLock(ctx, 9999)
	require.NoError(t, err)
	err = locker1.TryLock(ctx, 9999)
	require.EqualError(t, err, ErrVidTaskConflict.Error())
	locker1.Unlock(ctx, 9999)
	err = locker1.TryLock(ctx, 9999)
	require.NoError(t, err)
	locker1.Unlock(ctx, 9999)
}
