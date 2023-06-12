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

package fileutil

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLockAndUnlock(t *testing.T) {
	testDir, err := os.MkdirTemp(os.TempDir(), "testflock")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	pathname := filepath.Join(testDir, "testflock")
	println(pathname)

	// lock the file
	l, err := LockFile(pathname)
	require.NoError(t, err)
	require.NotNil(t, l)

	tl, err := TryLockFile(pathname)
	require.Nil(t, tl)
	require.Equal(t, err, ErrLocked)

	// unlock the file
	err = l.Unlock()
	require.NoError(t, err)

	// try lock the unlocked file
	l, err = TryLockFile(pathname)
	require.NoError(t, err)
	require.NotNil(t, l)

	// blocking on locked file
	locked := make(chan struct{}, 1)
	go func() {
		bl, blerr := LockFile(pathname)
		require.NoError(t, blerr)

		locked <- struct{}{}

		blerr = bl.Unlock()
		require.NoError(t, blerr)
	}()

	select {
	case <-locked:
		t.Error("unexpected unblocking")
	case <-time.After(100 * time.Millisecond):
	}

	// unlock
	err = l.Unlock()
	require.NoError(t, err)

	// the previously blocked routine should be unblocked
	select {
	case <-locked:
	case <-time.After(1 * time.Second):
		t.Error("unexpected blocking")
	}
}
