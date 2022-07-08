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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/common/proto"
)

func MockEmptyVolTaskLocker() {
	VolTaskLockerInst().mu.Lock()
	defer VolTaskLockerInst().mu.Unlock()
	VolTaskLockerInst().taskMap = make(map[proto.Vid]struct{})
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
