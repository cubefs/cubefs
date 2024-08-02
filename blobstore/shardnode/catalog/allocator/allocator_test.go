// Copyright 2024 The CubeFS Authors.
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

package allocator

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/common/codemode"
)

func TestNewAllocator(t *testing.T) {
	ctx := context.Background()
	tp := NewMockAllocTransport(t)

	alc, err := NewAllocator(ctx, BlobConfig{}, VolConfig{}, tp)
	require.NoError(t, err)
	defer alc.Close()
	require.NotNil(t, alc)
}

func TestAllocSlices(t *testing.T) {
	ctx := context.Background()
	tp := NewMockAllocTransport(t)

	alc, err := NewAllocator(ctx, BlobConfig{}, VolConfig{}, tp)
	require.NoError(t, err)
	defer alc.Close()

	time.Sleep(100 * time.Millisecond)

	fileSize := uint64(1024 * 10)
	sliceSize := uint32(64)
	n := blobCount(fileSize, sliceSize)

	ret, err := alc.AllocSlices(ctx, codemode.EC6P6, fileSize, sliceSize)
	require.NoError(t, err)
	require.Equal(t, uint32(n), ret[0].Count)
}
