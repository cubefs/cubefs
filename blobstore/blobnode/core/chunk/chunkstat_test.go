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

package chunk

import (
	"testing"
	"time"

	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/stretchr/testify/require"
)

func TestChunkStats_write(t *testing.T) {
	stat := ChunkStats{}

	func() {
		stat.writeBefore()
		defer stat.writeAfter(1024, time.Now())

		require.Equal(t, int(1), int(stat.WriteInque))
		time.Sleep(time.Second * 1)
	}()

	require.Equal(t, int(0), int(stat.WriteInque))
	require.Equal(t, int(1), int(stat.TotalWriteCnt))
	require.Equal(t, int(1024), int(stat.TotalWriteBytes))
	log.Info(stat.TotalWriteDelay)
	require.True(t, int64(stat.TotalWriteDelay) >= int64(time.Second))
}

func TestChunkStats_write_1(t *testing.T) {
	stat := ChunkStats{}
	now := time.Now()

	stat.writeBefore()
	require.Equal(t, int(1), int(stat.WriteInque))
	time.Sleep(time.Second * 1)
	stat.writeAfter(1024, now)

	require.Equal(t, int(0), int(stat.WriteInque))
	require.Equal(t, int(1), int(stat.TotalWriteCnt))
	require.Equal(t, int(1024), int(stat.TotalWriteBytes))
	log.Info(stat.TotalWriteDelay)
	require.True(t, int64(stat.TotalWriteDelay) >= int64(time.Second))
}
