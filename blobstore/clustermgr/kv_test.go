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

package clustermgr

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

func TestKV(t *testing.T) {
	testService, clean := initTestService(t)
	defer clean()
	testClusterClient := initTestClusterClient(testService)
	ctx := newCtx()

	{
		err := testClusterClient.SetKV(ctx, "test1", []byte("value"))
		require.NoError(t, err)
		err = testClusterClient.SetKV(ctx, "", nil)
		require.Error(t, err)

		mode := []codemode.Policy{
			{ModeName: codemode.EC15P12.Name(), MinSize: 0, MaxSize: 1073741824, SizeRatio: 0.8, Enable: true},
		}
		val, _ := json.Marshal(mode)
		err = testClusterClient.SetKV(ctx, proto.CodeModeConfigKey, val)
		require.Error(t, err)

	}
	{
		v, err := testClusterClient.GetKV(ctx, "test1")
		require.NoError(t, err)
		require.Equal(t, string(v.Value), "value")

		_, err = testClusterClient.GetKV(ctx, "")
		require.Error(t, err)

		_, err = testClusterClient.GetKV(ctx, "no-exist-key")
		require.Error(t, err)
	}

	{
		err := testClusterClient.DeleteKV(ctx, "test1")
		require.NoError(t, err)
		_, err = testClusterClient.GetKV(ctx, "test1")
		require.Error(t, err)

		err = testClusterClient.DeleteKV(ctx, proto.CodeModeConfigKey)
		require.Error(t, err)
		err = testClusterClient.DeleteKV(ctx, proto.VolumeChunkSizeKey)
		require.Error(t, err)
		err = testClusterClient.DeleteKV(ctx, proto.VolumeReserveSizeKey)
		require.Error(t, err)
	}

	{
		_, err := testClusterClient.ListKV(ctx, &clustermgr.ListKvOpts{})
		require.NoError(t, err)
	}
}

func BenchmarkService_KvSet(b *testing.B) {
	testService, clean := initTestService(b)
	defer clean()
	testClusterClient := initTestClusterClient(testService)

	testCases := []struct {
		key   string
		value []byte
	}{
		{key: "test1", value: []byte("migrate-1023-1236-repair-task")},
		{key: "test2", value: []byte("repair-1")},
		{key: "test3", value: []byte("")},
		{key: "test4", value: []byte("test-repair")},
	}

	ctx := newCtx()
	b.ResetTimer()
	for i, tCase := range testCases {
		b.Run(fmt.Sprintf("case-%d", i), func(b *testing.B) {
			for ii := 0; ii < b.N; ii++ {
				testClusterClient.SetKV(ctx, tCase.key, tCase.value)
			}
		})
	}
}
