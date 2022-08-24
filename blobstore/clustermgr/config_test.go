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
	"testing"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/proto"

	"github.com/stretchr/testify/require"
)

func TestConfig(t *testing.T) {
	testService, clean := initTestService(t)
	defer clean()
	testClusterClient := initTestClusterClient(testService)
	ctx := newCtx()

	// test set config
	{
		err := testClusterClient.SetConfig(ctx, &clustermgr.ConfigSetArgs{Key: "idc1", Value: "idc1"})
		require.NoError(t, err)

		b, err := json.Marshal(1)
		require.NoError(t, err)
		err = testClusterClient.SetConfig(ctx, &clustermgr.ConfigSetArgs{Key: "num", Value: string(b)})
		require.NoError(t, err)

		// failed case
		err = testClusterClient.SetConfig(ctx, &clustermgr.ConfigSetArgs{Key: proto.CodeModeConfigKey, Value: string(b)})
		require.Error(t, err)
	}

	// test get config
	{
		key := "idc1"
		val, err := testClusterClient.GetConfig(ctx, key)
		require.NoError(t, err)
		require.Equal(t, "idc1", val)

		key2 := "num"
		var n int
		val, err = testClusterClient.GetConfig(ctx, key2)
		require.NoError(t, err)
		err = json.Unmarshal([]byte(val), &n)
		require.NoError(t, err)
		require.Equal(t, 1, n)
	}

	// test list config
	{
		_, err := testClusterClient.ListConfig(ctx)
		require.NoError(t, err)
	}

	// test delete config
	{
		key := "idc1"
		err := testClusterClient.DeleteConfig(ctx, key)
		require.NoError(t, err)
		_, err = testClusterClient.GetConfig(ctx, key)
		require.Error(t, err)
	}
}
