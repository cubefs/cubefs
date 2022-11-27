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

package configmgr

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/normaldb"
	_ "github.com/cubefs/cubefs/blobstore/testing/nolog"
)

func TestConfigMgr(t *testing.T) {
	testDir, err := ioutil.TempDir("", "cf")
	defer os.RemoveAll(testDir)
	ctx := context.Background()
	require.NoError(t, err)
	normalDB, err := normaldb.OpenNormalDB(testDir, false)
	require.NoError(t, err)

	cfMap := map[string]interface{}{
		"forbid_sync_config": false,
		"enable_recycle":     false,
		"idc":                []interface{}{"idc1", "idc2"},
		"test_map1":          map[string]interface{}{"a": 1, "b": 2},
	}

	configmgr, err := New(normalDB, cfMap)
	require.NoError(t, err)

	cfList, _ := configmgr.List(ctx)
	require.Equal(t, 4, len(cfList))

	ret2, _ := configmgr.Get(ctx, "enable_recycle")
	var vv bool
	json.Unmarshal([]byte(ret2), &vv)
	require.Equal(t, false, vv)

	err = configmgr.Delete(ctx, "enable_recycle")
	require.NoError(t, err)

	cfList2, _ := configmgr.List(ctx)
	require.Equal(t, 4, len(cfList2))

	idcRet, err := configmgr.Get(ctx, "idc")
	require.NoError(t, err)
	var vv2 []interface{}
	json.Unmarshal([]byte(idcRet), &vv2)
	require.Equal(t, []interface{}{"idc1", "idc2"}, vv2)

	idcRet2, err := configmgr.Get(ctx, "test_map1")
	require.NoError(t, err)
	var vv3 map[string]interface{}
	json.Unmarshal([]byte(idcRet2), &vv3)
	require.Equal(t, map[string]interface{}{"a": float64(1), "b": float64(2)}, vv3)
}
