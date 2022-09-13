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
	"math/rand"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/clustermgr/kvmgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/kvdb"
	_ "github.com/cubefs/cubefs/blobstore/testing/nolog"
)

func TestConfigMgr(t *testing.T) {
	tmpKvDBPath := "/tmp/config/tmpKvDBPath" + strconv.Itoa(rand.Intn(1000000000))
	defer os.RemoveAll(tmpKvDBPath)

	kvDB, _ := kvdb.Open(tmpKvDBPath, false)
	kvMgr, err := kvmgr.NewKvMgr(kvDB)
	require.NoError(t, err)

	ctx := context.Background()
	cfMap := map[string]interface{}{
		"forbid_sync_config": false,
		"enable_recycle":     false,
		"idc":                []interface{}{"idc1", "idc2"},
		"test_map1":          map[string]interface{}{"a": 1, "b": 2},
	}

	configMgr, err := New(kvMgr, cfMap)
	require.NoError(t, err)

	ret, _ := configMgr.Get(ctx, "enable_recycle")
	var enableRec bool
	json.Unmarshal([]byte(ret), &enableRec)
	require.Equal(t, false, enableRec)

	err = configMgr.Delete(ctx, "enable_recycle")
	require.NoError(t, err)

	idcRet, err := configMgr.Get(ctx, "idc")
	require.NoError(t, err)
	var vv2 []interface{}
	json.Unmarshal([]byte(idcRet), &vv2)
	require.Equal(t, []interface{}{"idc1", "idc2"}, vv2)

	idcRet2, err := configMgr.Get(ctx, "test_map1")
	require.NoError(t, err)
	var vv3 map[string]interface{}
	json.Unmarshal([]byte(idcRet2), &vv3)
	require.Equal(t, map[string]interface{}{"a": float64(1), "b": float64(2)}, vv3)
}
