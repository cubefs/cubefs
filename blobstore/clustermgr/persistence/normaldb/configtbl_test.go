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

package normaldb

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfigTbl(t *testing.T) {
	testDir, _ := ioutil.TempDir("", "cf")
	defer os.RemoveAll(testDir)
	configDB, err := OpenNormalDB(testDir, false)
	require.NoError(t, err)
	ct, err := OpenConfigTable(configDB)
	require.NoError(t, err)
	{
		key1 := "test1"
		val1 := []byte("123")
		err = ct.Update([]byte(key1), val1)

		require.NoError(t, err)

		ret, err := ct.Get(key1)
		require.NoError(t, err)

		require.Equal(t, "123", ret)
	}

	{
		key2 := "test2"
		val2, _ := json.Marshal(123)

		err = ct.Update([]byte(key2), val2)
		require.NoError(t, err)

		ret, err := ct.Get(key2)
		require.NoError(t, err)

		var vv int
		err = json.Unmarshal([]byte(ret), &vv)
		require.NoError(t, err)

		require.Equal(t, 123, vv)
	}

	{
		ret, err := ct.List()
		require.NoError(t, err)
		require.Equal(t, "123", ret["test1"])
	}

	{
		err = ct.Delete("test1")
		require.NoError(t, err)
		ret, err := ct.Get("test1")
		require.Error(t, err)
		require.Equal(t, "", ret)
	}
}
