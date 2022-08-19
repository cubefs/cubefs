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

package kvdb

import (
	"math/rand"
	"os"
	"strconv"
	"testing"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/stretchr/testify/require"
)

var (
	kvDB     *KvDB
	kvTbl    *KvTable
	kvDBPath = "/tmp/kvdb"
)

func initKvDB() {
	kvDBPath += strconv.Itoa(rand.Intn(20000))

	var err error
	kvDB, err = Open(kvDBPath, false)
	if err != nil {
		log.Error("open db error")
	}

	kvTbl, err = OpenKvTable(kvDB)
	if err != nil {
		log.Error("open kv table error")
	}
}

func closeKvDB() {
	kvDB.Close()
	os.RemoveAll(kvDBPath)
}

func TestKvTable(t *testing.T) {
	initKvDB()
	defer closeKvDB()

	putCases := []struct {
		key   string
		value []byte
	}{
		{
			key:   "repair-100-10000-1",
			value: []byte("disk-100-vid-10000-test1"),
		},
		{
			key:   "repair-100-10000-2",
			value: []byte("disk-101-vid-10000-test2"),
		},
		{
			key:   "repair-100-10000-11",
			value: []byte("disk-101-vid-10000-test3"),
		},
		{
			key:   "repair-101-10001-1",
			value: []byte("disk-101-vid-10001-test4"),
		},
		{
			key:   "repair-1000-1000-1",
			value: []byte("disk-1000-vid-1000-test5"),
		},
	}

	for _, c := range putCases {
		err := kvTbl.Set([]byte(c.key), c.value)
		require.NoError(t, err)
	}

	for i, c := range putCases {
		val, err := kvTbl.Get([]byte(c.key))
		require.NoError(t, err)
		require.Equal(t, val, putCases[i].value)
	}

	listCases := []clustermgr.ListKvOpts{
		{
			Prefix: "1",
			Marker: "",
			Count:  0,
		},
		{
			Prefix: "repair-100-",
			Marker: "",
			Count:  1,
		},
		{
			Prefix: "repair-100",
			Marker: "",
			Count:  3,
		},
		{
			Prefix: "repair-100-10000-",
			Marker: "",
			Count:  2,
		},
	}

	for i, c := range listCases {
		ret, err := kvTbl.List(&c)
		require.NoError(t, err)
		require.Equal(t, len(ret), listCases[i].Count)
	}

	err := kvTbl.Delete([]byte(putCases[1].key))
	require.NoError(t, err)

	err = kvTbl.Delete([]byte("not exist key"))
	require.NoError(t, err)

	v, err := kvTbl.Get([]byte(putCases[1].key))
	require.Error(t, err)
	require.Equal(t, []byte(nil), v)

	ret, err := kvTbl.List(&clustermgr.ListKvOpts{
		Prefix: "repair-100-",
		Marker: "",
		Count:  3,
	})
	require.NoError(t, err)
	require.Equal(t, len(ret), 2)
}
