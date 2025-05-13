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
	"bytes"
	"context"
	"math/rand"
	"os"
	"strconv"
	"testing"

	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/normaldb"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/raftdb"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"

	"github.com/stretchr/testify/require"
)

func TestEncodeSnapshotData(t *testing.T) {
	dbName := "testDB"
	cfName := "testCF"
	key := "testKey"
	value := "testValue"

	snap := &SnapshotData{
		Header: SnapshotItem{DbName: dbName, CfName: cfName},
		Key:    []byte(key),
		Value:  []byte(value),
	}

	data, err := EncodeSnapshotData(snap)
	require.NoError(t, err)

	decodeSnap, err := DecodeSnapshotData(bytes.NewBuffer(data))
	require.NoError(t, err)

	require.Equal(t, snap.Header.DbName, decodeSnap.Header.DbName)
	require.Equal(t, snap.Header.CfName, decodeSnap.Header.CfName)
	require.Equal(t, snap.Key, decodeSnap.Key)
	require.Equal(t, snap.Value, decodeSnap.Value)
}

func TestSnapshot(t *testing.T) {
	tmpNormalDBPat1 := "/tmp/tmpsnapshotnormaldb1" + strconv.Itoa(rand.Intn(1000000000))
	os.MkdirAll(tmpNormalDBPat1, 0o755)
	defer os.RemoveAll(tmpNormalDBPat1)
	tmpNormalDBPat2 := "/tmp/tmpsnapshotnormaldb2" + strconv.Itoa(rand.Intn(1000000000))
	os.MkdirAll(tmpNormalDBPat2, 0o755)
	defer os.RemoveAll(tmpNormalDBPat2)

	normalDB1, err := normaldb.OpenNormalDB(tmpNormalDBPat1)
	require.NoError(t, err)
	defer normalDB1.Close()

	normalDB2, err := normaldb.OpenNormalDB(tmpNormalDBPat2)
	require.NoError(t, err)
	defer normalDB2.Close()

	_, ctx := trace.StartSpanFromContext(context.Background(), "")

	// insert test data
	{
		scopeTbl1, err := normaldb.OpenScopeTable(normalDB1)
		require.NoError(t, err)
		err = scopeTbl1.Put("testName", uint64(1))
		require.NoError(t, err)

		diskDropTbl1, err := normaldb.OpenBlobNodeDroppedDiskTable(normalDB1)
		require.NoError(t, err)
		err = diskDropTbl1.AddDroppingDisk(proto.DiskID(1))
		require.NoError(t, err)

		scopeTbl2, err := normaldb.OpenScopeTable(normalDB2)
		require.NoError(t, err)
		err = scopeTbl2.Put("testName", 2)
		require.NoError(t, err)

		diskDropTbl2, err := normaldb.OpenBlobNodeDroppedDiskTable(normalDB2)
		require.NoError(t, err)
		err = diskDropTbl2.AddDroppingDisk(proto.DiskID(2))
		require.NoError(t, err)
	}

	// create snapshot and apply snapshot
	{
		tmpDBPath := "/tmp/tmpraftdb" + strconv.Itoa(rand.Intn(1000000000))
		os.MkdirAll(tmpDBPath, 0o755)
		defer os.RemoveAll(tmpDBPath)
		raftDB, err := raftdb.OpenRaftDB(tmpDBPath)
		require.NoError(t, err)
		defer raftDB.Close()

		dbName1 := "normal1"
		dbName2 := "normal2"
		raftNode, err := NewRaftNode(&RaftNodeConfig{FlushNumInterval: 1, TruncateNumInterval: 1, ApplyIndex: 1}, raftDB, map[string]SnapshotDB{dbName1: normalDB1, dbName2: normalDB2})
		require.NoError(t, err)

		snapshot := raftNode.CreateRaftSnapshot(10)
		defer snapshot.Close()

		snapshot.Name()
		index := snapshot.Index()
		require.Equal(t, uint64(1), index)

		applyNormalDBPat1 := "/tmp/tmpsnapshotnormaldb1" + strconv.Itoa(rand.Intn(1000000000))
		defer os.RemoveAll(applyNormalDBPat1)
		applyNormalDBPat2 := "/tmp/tmpsnapshotnormaldb2" + strconv.Itoa(rand.Intn(1000000000))
		defer os.RemoveAll(applyNormalDBPat2)

		applyNormalDB1, err := normaldb.OpenNormalDB(applyNormalDBPat1)
		require.NoError(t, err)
		defer applyNormalDB1.Close()

		applyNormalDB2, err := normaldb.OpenNormalDB(applyNormalDBPat2)
		require.NoError(t, err)
		defer applyNormalDB2.Close()

		err = raftNode.ApplyRaftSnapshot(ctx, snapshot)
		require.NoError(t, err)
	}
}
