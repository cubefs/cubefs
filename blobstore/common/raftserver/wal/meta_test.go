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

package wal

import (
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/etcd/raft/v3/raftpb"
)

func TestMeta(t *testing.T) {
	dir := "/tmp/raftwal-" + string(genRandomBytes(8))
	clear := func() {
		err := os.RemoveAll(dir)
		if err != nil {
			t.Fatal(err)
		}
	}
	clear()
	defer clear()

	os.MkdirAll(dir, 0o755)
	mt, _, _, err := NewMeta(dir)
	require.Nil(t, err)
	defer mt.Close()
	require.Equal(t, 0, mt.curIdx)

	st := Snapshot{10, 1}
	mt.SaveSnapshot(st)
	require.Equal(t, 1, mt.curIdx)
	require.Equal(t, uint64(1), mt.mdatas[mt.curIdx].ver)

	hs := pb.HardState{
		Term:   2,
		Vote:   1,
		Commit: 20,
	}
	mt.SaveHardState(hs)
	require.Equal(t, 0, mt.curIdx)
	require.Equal(t, uint64(2), mt.mdatas[mt.curIdx].ver)

	st = Snapshot{20, 1}
	hs = pb.HardState{
		Term:   3,
		Vote:   2,
		Commit: 30,
	}
	mt.SaveAll(st, hs)
	require.Equal(t, 1, mt.curIdx)
	require.Equal(t, uint64(3), mt.mdatas[mt.curIdx].ver)

	mtData := metaData{}
	mtData.decode(mt.dataRef[mt.curIdx])
	require.Equal(t, uint64(3), mtData.ver)
	require.Equal(t, uint64(3), mtData.term)
	require.Equal(t, uint64(2), mtData.vote)
	require.Equal(t, uint64(30), mtData.commit)
	require.Equal(t, uint64(1), mtData.snapTerm)
	require.Equal(t, uint64(20), mtData.snapIndex)

	err = mt.Sync()
	require.Nil(t, err)
	mt.Close()

	// reload test
	mt, st, hs, _ = NewMeta(dir)
	require.Equal(t, 1, mt.curIdx)
	require.Equal(t, uint64(20), st.Index)
	require.Equal(t, uint64(1), st.Term)
	require.Equal(t, uint64(3), hs.Term)
	require.Equal(t, uint64(2), hs.Vote)
	require.Equal(t, uint64(30), hs.Commit)
}

func TestExceptionMeta(t *testing.T) {
	dir := "/tmp/raftwal-" + string(genRandomBytes(8))
	clear := func() {
		err := os.RemoveAll(dir)
		if err != nil {
			t.Fatal(err)
		}
	}
	clear()
	defer clear()

	os.MkdirAll(dir, 0o755)
	err := os.WriteFile(path.Join(dir, "meta01"), []byte("123456789"), 0o644)
	require.Nil(t, err)
	_, _, _, err = NewMeta(dir)
	require.NotNil(t, err)

	md := metaData{
		ver:       1,
		term:      1,
		vote:      1,
		commit:    1,
		snapTerm:  1,
		snapIndex: 1,
	}
	data := make([]byte, metaSize)
	md.encodeTo(data)
	data[0] = 3
	err = os.WriteFile(path.Join(dir, "meta01"), data, 0o644)
	require.Nil(t, err)
	err = os.WriteFile(path.Join(dir, "meta02"), data, 0o644)
	require.Nil(t, err)
	_, _, _, err = NewMeta(dir)
	require.NotNil(t, err)
}
