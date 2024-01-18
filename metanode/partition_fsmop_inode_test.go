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

package metanode

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/proto"
)

const testPath = "/tmp/testMetaPartition/"

func newMP() *metaPartition {
	mpC := &MetaPartitionConfig{
		PartitionId:   1,
		VolName:       "test_vol",
		Start:         0,
		End:           100,
		PartitionType: 1,
		Peers:         nil,
		RootDir:       testPath,
	}
	metaM := &metadataManager{
		nodeId:     1,
		zoneName:   "test",
		raftStore:  nil,
		partitions: make(map[uint64]MetaPartition),
		metaNode:   &MetaNode{},
	}

	partition := NewMetaPartition(mpC, metaM)
	mp := partition.(*metaPartition)
	mp.uidManager = NewUidMgr(mpC.VolName, mpC.PartitionId)
	return mp
}

func newRequestInfo() *RequestInfo {
	return &RequestInfo{
		RequestTime:        time.Now().UnixNano(),
		DataCrc:            1,
		ReqID:              12345,
		EnableRemoveDupReq: true,

		ClientInfo: proto.ClientInfo{
			ClientID:        8888,
			ClientIP:        456,
			ClientStartTime: time.Now().UnixNano(),
		},
	}
}

func TestInode_ExtentsTruncate(t *testing.T) {

	os.RemoveAll(testPath)
	defer os.RemoveAll(testPath)

	mp := newMP()
	require.NotNil(t, mp)

	// add data to mp
	ino := NewInode(0, 0)
	mp.inodeTree.ReplaceOrInsert(ino, true)
	dentry := &Dentry{ParentId: 0, Name: "/", Inode: 0}
	mp.dentryTree.ReplaceOrInsert(dentry, true)
	extend := &Extend{inode: 0}
	mp.extendTree.ReplaceOrInsert(extend, true)

	multipart := &Multipart{
		id:       "id",
		key:      "key",
		initTime: time.Unix(0, 0),
		parts:    Parts{},
		extend:   MultipartExtend{},
	}
	mp.multipartTree.ReplaceOrInsert(multipart, true)

	requestInfo := newRequestInfo()

	se := NewSortedExtents()
	se.AppendWithCheck(proto.ExtentKey{FileOffset: 0, Size: 1000, ExtentId: 1}, nil)
	se.AppendWithCheck(proto.ExtentKey{FileOffset: 2000, Size: 1000, ExtentId: 2}, nil)
	se.AppendWithCheck(proto.ExtentKey{FileOffset: 4000, Size: 1000, ExtentId: 3}, nil)
	se.AppendWithCheck(proto.ExtentKey{FileOffset: 3000, Size: 500, ExtentId: 4}, nil)
	ino.Extents = se
	extents := mp.fsmAppendExtents(ino, requestInfo)
	require.Equal(t, proto.OpOk, extents)
	extents = mp.fsmAppendExtents(ino, requestInfo)
	require.Equal(t, proto.OpOk, extents)

	ino1 := ino.Copy().(*Inode)
	ino1.Size = 2000
	requestInfo = newRequestInfo()
	requestInfo.ReqID = 123456
	response := mp.fsmExtentsTruncate(ino1, requestInfo)
	require.Equal(t, proto.OpOk, response.Status)

	// write new data with new request
	requestInfo1 := newRequestInfo()
	requestInfo1.ReqID = 1234567
	se = NewSortedExtents()
	se.AppendWithCheck(proto.ExtentKey{FileOffset: 5000, Size: 600, ExtentId: 5}, nil)
	ino2 := ino.Copy().(*Inode)
	ino2.Extents = se
	extents = mp.fsmAppendExtents(ino2, requestInfo1)
	require.Equal(t, proto.OpOk, extents)
	require.Equal(t, len(mp.inodeTree.CopyGet(ino).(*Inode).Extents.eks), 2)
	require.Equal(t, mp.inodeTree.CopyGet(ino).(*Inode).Size, uint64(5600))

	// truncate again with same request as first time, if remove dup req success, the len of eks should be 2
	response = mp.fsmExtentsTruncate(ino1, requestInfo)
	require.Equal(t, proto.OpOk, response.Status)
	require.Equal(t, len(mp.inodeTree.CopyGet(ino).(*Inode).Extents.eks), 2)
	require.Equal(t, mp.inodeTree.CopyGet(ino).(*Inode).Size, uint64(5600))
}

func TestLinkAndUnlink(t *testing.T) {
	os.RemoveAll(testPath)
	defer os.RemoveAll(testPath)

	mp := newMP()
	require.NotNil(t, mp)

	// add data to mp
	ino := NewInode(0, 0)
	mp.inodeTree.ReplaceOrInsert(ino, true)
	dentry := &Dentry{ParentId: 0, Name: "/", Inode: 0}
	mp.dentryTree.ReplaceOrInsert(dentry, true)
	extend := &Extend{inode: 0}
	mp.extendTree.ReplaceOrInsert(extend, true)

	requestInfo := newRequestInfo()
	linkInode := mp.fsmCreateLinkInode(ino, 0, requestInfo)
	require.NotNil(t, linkInode)
	require.Equal(t, proto.OpOk, linkInode.Status)
	require.Equal(t, uint32(2), linkInode.Msg.NLink)

	// repeated request should link failed, the result should be same with first request
	linkInode1 := mp.fsmCreateLinkInode(ino, 0, requestInfo)
	require.NotNil(t, linkInode1)
	require.Equal(t, proto.OpOk, linkInode1.Status)
	require.Equal(t, uint32(2), linkInode1.Msg.NLink)

	requestInfo1 := newRequestInfo()
	requestInfo1.ReqID = 3345
	unlinkInode := mp.fsmUnlinkInode(ino, 1, requestInfo1)
	require.NotNil(t, unlinkInode)
	require.Equal(t, proto.OpOk, unlinkInode.Status)
	require.Equal(t, uint32(1), unlinkInode.Msg.NLink)

	unlinkInode1 := mp.fsmUnlinkInode(ino, 1, requestInfo1)
	require.NotNil(t, unlinkInode1)
	require.Equal(t, proto.OpOk, unlinkInode1.Status)
	require.Equal(t, uint32(1), unlinkInode1.Msg.NLink)
}
