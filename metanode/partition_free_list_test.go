// Copyright 2018 The CubeFS Authors.
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
	"path"
	"strings"
	"testing"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/fileutil"
	"github.com/stretchr/testify/require"
)

var (
	VolNameForFreeListTest = "TestForFreeList"
)

func newPartitionForFreeList(conf *MetaPartitionConfig, manager *metadataManager) (mp *metaPartition) {
	mp = &metaPartition{
		config:        conf,
		dentryTree:    NewBtree(),
		inodeTree:     NewBtree(),
		extendTree:    NewBtree(),
		multipartTree: NewBtree(),
		stopC:         make(chan bool),
		storeChan:     make(chan *storeMsg, 100),
		freeList:      newFreeList(),
		extDelCh:      make(chan []proto.ExtentKey, defaultDelExtentsCnt),
		extReset:      make(chan struct{}),
		vol:           NewVol(),
		manager:       manager,
	}
	mp.config.Cursor = 0
	mp.config.End = 100000
	mp.uidManager = NewUidMgr(conf.VolName, mp.config.PartitionId)
	mp.mqMgr = NewQuotaManager(conf.VolName, mp.config.PartitionId)
	return mp
}

func TestPersistInodesFreeList(t *testing.T) {
	rootDir, err := os.MkdirTemp("", "")
	defer os.RemoveAll(rootDir)
	require.NoError(t, err)
	config := &MetaPartitionConfig{
		PartitionId:   10001,
		VolName:       VolNameForFreeListTest,
		PartitionType: proto.VolumeTypeHot,
		RootDir:       rootDir,
	}
	mp := newPartitionForFreeList(config, &metadataManager{partitions: make(map[uint64]MetaPartition)})
	const testCount = DeleteInodeFileRollingSize/8 + 1000
	inodes := make([]uint64, 0, testCount)
	for i := 0; i < testCount; i++ {
		inodes = append(inodes, uint64(i))
	}
	mp.persistDeletedInodes([]uint64{0})
	fileName := path.Join(config.RootDir, DeleteInodeFileExtension)
	oldIno, err := fileutil.Stat(fileName)
	require.NoError(t, err)
	mp.persistDeletedInodes(inodes)
	dentries, err := os.ReadDir(rootDir)
	require.NoError(t, err)
	// NOTE: rolling must happend once
	cnt := 0
	for _, dentry := range dentries {
		if strings.HasPrefix(dentry.Name(), DeleteInodeFileExtension) {
			cnt++
			info, err := os.Stat(path.Join(rootDir, dentry.Name()))
			require.NoError(t, err)
			t.Logf("found delete inode file %v size %v MB", dentry.Name(), info.Size()/util.MB)
		}
	}
	if cnt < 2 {
		nowIno, err := fileutil.Stat(fileName)
		require.NoError(t, err)
		require.NotEqual(t, oldIno.Ino, nowIno.Ino)
		return
	}
	require.Greater(t, cnt, 1)
}
