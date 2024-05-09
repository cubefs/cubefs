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
	"sync"
	"testing"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/stretchr/testify/require"
)

func TestPersistInodesFreeList(t *testing.T) {
	rootDir, err := os.MkdirTemp("", "")
	defer os.RemoveAll(rootDir)
	require.NoError(t, err)
	config := &MetaPartitionConfig{
		PartitionId:   10001,
		VolName:       VolNameForTest,
		PartitionType: proto.VolumeTypeHot,
		RootDir:       rootDir,
	}
	mp := newPartitionForFreeList(config, &metadataManager{partitions: make(map[uint64]MetaPartition)})
	t.Logf("Persist one inode")
	mp.persistDeletedInodes([]uint64{0})
	fileName := path.Join(config.RootDir, DeleteInodeFileExtension)
	oldIno, err := fileutil.Stat(fileName)
	require.NoError(t, err)
	t.Logf("Persist many inodes")
	const persistBatchCount = 50000
	const testCount = DeleteInodeFileRollingSize / 8
	inodes := make([]uint64, 0, persistBatchCount)
	for i := 0; i < persistBatchCount; i++ {
		inodes = append(inodes, uint64(i)+1000000)
	}
	for i := 0; i < testCount; i += len(inodes) {
		mp.persistDeletedInodes(inodes)
		t.Logf("Persist %v inodes", i)
	}
	dentries, err := fileutil.ReadDir(rootDir)
	require.NoError(t, err)
	cnt := 0
	for _, dentry := range dentries {
		if strings.HasPrefix(dentry, DeleteInodeFileExtension) {
			cnt++
			info, err := os.Stat(path.Join(rootDir, dentry))
			require.NoError(t, err)
			t.Logf("found delete inode file %v size %v MB", dentry, info.Size()/util.MB)
		}
	}
	if cnt < 2 {
		nowIno, err := fileutil.Stat(fileName)
		require.NoError(t, err)
		require.NotEqualValues(t, oldIno.Ino, nowIno.Ino)
		return
	}
	require.Greater(t, cnt, 1)
}
