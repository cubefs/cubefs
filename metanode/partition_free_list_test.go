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
	_deleteInodeFileRollingSize = 8 * util.MB
	defer func() {
		_deleteInodeFileRollingSize = DeleteInodeFileRollingSize
	}()

	rootDir, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	defer os.RemoveAll(rootDir)
	config := &MetaPartitionConfig{
		PartitionId:   10001,
		VolName:       VolNameForTest,
		PartitionType: proto.VolumeTypeHot,
		RootDir:       rootDir,
	}
	mp := newPartition(config, &metadataManager{partitions: make(map[uint64]MetaPartition), volUpdating: new(sync.Map)})
	testCount := _deleteInodeFileRollingSize/8*2 + 1000
	for i := uint64(1); i < testCount; i += 1000 {
		inodes := make([]uint64, 1000)
		for idx := range inodes {
			inodes[idx] = i + uint64(idx)
		}
		mp.persistDeletedInodes(newCtx(), inodes)
	}
	dentries, err := os.ReadDir(rootDir)
	require.NoError(t, err)
	cnt := 0
	for _, dentry := range dentries {
		if strings.HasPrefix(dentry.Name(), DeleteInodeFileExtension) {
			cnt++
			info, err := os.Stat(path.Join(rootDir, dentry.Name()))
			require.NoError(t, err)
			t.Logf("found delete inode file %v size %v MB", dentry.Name(), info.Size()/util.MB)
		}
	}
	require.Greater(t, cnt, 1)
}
