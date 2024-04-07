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
	"path"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/diskmon"
	"github.com/stretchr/testify/require"
)

func newMetaNodeForDiskTest(t *testing.T, dir string, rocksdbDir string, raftDir string) (mn *MetaNode) {
	var _ interface{} = t
	mn = &MetaNode{
		metadataDir: dir,
		rocksDirs: []string{
			rocksdbDir,
		},
		raftDir:        raftDir,
		disks:          make(map[string]*diskmon.FsCapMon),
		diskStopCh:     make(chan struct{}),
		rocksdbManager: NewRocksdbManager(0, 0),
	}
	return
}

func TestCheckRocksdbDisk(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	raftDir, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	rocksdbDir := path.Join(tempDir, "db")
	err = os.MkdirAll(rocksdbDir, 0o755)
	require.NoError(t, err)
	mn := newMetaNodeForDiskTest(t, tempDir, rocksdbDir, raftDir)
	err = mn.rocksdbManager.Register(tempDir)
	require.NoError(t, err)
	err = mn.rocksdbManager.Register(raftDir)
	require.NoError(t, err)
	err = mn.rocksdbManager.Register(rocksdbDir)
	require.NoError(t, err)
	err = mn.startDiskStat()
	require.NoError(t, err)
	time.Sleep(11 * time.Second)
	stats := mn.getRocksDBDiskStat()
	require.EqualValues(t, 1, len(stats))
	require.EqualValues(t, rocksdbDir, stats[0].Path)
	require.NotEqualValues(t, 0, stats[0].Used)
	require.EqualValues(t, proto.ReadWrite, stats[0].Status)
	mn.stopDiskStat()
}
