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

package disk

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/util/log"

	"github.com/stretchr/testify/require"
)

func TestDiskMetricReport(t *testing.T) {
	testDir, err := ioutil.TempDir(os.TempDir(), "TestDiskMetricReport")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx := context.Background()

	diskpath := filepath.Join(testDir, "DiskPath")
	log.Info(diskpath)

	err = os.MkdirAll(diskpath, 0o755)
	require.NoError(t, err)

	diskConfig := core.Config{
		BaseConfig: core.BaseConfig{
			Path:       diskpath,
			AutoFormat: true,
		},
		HostInfo: core.HostInfo{
			ClusterID: proto.ClusterID(1),
			IDC:       "z0",
		},
		AllocDiskID:      getDiskIDFn,
		NotifyCompacting: setChunkCompactFn,
		HandleIOError:    handleIOErrorFn,
	}
	ds, err := NewDiskStorage(ctx, diskConfig)
	require.NoError(t, err)
	require.NotNil(t, ds)

	cs, err := ds.CreateChunk(ctx, proto.Vuid(1), core.DefaultChunkSize)
	require.NoError(t, err)
	require.NotNil(t, cs)

	cs, err = ds.CreateChunk(ctx, proto.Vuid(2), core.DefaultChunkSize)
	require.NoError(t, err)
	require.NotNil(t, cs)

	ds.metricReport(ctx)
}
