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

package core

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/common/proto"
)

func TestFormatInfo(t *testing.T) {
	formatInfo := &FormatInfo{}
	formatInfo.FormatInfoProtectedField = FormatInfoProtectedField{
		DiskID:  proto.DiskID(101),
		Version: 1,
		Format:  FormatMetaTypeV1,
		Ctime:   time.Now().UnixNano(),
	}

	checkSum, err := formatInfo.calCheckSumV2()
	require.NoError(t, err)
	formatInfo.CheckSum = checkSum

	ctx := context.Background()
	diskPath, err := os.MkdirTemp(os.TempDir(), "BlobNodeTestFormatInfo")
	require.NoError(t, err)
	defer os.RemoveAll(diskPath)

	sysPath := filepath.Join(diskPath, ".sys")
	err = os.MkdirAll(sysPath, 0o755)
	require.NoError(t, err)

	oldFormatJSON := fmt.Sprintf(`{"version":1,"diskid":101,"ctime":%d,"format":"fs","check_sum":%d}`,
		formatInfo.Ctime, checkSum)
	formatFile := filepath.Join(sysPath, ".format.json")
	err = os.WriteFile(formatFile, []byte(oldFormatJSON), 0o644)
	require.NoError(t, err)

	_, err = ReadFormatInfo(ctx, diskPath)
	require.NotNil(t, err)
	require.ErrorIs(t, err, ErrFormatV2CrcIsEmpty)

	// update formatInfo, with nodeID
	formatInfo.NodeID = proto.NodeID(2)
	formatInfo.NodeCtime = time.Now().UnixNano()
	// formatInfo.Version = DiskFormatVersionNode
	err = formatInfo.CalCheckSum()
	require.NoError(t, err)
	err = SaveDiskFormatInfo(ctx, diskPath, formatInfo)
	require.NoError(t, err)

	info, err := ReadFormatInfo(ctx, diskPath)
	require.NoError(t, err)
	require.Equal(t, info.NodeID, formatInfo.NodeID)
	require.NotEqual(t, proto.NodeID(0), info.NodeID)
}

func TestEnsureDiskArea(t *testing.T) {
	diskPath := "!!"
	err := EnsureDiskArea(diskPath, "")
	require.Error(t, err)
}

func TestOldVersionFomatInfo(t *testing.T) {
	ctx := context.Background()

	diskPath, err := os.MkdirTemp(os.TempDir(), "BlobNodeTestOldVersionFormatInfo")
	require.NoError(t, err)
	defer os.RemoveAll(diskPath)

	sysPath := filepath.Join(diskPath, ".sys")
	err = os.MkdirAll(sysPath, 0o755)
	require.NoError(t, err)

	oldFormatContent := `{"diskid":4,"version":1,"ctime":1766558856695434352,"format":"fs","check_sum":1501699157}`
	formatFile := filepath.Join(sysPath, ".format.json")
	err = os.WriteFile(formatFile, []byte(oldFormatContent), 0o644)
	require.NoError(t, err)

	// read old version
	info, err := ReadFormatInfo(ctx, diskPath)
	require.ErrorIs(t, err, ErrFormatV2CrcIsEmpty)
	require.NotNil(t, info)

	require.Equal(t, proto.DiskID(4), info.DiskID)
	require.Equal(t, uint8(1), info.Version)
	require.Equal(t, int64(1766558856695434352), info.Ctime)
	require.Equal(t, FormatMetaTypeV1, info.Format)
	require.Equal(t, uint32(1501699157), info.CheckSum)

	require.Equal(t, proto.NodeID(0), info.NodeID)
	require.Equal(t, int64(0), info.NodeCtime)

	// update formatInfo, nodeID
	info.NodeID = proto.NodeID(2)
	info.NodeCtime = time.Now().UnixNano()
	// info.Version = DiskFormatVersionNode

	err = info.CalCheckSum()
	require.NoError(t, err)
	err = SaveDiskFormatInfo(ctx, diskPath, info)
	require.NoError(t, err)

	infoNew, err := ReadFormatInfo(ctx, diskPath)
	require.NoError(t, err)
	require.Equal(t, *info, *infoNew)

	// rollback, startup read old version
	info.Version = 1
	info, err = ReadFormatInfo(ctx, diskPath)
	require.NoError(t, err)
	err = info.Verify()
	require.NoError(t, err)

	newFormatContent := `{"diskid":4,"version":1,"ctime":1766558856695434352,"format":"fs",
		"nodeid":2,"node_ctime":1767166122300324697,"check_sum":1501699157,"check_sum_v2":1111,"xxx":0}`
	err = os.WriteFile(formatFile, []byte(newFormatContent), 0o644)
	require.NoError(t, err)

	configFile := filepath.Join(sysRootPath(diskPath), formatConfigFile)
	buf, err := os.ReadFile(configFile)
	require.NoError(t, err)

	infoV1 := &FormatInfoV1{}
	err = json.Unmarshal(buf, infoV1)
	require.NoError(t, err)
	require.Equal(t, uint32(1501699157), infoV1.CheckSum)
}
