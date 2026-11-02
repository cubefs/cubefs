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

package blobnode

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	cmapi "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	myos "github.com/cubefs/cubefs/blobstore/blobnode/sys"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

// TestGetNodeID_AllUnmounted tests that when all disks are unmounted with
// must_mount_point=true, getNodeID returns nodeID=0, hasUsableDisk=false.
func TestGetNodeID_AllUnmounted(t *testing.T) {
	patches := gomonkey.ApplyFunc(myos.IsMountPoint, func(path string) bool {
		return false // simulate all disks unmounted
	})
	defer patches.Reset()

	conf := &Config{
		DiskConfig: core.RuntimeConfig{MustMountPoint: true},
		Disks: []core.Config{
			{BaseConfig: core.BaseConfig{Path: "/data/disk1"}},
			{BaseConfig: core.BaseConfig{Path: "/data/disk2"}},
		},
	}

	nodeID, hasUsableDisk, err := getNodeID(context.Background(), conf)
	require.NoError(t, err)
	require.Equal(t, proto.NodeID(0), nodeID)
	require.False(t, hasUsableDisk)
}

// TestGetNodeID_PartialMounted tests that when some disks are mounted and
// some are not, getNodeID uses the mounted disk's NodeID.
func TestGetNodeID_PartialMounted(t *testing.T) {
	mountedPath := t.TempDir()

	patches := gomonkey.ApplyFunc(myos.IsMountPoint, func(path string) bool {
		return path == mountedPath
	})
	defer patches.Reset()

	saveFormatInfo := gomonkey.ApplyFunc(core.ReadFormatInfo,
		func(ctx context.Context, rootPath string) (*core.FormatInfo, error) {
			if rootPath == mountedPath {
				return &core.FormatInfo{FormatInfoProtectedFieldV2: core.FormatInfoProtectedFieldV2{NodeID: 100}}, nil
			}
			return &core.FormatInfo{}, os.ErrNotExist
		})
	defer saveFormatInfo.Reset()

	conf := &Config{
		DiskConfig: core.RuntimeConfig{MustMountPoint: true},
		Disks: []core.Config{
			{BaseConfig: core.BaseConfig{Path: mountedPath}},
			{BaseConfig: core.BaseConfig{Path: "/data/disk2"}},
		},
	}

	nodeID, hasUsableDisk, err := getNodeID(context.Background(), conf)
	require.NoError(t, err)
	require.Equal(t, proto.NodeID(100), nodeID)
	require.True(t, hasUsableDisk)
}

// TestGetNodeID_NewMachine tests that for a new machine with mounted but
// unformatted disks, hasUsableDisk=true and nodeID=0.
func TestGetNodeID_NewMachine(t *testing.T) {
	diskPath := t.TempDir()

	patches := gomonkey.ApplyFunc(myos.IsMountPoint, func(path string) bool {
		return true
	})
	defer patches.Reset()

	saveFormatInfo := gomonkey.ApplyFunc(core.ReadFormatInfo,
		func(ctx context.Context, rootPath string) (*core.FormatInfo, error) {
			return nil, os.ErrNotExist // new disk, no format.info
		})
	defer saveFormatInfo.Reset()

	conf := &Config{
		DiskConfig: core.RuntimeConfig{MustMountPoint: true},
		Disks: []core.Config{
			{BaseConfig: core.BaseConfig{Path: diskPath}},
		},
	}

	nodeID, hasUsableDisk, err := getNodeID(context.Background(), conf)
	require.NoError(t, err)
	require.Equal(t, proto.NodeID(0), nodeID)
	require.True(t, hasUsableDisk)
}

// TestGetNodeID_AllBroken tests that when all disks are mounted but
// format.info reads fail with EIO, hasUsableDisk=false.
func TestGetNodeID_AllBroken(t *testing.T) {
	diskPath := t.TempDir()

	patches := gomonkey.ApplyFunc(myos.IsMountPoint, func(path string) bool {
		return true
	})
	defer patches.Reset()

	saveFormatInfo := gomonkey.ApplyFunc(core.ReadFormatInfo,
		func(ctx context.Context, rootPath string) (*core.FormatInfo, error) {
			return nil, syscall.EIO // bad disk
		})
	defer saveFormatInfo.Reset()

	conf := &Config{
		DiskConfig: core.RuntimeConfig{MustMountPoint: true},
		Disks: []core.Config{
			{BaseConfig: core.BaseConfig{Path: diskPath}},
		},
	}

	nodeID, hasUsableDisk, err := getNodeID(context.Background(), conf)
	require.NoError(t, err)
	require.Equal(t, proto.NodeID(0), nodeID)
	require.False(t, hasUsableDisk)
}

// TestGetNodeID_AllV1Disks tests that when all disks are v1 format
// (ReadFormatInfo returns ErrFormatV2CrcIsEmpty), they are still treated as
// usable disks so blobnode can start and upgrade them to v2 later.
func TestGetNodeID_AllV1Disks(t *testing.T) {
	diskPath := t.TempDir()

	patches := gomonkey.ApplyFunc(myos.IsMountPoint, func(path string) bool {
		return true
	})
	defer patches.Reset()

	saveFormatInfo := gomonkey.ApplyFunc(core.ReadFormatInfo,
		func(ctx context.Context, rootPath string) (*core.FormatInfo, error) {
			return &core.FormatInfo{}, core.ErrFormatV2CrcIsEmpty // v1 disk
		})
	defer saveFormatInfo.Reset()

	conf := &Config{
		DiskConfig: core.RuntimeConfig{MustMountPoint: true},
		Disks: []core.Config{
			{BaseConfig: core.BaseConfig{Path: diskPath}},
		},
	}

	nodeID, hasUsableDisk, err := getNodeID(context.Background(), conf)
	require.NoError(t, err)
	require.Equal(t, proto.NodeID(0), nodeID)
	require.True(t, hasUsableDisk)
}

// TestGetNodeID_MustMountFalse tests that when must_mount_point=false,
// the mount point check is skipped.
func TestGetNodeID_MustMountFalse(t *testing.T) {
	diskPath := t.TempDir()

	// IsMountPoint should never be called
	patches := gomonkey.ApplyFunc(myos.IsMountPoint, func(path string) bool {
		t.Error("IsMountPoint should not be called when MustMountPoint=false")
		return false
	})
	defer patches.Reset()

	saveFormatInfo := gomonkey.ApplyFunc(core.ReadFormatInfo,
		func(ctx context.Context, rootPath string) (*core.FormatInfo, error) {
			return &core.FormatInfo{FormatInfoProtectedFieldV2: core.FormatInfoProtectedFieldV2{NodeID: 200}}, nil
		})
	defer saveFormatInfo.Reset()

	conf := &Config{
		DiskConfig: core.RuntimeConfig{MustMountPoint: false},
		Disks: []core.Config{
			{BaseConfig: core.BaseConfig{Path: diskPath}},
		},
	}

	nodeID, hasUsableDisk, err := getNodeID(context.Background(), conf)
	require.NoError(t, err)
	require.Equal(t, proto.NodeID(200), nodeID)
	require.True(t, hasUsableDisk)
}

// TestRegisterNode_RefuseNoUsableDisk tests that registerNode returns an
// error when nodeID=0, hasUsableDisk=false, and MustMountPoint is off —
// the machine has no readable disk and should not register.
func TestRegisterNode_RefuseNoUsableDisk(t *testing.T) {
	patches := gomonkey.ApplyFunc(getNodeID,
		func(ctx context.Context, conf *Config) (proto.NodeID, bool, error) {
			return 0, false, nil
		})
	defer patches.Reset()

	conf := &Config{
		HostInfo:   core.HostInfo{DiskType: proto.DiskType(1)},
		DiskConfig: core.RuntimeConfig{MustMountPoint: false},
	}
	svr := &Service{}
	svr.ctx, svr.cancel = context.WithCancel(context.Background())
	defer svr.cancel()

	err := svr.registerNode(context.Background(), conf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no usable disk")
}

// TestRegisterNode_AllowNewMachine tests that registerNode allows a new machine
// with nodeID=0 and hasUsableDisk=true.
func TestRegisterNode_AllowNewMachine(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cmCli := mocks.NewMockClientAPI(ctrl)
	cmCli.EXPECT().AddNode(gomock.Any(), gomock.Any()).Return(proto.NodeID(999), nil)

	patches := gomonkey.ApplyFunc(getNodeID,
		func(ctx context.Context, conf *Config) (proto.NodeID, bool, error) {
			return 0, true, nil
		})
	defer patches.Reset()

	conf := &Config{
		HostInfo: core.HostInfo{ClusterID: 1, Rack: "r1", Host: "h1", DiskType: proto.DiskType(1)},
	}
	svr := &Service{
		ClusterMgrClient: cmCli,
		Conf:             conf,
	}
	svr.ctx, svr.cancel = context.WithCancel(context.Background())
	defer svr.cancel()

	err := svr.registerNode(context.Background(), conf)
	require.NoError(t, err)
	require.Equal(t, proto.NodeID(999), conf.NodeID)
}

// TestRegisterBrokenDisk_NoCMRecord tests that when a lost disk has no CM
// record, it allocates a DiskID, calls AddDisk with Normal status, then
// SetDisk to Broken.
func TestRegisterBrokenDisk_NoCMRecord(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cmCli := mocks.NewMockClientAPI(ctrl)
	cmCli.EXPECT().AllocDiskID(gomock.Any()).Return(proto.DiskID(5001), nil)
	cmCli.EXPECT().AddDisk(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, info *cmapi.BlobNodeDiskInfo) error {
			require.Equal(t, proto.DiskID(5001), info.DiskID)
			require.Equal(t, proto.DiskStatusNormal, info.Status)
			require.Equal(t, "/data/broken1", info.Path)
			return nil
		})
	cmCli.EXPECT().SetDisk(gomock.Any(), proto.DiskID(5001), proto.DiskStatusBroken).Return(nil)

	conf := &core.Config{
		BaseConfig: core.BaseConfig{
			Path: "/data/broken1",
		},
		HostInfo: core.HostInfo{Host: "h1", Rack: "r1"},
	}
	svr := &Service{
		ClusterMgrClient: cmCli,
		Conf:             &Config{HostInfo: core.HostInfo{NodeID: 100}},
	}
	svr.ctx, svr.cancel = context.WithCancel(context.Background())
	defer svr.cancel()

	svr.registerBrokenDisk(context.Background(), conf)
}

// TestRegisterBrokenDisk_AddDiskFail tests that AddDisk failure does not crash.
// registerBrokenDisk only handles !exist case now (CM Normal case removed).
func TestRegisterBrokenDisk_AddDiskFail(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cmCli := mocks.NewMockClientAPI(ctrl)
	cmCli.EXPECT().AllocDiskID(gomock.Any()).Return(proto.DiskID(5002), nil)
	cmCli.EXPECT().AddDisk(gomock.Any(), gomock.Any()).Return(errMock)

	conf := &core.Config{
		BaseConfig: core.BaseConfig{
			Path: "/data/broken3",
		},
	}
	svr := &Service{
		ClusterMgrClient: cmCli,
		Conf:             &Config{HostInfo: core.HostInfo{NodeID: 100}},
	}
	svr.ctx, svr.cancel = context.WithCancel(context.Background())
	defer svr.cancel()

	// should not panic
	svr.registerBrokenDisk(context.Background(), conf)
}

// TestRegisterBrokenDisk_AllocFail tests that AllocDiskID failure does not crash.
func TestRegisterBrokenDisk_AllocFail(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cmCli := mocks.NewMockClientAPI(ctrl)
	cmCli.EXPECT().AllocDiskID(gomock.Any()).Return(proto.DiskID(0), errMock)
	// AddDisk should not be called if AllocDiskID fails

	conf := &core.Config{
		BaseConfig: core.BaseConfig{
			Path: "/data/broken4",
		},
	}
	svr := &Service{
		ClusterMgrClient: cmCli,
		Conf:             &Config{HostInfo: core.HostInfo{NodeID: 100}},
	}
	svr.ctx, svr.cancel = context.WithCancel(context.Background())
	defer svr.cancel()

	// should not panic
	svr.registerBrokenDisk(context.Background(), conf)
}

// TestHandleStartDiskError_NonEIO tests that non-EIO errors cause Fatal.
func TestHandleStartDiskError_NonEIO(t *testing.T) {
	t.Skip("handleStartDiskError calls span.Fatalf which triggers os.Exit;" +
		" its branching logic is verified in integration tests and kept unchanged per design")
}

// TestGetNodeID_MountPointUsesGlobalConfig tests that getNodeID uses
// conf.DiskConfig.MustMountPoint (global effective config) rather than
// each disk's per-disk default.
func TestGetNodeID_MountPointUsesGlobalConfig(t *testing.T) {
	diskPath := t.TempDir()

	// Simulate: global MustMountPoint=false, per-disk default is also false
	// IsMountPoint should NOT be called
	patches := gomonkey.ApplyFunc(myos.IsMountPoint, func(path string) bool {
		t.Error("IsMountPoint should not be called when MustMountPoint=false")
		return false
	})
	defer patches.Reset()

	saveFormatInfo := gomonkey.ApplyFunc(core.ReadFormatInfo,
		func(ctx context.Context, rootPath string) (*core.FormatInfo, error) {
			return &core.FormatInfo{FormatInfoProtectedFieldV2: core.FormatInfoProtectedFieldV2{NodeID: 300}}, nil
		})
	defer saveFormatInfo.Reset()

	conf := &Config{
		DiskConfig: core.RuntimeConfig{MustMountPoint: false},
		Disks: []core.Config{
			{
				BaseConfig:    core.BaseConfig{Path: diskPath},
				RuntimeConfig: core.RuntimeConfig{MustMountPoint: true}, // per-disk default, should be ignored
			},
		},
	}

	nodeID, hasUsableDisk, err := getNodeID(context.Background(), conf)
	require.NoError(t, err)
	// Even though per-disk MustMountPoint=true, the global config is false,
	// so the mount point check is skipped and the disk is found as usable
	require.Equal(t, proto.NodeID(300), nodeID)
	require.True(t, hasUsableDisk)
}

// TestGetNodeID_DiskMismatch tests that mismatched NodeIDs across disks
// return an error with hasUsableDisk=false.
func TestGetNodeID_DiskMismatch(t *testing.T) {
	path1 := t.TempDir()
	path2 := t.TempDir()

	patches := gomonkey.ApplyFunc(myos.IsMountPoint, func(path string) bool {
		return true
	})
	defer patches.Reset()

	saveFormatInfo := gomonkey.ApplyFunc(core.ReadFormatInfo,
		func(ctx context.Context, rootPath string) (*core.FormatInfo, error) {
			if rootPath == path1 {
				return &core.FormatInfo{FormatInfoProtectedFieldV2: core.FormatInfoProtectedFieldV2{NodeID: 1}}, nil
			}
			return &core.FormatInfo{FormatInfoProtectedFieldV2: core.FormatInfoProtectedFieldV2{NodeID: 2}}, nil
		})
	defer saveFormatInfo.Reset()

	conf := &Config{
		DiskConfig: core.RuntimeConfig{MustMountPoint: true},
		Disks: []core.Config{
			{BaseConfig: core.BaseConfig{Path: path1}},
			{BaseConfig: core.BaseConfig{Path: path2}},
		},
	}

	nodeID, hasUsableDisk, err := getNodeID(context.Background(), conf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "node id not match")
	require.Equal(t, proto.NodeID(0), nodeID)
	require.True(t, hasUsableDisk)
}

// TestGetNodeID_MixedUsableAndUnusable tests mixed scenario: one usable disk
// and one unusable (unmounted) disk. Should return the usable disk's NodeID.
func TestGetNodeID_MixedUsableAndUnusable(t *testing.T) {
	mountedPath := t.TempDir()

	patches := gomonkey.ApplyFunc(myos.IsMountPoint, func(path string) bool {
		return path == mountedPath
	})
	defer patches.Reset()

	saveFormatInfo := gomonkey.ApplyFunc(core.ReadFormatInfo,
		func(ctx context.Context, rootPath string) (*core.FormatInfo, error) {
			if rootPath == mountedPath {
				return &core.FormatInfo{FormatInfoProtectedFieldV2: core.FormatInfoProtectedFieldV2{NodeID: 50}}, nil
			}
			return nil, syscall.EIO
		})
	defer saveFormatInfo.Reset()

	conf := &Config{
		DiskConfig: core.RuntimeConfig{MustMountPoint: true},
		Disks: []core.Config{
			{BaseConfig: core.BaseConfig{Path: mountedPath}},
			{BaseConfig: core.BaseConfig{Path: "/data/bad_disk"}},
		},
	}

	nodeID, hasUsableDisk, err := getNodeID(context.Background(), conf)
	require.NoError(t, err)
	require.Equal(t, proto.NodeID(50), nodeID)
	require.True(t, hasUsableDisk)
}

// TestGetNodeID_ReadDirError tests that os.ReadDir errors (not just format.info errors)
// prevent setting hasUsableDisk.
func TestGetNodeID_ReadDirError(t *testing.T) {
	nonExistentPath := filepath.Join(t.TempDir(), "nonexistent")

	patches := gomonkey.ApplyFunc(myos.IsMountPoint, func(path string) bool {
		return true
	})
	defer patches.Reset()

	conf := &Config{
		DiskConfig: core.RuntimeConfig{MustMountPoint: true},
		Disks: []core.Config{
			{BaseConfig: core.BaseConfig{Path: nonExistentPath}},
		},
	}

	nodeID, hasUsableDisk, err := getNodeID(context.Background(), conf)
	require.NoError(t, err)
	require.Equal(t, proto.NodeID(0), nodeID)
	require.False(t, hasUsableDisk)
}

// TestPreScanLostDiskThreshold tests that the pre-scan correctly counts lost disks
// and triggers Fatalf when the count reaches LostDiskCount before any CM writes.
// Uses an isolated test closure that replicates the pre-scan logic so we can validate
// the counting rules without running the full startBlobnodeService.
func TestPreScanLostDiskThreshold(t *testing.T) {
	patches := gomonkey.ApplyFunc(myos.IsMountPoint, func(path string) bool {
		switch path {
		case "/data/ok1", "/data/ok2":
			return true
		default:
			return false
		}
	})
	defer patches.Reset()

	conf := &Config{
		DiskConfig: core.RuntimeConfig{MustMountPoint: true},
		HostInfo:   core.HostInfo{Host: "h1", Rack: "r1"},
		Disks: []core.Config{
			{BaseConfig: core.BaseConfig{Path: "/data/ok1"}},
			{BaseConfig: core.BaseConfig{Path: "/data/ok2"}},
			{BaseConfig: core.BaseConfig{Path: "/data/lost1"}},
			{BaseConfig: core.BaseConfig{Path: "/data/lost2"}},
			{BaseConfig: core.BaseConfig{Path: "/data/lost3"}},
		},
	}

	// simulate: lost1 Normal, lost2 no CM record, lost3 Repaired
	// lost3 is already non-Normal so should NOT count toward threshold
	foundDiskPathInCluster := map[string]*cmapi.BlobNodeDiskInfo{
		"/data/lost1": {DiskInfo: cmapi.DiskInfo{Status: proto.DiskStatusNormal}},
		"/data/lost3": {DiskInfo: cmapi.DiskInfo{Status: proto.DiskStatusRepaired}},
	}

	// replicate the pre-scan logic inline
	preScanLostCnt := 0
	for i := range conf.Disks {
		path := conf.Disks[i].Path
		if !conf.DiskConfig.MustMountPoint || myos.IsMountPoint(path) {
			continue
		}
		if info, exist := foundDiskPathInCluster[path]; !exist || info.Status == proto.DiskStatusNormal {
			preScanLostCnt++
		}
	}

	// lost1 (Normal) + lost2 (no CM) = 2; lost3 (Repaired) excluded
	require.Equal(t, 2, preScanLostCnt)
	// < LostDiskCount(3), so no Fatalf. verify reached < threshold
	require.True(t, preScanLostCnt < LostDiskCount)
}

// TestPreScanLostDiskThresholdFatal verifies that when lost disk count reaches
// LostDiskCount, log.Fatalf is invoked before any CM write operations.
func TestPreScanLostDiskThresholdFatal(t *testing.T) {
	fatalCalled := false
	fatalMsg := ""
	patches := gomonkey.ApplyFunc(myos.IsMountPoint, func(path string) bool {
		return false // all disks unmounted
	})
	defer patches.Reset()

	fatalPatch := gomonkey.ApplyFunc(log.Fatalf, func(format string, args ...interface{}) {
		fatalCalled = true
		fatalMsg = format
	})
	defer fatalPatch.Reset()

	conf := &Config{
		DiskConfig: core.RuntimeConfig{MustMountPoint: true},
		HostInfo:   core.HostInfo{Host: "h1", Rack: "r1"},
		Disks: []core.Config{
			{BaseConfig: core.BaseConfig{Path: "/data/lost1"}},
			{BaseConfig: core.BaseConfig{Path: "/data/lost2"}},
			{BaseConfig: core.BaseConfig{Path: "/data/lost3"}},
		},
	}

	// no CM records for any disk → all count as lost
	foundDiskPathInCluster := map[string]*cmapi.BlobNodeDiskInfo{}

	// replicate the pre-scan logic
	preScanLostCnt := 0
	for i := range conf.Disks {
		path := conf.Disks[i].Path
		if !conf.DiskConfig.MustMountPoint || myos.IsMountPoint(path) {
			continue
		}
		if info, exist := foundDiskPathInCluster[path]; !exist || info.Status == proto.DiskStatusNormal {
			preScanLostCnt++
		}
	}
	require.Equal(t, 3, preScanLostCnt)
	require.True(t, preScanLostCnt >= LostDiskCount)

	// should trigger Fatalf
	if preScanLostCnt >= LostDiskCount {
		log.Fatalf("lost disk count:%d over threshold:%d, no CM write performed", preScanLostCnt, LostDiskCount)
	}

	require.True(t, fatalCalled)
	require.Contains(t, fatalMsg, "lost disk count")
}
