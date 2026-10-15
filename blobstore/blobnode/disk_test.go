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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	cmapi "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/blobnode/base"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	bloberr "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
	"github.com/cubefs/cubefs/blobstore/util/limit/keycount"
)

func TestService_WsprpcDiskProbe(t *testing.T) {
	service, mockcm := newTestBlobNodeService(t, "DiskProbe")
	defer cleanTestBlobNodeService(service)

	span, _ := trace.StartSpanFromContextWithTraceID(context.Background(), "", "")

	host := runTestServer(service)

	testDisk := mockcm.disks[0]
	disk1Path := testDisk.path
	disk1ID := testDisk.diskId

	span.Infof("disk1Path:%v, ID:%v", disk1Path, disk1ID)

	cca := &bnapi.DiskProbeArgs{
		Path: disk1Path,
	}

	b, err := json.Marshal(cca)
	require.NoError(t, err)

	// err: Non-empty path
	req, err := http.NewRequest(http.MethodPost, host+"/disk/probe", bytes.NewReader(b))
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NotNil(t, resp)
	resp.Body.Close()
	require.Equal(t, bloberr.CodePathNotEmpty, resp.StatusCode)
	span.Infof("=== resp:%v, err:%v ===", resp, err)

	// err: path not exist
	_ = os.RemoveAll(disk1Path)
	req, err = http.NewRequest(http.MethodPost, host+"/disk/probe", bytes.NewReader(b))
	require.NoError(t, err)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, bloberr.CodePathNotExist, resp.StatusCode)
	span.Infof("=== resp:%v, err:%v ===", resp, err)

	// err: online disk
	// Use DiskStatusRepaired to allow disk probe after cluster ID check
	mockcm.disks[0].status = proto.DiskStatusRepaired
	err = os.MkdirAll(disk1Path, 0o755)
	require.NoError(t, err)
	req, err = http.NewRequest(http.MethodPost, host+"/disk/probe", bytes.NewReader(b))
	require.NoError(t, err)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, bloberr.CodePathFindOnline, resp.StatusCode)
	span.Infof("=== resp:%v, err:%v ===", resp, err)

	// gc disk storage
	done := make(chan struct{})
	service.lock.Lock()
	ds := service.Disks[disk1ID]
	ds.SetOnCloseFn(func() { close(done) })
	ds.PrepareClose(context.Background())
	delete(service.Disks, disk1ID)
	service.lock.Unlock()
	require.True(t, ds.IsClosing())

	for i := 0; i < 2; i++ {
		time.Sleep(time.Second * 1)
		runtime.GC()
	}

	select {
	case <-done:
		span.Infof("success gc")
	case <-time.After(10 * time.Second):
		t.Fail()
	}

	// empty and non disk storage . will success . Test disk register OK
	req, err = http.NewRequest(http.MethodPost, host+"/disk/probe", bytes.NewReader(b))
	require.NoError(t, err)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	span.Infof("=== resp:%v, err:%v ===", resp, err)
	require.Equal(t, 200, resp.StatusCode)
}

// TestService_DiskProbe_ClusterIDValidation tests the cluster ID validation logic
// This test verifies the new ClusterID check
func TestService_DiskProbe_ClusterIDAndDiskStatus(t *testing.T) {
	service, mockcm := newTestBlobNodeService(t, "DiskProbe_ClusterIDValidation")
	defer cleanTestBlobNodeService(service)

	span, _ := trace.StartSpanFromContext(context.Background(), "")
	host := runTestServer(service)

	disk1Path := mockcm.disks[0].path
	mockcm.disks[0].status = proto.DiskStatusRepaired // set repairing, check disk status

	// Test with correct cluster ID but disk not found in cm
	t.Run("CorrectClusterID_DiskNotFoundInCluster", func(t *testing.T) {
		// Use a path that doesn't exist in mock cluster
		testPath := disk1Path + "/test_disk_not_in_cluster_wrong_path"
		defer os.RemoveAll(testPath)

		// Create empty directory to pass the "path not empty" check
		err := os.MkdirAll(testPath, 0o755)
		require.NoError(t, err)

		cca := &bnapi.DiskProbeArgs{
			Path: testPath,
		}
		b, err := json.Marshal(cca)
		require.NoError(t, err)

		// Reset disk status for cleanup
		mockcm.disks[0].status = proto.DiskStatusRepaired
		req, err := http.NewRequest(http.MethodPost, host+"/disk/probe", bytes.NewReader(b))
		require.NoError(t, err)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		// Should fail with 605 (ErrPathNotExist) because oldDisk.DiskID == 0
		span.Infof("Disk not found in cluster test - Status Code: %d (expected: 605)", resp.StatusCode)
		require.Equal(t, bloberr.CodePathNotExist, resp.StatusCode,
			"Should reject disk not found in cluster with 605 (ErrPathNotExist)")
	})

	// Test with correct cluster ID but disk status is not Repaired
	t.Run("CorrectClusterID_DiskStatusNotRepaired", func(t *testing.T) {
		testPath := disk1Path
		defer os.RemoveAll(testPath)

		// Set disk status to Repairing (not Repaired) to trigger status check failure
		mockcm.disks[0].status = proto.DiskStatusRepairing

		// Create empty directory to pass the "path not empty" check
		err := os.RemoveAll(testPath)
		require.NoError(t, err)
		err = os.MkdirAll(testPath, 0o755)
		require.NoError(t, err)

		cca := &bnapi.DiskProbeArgs{
			Path: testPath,
		}
		b, err := json.Marshal(cca)
		require.NoError(t, err)

		req, err := http.NewRequest(http.MethodPost, host+"/disk/probe", bytes.NewReader(b))
		require.NoError(t, err)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		// Should fail on disk status check with 603 (ErrInternal)
		span.Infof("Disk status not repaired test - Status Code: %d (expected: 603)", resp.StatusCode)
		require.Equal(t, bloberr.CodeInternal, resp.StatusCode,
			"Should reject disk with non-Repaired status with 603 (ErrInternal)")
	})
}

func TestAllowBrokenDisk(t *testing.T) {
	ctx := context.Background()
	s, cm, diskID := newWritableBrokenDisk(t, 3)

	// two bad nodes, this disk's node is a third: still under the limit
	expectListDisk(cm, proto.DiskStatusBroken, nil, 1, 1)
	expectListDisk(cm, proto.DiskStatusRepairing, nil, 2)
	allow, err := s.allowBrokenDisk(ctx, diskID)
	require.NoError(t, err)
	require.True(t, allow)

	// three other nodes already fill the limit
	expectListDisk(cm, proto.DiskStatusBroken, nil, 1, 2, 3)
	expectListDisk(cm, proto.DiskStatusRepairing, nil)
	allow, err = s.allowBrokenDisk(ctx, diskID)
	require.NoError(t, err)
	require.False(t, allow)

	// this disk's host is already counted, and the host count is still within the limit
	expectListDisk(cm, proto.DiskStatusBroken, nil, 4, 1)
	expectListDisk(cm, proto.DiskStatusRepairing, nil, 2)
	allow, err = s.allowBrokenDisk(ctx, diskID)
	require.NoError(t, err)
	require.True(t, allow)

	// manually broken hosts already exceed the limit, even on this disk's host
	expectListDisk(cm, proto.DiskStatusBroken, nil, 4, 1, 2, 3)
	expectListDisk(cm, proto.DiskStatusRepairing, nil)
	allow, err = s.allowBrokenDisk(ctx, diskID)
	require.NoError(t, err)
	require.False(t, allow)

	expectListDisk(cm, proto.DiskStatusBroken, errors.New("cm down"))
	allow, err = s.allowBrokenDisk(ctx, diskID)
	require.Error(t, err)
	require.False(t, allow)

	// missing disk and a non-writable disk do not query clustermgr
	allow, err = s.allowBrokenDisk(ctx, proto.DiskID(100))
	require.NoError(t, err)
	require.True(t, allow)

	ro := NewMockDiskAPI(gomock.NewController(t))
	ro.EXPECT().IsWritable().Return(false).AnyTimes()
	s.Disks[diskID] = ro
	allow, err = s.allowBrokenDisk(ctx, diskID)
	require.NoError(t, err)
	require.True(t, allow)
}

func TestHandleDataIOErrorPending(t *testing.T) {
	ctx := context.Background()
	s, cm, diskID, started := newMarkableBrokenDisk(t, 3)
	diskErr := &base.DataIOError{Err: syscall.EIO}

	expectListDisk(cm, proto.DiskStatusBroken, errors.New("cm down"))
	s.handleDataIOError(ctx, diskID, diskErr)
	require.True(t, s.isPendingBrokenDisk(diskID))

	// a later data EIO does not query clustermgr again
	s.handleDataIOError(ctx, diskID, diskErr)
	require.Len(t, s.pendingBrokenDisks, 1)

	// still at the limit: stay pending
	expectListDisk(cm, proto.DiskStatusBroken, nil, 1, 2, 3)
	expectListDisk(cm, proto.DiskStatusRepairing, nil)
	s.tryPendingBrokenDisks()
	require.True(t, s.isPendingBrokenDisk(diskID))

	// one bad node is gone: mark and drop the record
	expectListDisk(cm, proto.DiskStatusBroken, nil, 1, 2)
	expectListDisk(cm, proto.DiskStatusRepairing, nil)
	s.tryPendingBrokenDisks()
	require.False(t, s.isPendingBrokenDisk(diskID))
	waitDiskMark(t, s, started)
}

func TestHandleDiskIOErrorMetaMarksNow(t *testing.T) {
	ctx := context.Background()
	s, cm, diskID, started := newMarkableBrokenDisk(t, 3)

	expectListDisk(cm, proto.DiskStatusBroken, nil, 1, 2, 3)
	expectListDisk(cm, proto.DiskStatusRepairing, nil)
	s.handleDiskIOError(ctx, diskID, &base.DataIOError{Err: syscall.EIO})
	require.True(t, s.isPendingBrokenDisk(diskID))

	s.handleDiskIOError(ctx, diskID, syscall.EIO)
	require.False(t, s.isPendingBrokenDisk(diskID))
	waitDiskMark(t, s, started)
}

func TestListDisksByStatusPages(t *testing.T) {
	var markers []proto.DiskID
	disks, err := listDisksByStatus(context.Background(), func(_ context.Context, args *cmapi.ListOptionArgs) (cmapi.ListDiskRet, error) {
		markers = append(markers, args.Marker)
		if args.Marker == proto.InvalidDiskID {
			return cmapi.ListDiskRet{
				Disks:  []*cmapi.BlobNodeDiskInfo{{DiskInfo: cmapi.DiskInfo{NodeID: 1}}},
				Marker: 10,
			}, nil
		}
		require.Equal(t, proto.DiskID(10), args.Marker)
		return cmapi.ListDiskRet{
			Disks:  []*cmapi.BlobNodeDiskInfo{{DiskInfo: cmapi.DiskInfo{NodeID: 2}}},
			Marker: proto.InvalidDiskID,
		}, nil
	}, proto.DiskStatusBroken)
	require.NoError(t, err)
	require.Equal(t, []proto.DiskID{proto.InvalidDiskID, 10}, markers)
	require.Len(t, disks, 2)
}

func newWritableBrokenDisk(t *testing.T, limit int) (*Service, *mocks.MockClientAPI, proto.DiskID) {
	t.Helper()
	ctrl := gomock.NewController(t)
	diskID := proto.DiskID(9)
	ds := NewMockDiskAPI(ctrl)
	ds.EXPECT().IsWritable().Return(true).AnyTimes()
	ds.EXPECT().NodeID().Return(proto.NodeID(4)).AnyTimes()
	cm := mocks.NewMockClientAPI(ctrl)
	s := newPendingBrokenDisksService(map[proto.DiskID]core.DiskAPI{diskID: ds}, cm, limit)
	return s, cm, diskID
}

func newMarkableBrokenDisk(t *testing.T, limit int) (*Service, *mocks.MockClientAPI, proto.DiskID, <-chan struct{}) {
	t.Helper()
	ctrl := gomock.NewController(t)
	diskID := proto.DiskID(9)
	started := make(chan struct{})
	ds := NewMockDiskAPI(ctrl)
	ds.EXPECT().IsWritable().Return(true).AnyTimes()
	ds.EXPECT().NodeID().Return(proto.NodeID(4)).AnyTimes()
	ds.EXPECT().DiskInfo().Return(cmapi.BlobNodeDiskInfo{}).AnyTimes()
	ds.EXPECT().SetStatus(proto.DiskStatusBroken).Times(1)
	ds.EXPECT().ID().DoAndReturn(func() proto.DiskID {
		close(started)
		return diskID
	}).AnyTimes()
	cm := mocks.NewMockClientAPI(ctrl)
	cm.EXPECT().SetReadonlyDisk(gomock.Any(), diskID, true).Return(nil).AnyTimes()
	cm.EXPECT().SetDisk(gomock.Any(), diskID, proto.DiskStatusBroken).Return(nil)
	s := newPendingBrokenDisksService(map[proto.DiskID]core.DiskAPI{diskID: ds}, cm, limit)
	return s, cm, diskID, started
}

func waitDiskMark(t *testing.T, s *Service, started <-chan struct{}) {
	t.Helper()
	close(s.closeCh)
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("mark broken did not start")
	}
}

func expectListDisk(cm *mocks.MockClientAPI, status proto.DiskStatus, err error, nodes ...proto.NodeID) {
	disks := make([]*cmapi.BlobNodeDiskInfo, 0, len(nodes))
	for _, id := range nodes {
		disks = append(disks, &cmapi.BlobNodeDiskInfo{DiskInfo: cmapi.DiskInfo{NodeID: id}})
	}
	cm.EXPECT().ListDisk(gomock.Any(), &cmapi.ListOptionArgs{
		Status: status,
		Count:  200,
	}).Return(cmapi.ListDiskRet{Disks: disks, Marker: proto.InvalidDiskID}, err)
}

func newPendingBrokenDisksService(disks map[proto.DiskID]core.DiskAPI, cli cmapi.APIBlobnode, limit int) *Service {
	return &Service{
		Disks:            disks,
		ClusterMgrClient: cli,
		Conf: &Config{
			MaxClusterBadDiskNodes:     limit,
			DiskStatusCheckIntervalSec: 3600,
		},
		BrokenLimitPerDisk: keycount.New(1),
		pendingBrokenDisks: make(map[proto.DiskID]error),
		closeCh:            make(chan struct{}),
	}
}
