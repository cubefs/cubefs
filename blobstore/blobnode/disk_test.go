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
	"net/http"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	bloberr "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
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
