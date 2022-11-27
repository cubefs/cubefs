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
	"github.com/cubefs/cubefs/blobstore/blobnode/core/disk"
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
	require.Equal(t, 606, resp.StatusCode)
	span.Infof("=== resp:%v, err:%v ===", resp, err)

	// err: path not exist
	_ = os.RemoveAll(disk1Path)
	req, err = http.NewRequest(http.MethodPost, host+"/disk/probe", bytes.NewReader(b))
	require.NoError(t, err)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, 605, resp.StatusCode)
	span.Infof("=== resp:%v, err:%v ===", resp, err)

	// err: online disk
	err = os.MkdirAll(disk1Path, 0o755)
	require.NoError(t, err)
	req, err = http.NewRequest(http.MethodPost, host+"/disk/probe", bytes.NewReader(b))
	require.NoError(t, err)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, 607, resp.StatusCode)
	span.Infof("=== resp:%v, err:%v ===", resp, err)

	// gc disk storage
	done := make(chan struct{})
	service.lock.Lock()
	ds := service.Disks[disk1ID]
	ds.(*disk.DiskStorageWrapper).OnClosed = func() {
		close(done)
	}
	delete(service.Disks, disk1ID)
	service.lock.Unlock()

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

	// empty and non disk storage . will success
	req, err = http.NewRequest(http.MethodPost, host+"/disk/probe", bytes.NewReader(b))
	require.NoError(t, err)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	span.Infof("=== resp:%v, err:%v ===", resp, err)
	require.Equal(t, 200, resp.StatusCode)
}
