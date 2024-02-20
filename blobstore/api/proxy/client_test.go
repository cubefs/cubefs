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

package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	_ "github.com/cubefs/cubefs/blobstore/testing/nolog"
)

func TestClient_GetCacheVolume(t *testing.T) {
	cli := New(&Config{})
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"vid": 111, "units": [{"vuid": 425335980033}]}`))
	}))
	defer mockServer.Close()
	for _, args := range []clustermgr.CacheVolumeArgs{
		{Vid: 1, Version: 0, Flush: false},
		{Vid: 1, Version: 0xb06bccdb, Flush: false},
		{Vid: 2, Version: 0, Flush: true},
		{Vid: 2, Version: 0xb06bccdb, Flush: true},
	} {
		volume, err := cli.GetCacheVolume(context.Background(), mockServer.URL, &args)
		require.NoError(t, err)
		require.Equal(t, proto.Vid(111), volume.Vid)
		require.Equal(t, uint32(0), volume.Version)
		require.Equal(t, uint32(0xb06bccdb), volume.GetVersion())
	}
}

func TestClient_GetCacheDisk(t *testing.T) {
	cli := New(&Config{})
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"disk_id": 9876}`))
	}))
	defer mockServer.Close()
	for _, args := range []clustermgr.CacheDiskArgs{
		{DiskID: 1, Flush: false},
		{DiskID: 2, Flush: true},
	} {
		disk, err := cli.GetCacheDisk(context.Background(), mockServer.URL, &args)
		require.NoError(t, err)
		require.Equal(t, proto.DiskID(9876), disk.DiskID)
	}
}

func TestClient_GetCacheErase(t *testing.T) {
	cli := New(&Config{})
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer mockServer.Close()
	url := mockServer.URL
	require.Error(t, cli.Erase(context.Background(), url+"x", "volume-111"))
	require.NoError(t, cli.Erase(context.Background(), url, "disk-111"))
	require.NoError(t, cli.Erase(context.Background(), url, "ALL"))
}

func TestCacher_DiskvPathTransform(t *testing.T) {
	for _, cs := range []struct {
		key   string
		paths []string
	}{
		{"", []string{}},
		{"akey", []string{}},
		{"-id", []string{"8b", "d5"}},
		{"volume-", []string{"fc", "08"}},
		{"volume-111", []string{"59", "90"}},
		{"volume-111-", []string{"cb", "dc"}},
		{"volume-111-10", []string{"cf", "77"}},
		{"disk-111", []string{"a6", "51"}},
		{"disk-111-10", []string{"17", "3a"}},
	} {
		require.Equal(t, cs.paths, DiskvPathTransform(cs.key))
	}
}
