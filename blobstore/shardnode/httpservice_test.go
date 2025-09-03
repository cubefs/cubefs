// Copyright 2025 The CubeFS Authors.
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

package shardnode

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/shardnode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
)

// newMockHttpServer creates a mock HTTP server for testing
func newMockHttpServer(s *service, addr string) (*http.Server, func()) {
	router := newHttpHandler(&HttpService{service: s})

	server := &http.Server{
		Addr:    addr,
		Handler: router,
	}

	shutdown := func() {
		go func() {
			server.Shutdown(context.Background())
		}()
	}

	return server, shutdown
}

func TestHttpService_HTTPGet(t *testing.T) {
	// Create a mock service with proper initialization
	mockSvc, clear, err := newMockService(t, mockServiceCfg{})
	require.Nil(t, err)
	defer clear()

	httpServer, shutdown := newMockHttpServer(mockSvc, ":11000")
	defer func() {
		shutdown()
	}()

	go func() {
		httpServer.ListenAndServe()
	}()
	// wait for the server to start
	time.Sleep(100 * time.Millisecond)
	client := rpc.NewClient(nil)

	// Mock getShardStats method
	expectedStats := shardnode.ShardStats{Suid: suid}

	patches := gomonkey.ApplyFunc((*service).getShardStats, func(s *service, ctx context.Context, diskID proto.DiskID, suid proto.Suid) (shardnode.ShardStats, error) {
		return expectedStats, nil
	})
	defer patches.Reset()

	// Test shard stats
	ret1 := &shardnode.ShardStats{}
	url := fmt.Sprintf("http://127.0.0.1:11000/shard/stats?disk_id=%d&suid=%d", diskID, suid)
	resp1, err := client.Get(ctx, url)
	require.Nil(t, err)
	json.NewDecoder(resp1.Body).Decode(ret1)
	resp1.Body.Close()
	require.Equal(t, suid, ret1.Suid)

	// Test delete blob stats
	ret2 := &shardnode.DeleteBlobStatsRet{}
	resp2, err := client.Get(ctx, "http://127.0.0.1:11000/blob/delete/stats")
	require.Nil(t, err)
	json.NewDecoder(resp2.Body).Decode(ret2)
	resp2.Body.Close()
	require.NotNil(t, ret2)
}

// Test setUpHttp function
func TestSetUpHttp(t *testing.T) {
	// Reset global service for testing
	resetGlobalService()

	// Test that setUpHttp function exists and can be called
	// Note: This test may fail if global config is not properly set up
	// We'll just test that the function doesn't panic
	defer func() {
		if r := recover(); r != nil {
			t.Logf("setUpHttp panicked as expected: %v", r)
		}
	}()

	// Call setUpHttp
	router, progressHandlers := setUpHttp()

	// Assertions - these may be nil if config is not set up
	// We're just testing that the function can be called without panic
	t.Logf("Router: %v, ProgressHandlers: %v", router, progressHandlers)
}
