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

package allocator

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"

	"github.com/stretchr/testify/require"
)

type mockBidMgr struct{}

func (m *mockBidMgr) BidAlloc(c *rpc.Context) {
	args := new(clustermgr.BidScopeArgs)
	if err := c.ArgsBody(args); err != nil {
		c.RespondError(err)
		return
	}
	c.RespondJSON(clustermgr.BidScopeRet{
		StartBid: proto.BlobID(1 + args.Count),
		EndBid:   proto.BlobID(2 * args.Count),
	})
}

func runBidServer(svr *mockBidMgr) string {
	r := rpc.New()
	r.Handle(http.MethodPost, "/bid/alloc", svr.BidAlloc)
	testServer := httptest.NewServer(r)
	return testServer.URL
}

func TestGetBidScopes(t *testing.T) {
	mc := &mockBidMgr{}
	clusterHost := runBidServer(mc)
	mockCli := clustermgr.New(&clustermgr.Config{LbConfig: rpc.LbConfig{Hosts: []string{clusterHost}}})

	ctx := context.Background()
	bid, _ := NewBidMgr(ctx, BlobConfig{BidAllocNums: 10000}, mockCli)
	{
		objBidScopes, err := bid.Alloc(ctx, 3)
		if err != nil {
			t.Fatal(err)
		}

		require.Equal(t, 10001, int(objBidScopes[0].StartBid))
		require.Equal(t, 10003, int(objBidScopes[0].EndBid))

		time.Sleep(100 * time.Millisecond)
		objBidScopes11, err := bid.Alloc(ctx, 3)
		if err != nil {
			t.Fatal(err)
		}

		require.Equal(t, 10004, int(objBidScopes11[0].StartBid))
		require.Equal(t, 10006, int(objBidScopes11[0].EndBid))

		time.Sleep(100 * time.Millisecond)
		objBidScopes2, err := bid.Alloc(ctx, 9994)
		if err != nil {
			t.Fatal(err)
		}
		require.Equal(t, 10007, int(objBidScopes2[0].StartBid))
		require.Equal(t, 20000, int(objBidScopes2[0].EndBid))

		objBidScopes3, err := bid.Alloc(ctx, 6)
		if err != nil {
			t.Fatal(err)
		}

		require.Equal(t, 10001, int(objBidScopes3[0].StartBid))
		require.Equal(t, 10006, int(objBidScopes3[0].EndBid))

		time.Sleep(100 * time.Millisecond)
		objBidScopes4, err := bid.Alloc(ctx, 9998)
		if err != nil {
			t.Fatal(err)
		}
		require.Equal(t, 10007, int(objBidScopes4[0].StartBid))
		require.Equal(t, 20000, int(objBidScopes4[0].EndBid))

		require.Equal(t, 10001, int(objBidScopes4[1].StartBid))
		require.Equal(t, 10004, int(objBidScopes4[1].EndBid))
	}
}

func BenchmarkAllocBid(b *testing.B) {
	mc := &mockBidMgr{}
	clusterHost := runBidServer(mc)
	mockCli := clustermgr.New(&clustermgr.Config{LbConfig: rpc.LbConfig{Hosts: []string{clusterHost}}})

	ctx := context.Background()
	bid, _ := NewBidMgr(ctx, BlobConfig{BidAllocNums: 10000}, mockCli)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := bid.Alloc(ctx, 2)
		if err != nil {
			b.Fatal(err)
		}
	}
}
