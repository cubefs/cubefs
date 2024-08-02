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

package allocator

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/shardnode/base"
)

type mockBidMgr struct{}

func newMockBidTp(tb testing.TB) base.Transport {
	tp := base.NewMockTransport(gomock.NewController(tb))
	tp.EXPECT().AllocBid(gomock.Any(), gomock.Any()).Return(proto.BlobID(10001), nil).AnyTimes()
	return tp
}

func (mockBidMgr) BidAlloc(c *rpc.Context) {
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

func runBidServer(svr mockBidMgr) (string, func()) {
	r := rpc.New()
	r.Handle(http.MethodPost, "/bid/alloc", svr.BidAlloc)
	testServer := httptest.NewServer(r)
	return testServer.URL, func() { testServer.Close() }
}

func TestGetBidScopes(t *testing.T) {
	_, clean := runBidServer(mockBidMgr{})
	defer clean()

	tp := newMockBidTp(t)

	ctx := context.Background()
	bid, err := newBidMgr(ctx, BlobConfig{BidAllocNums: 10000}, tp)
	require.NoError(t, err)
	{
		objBidScopes, err := bid.alloc(ctx, 3)
		require.NoError(t, err)
		require.Equal(t, 10001, int(objBidScopes[0].startBid))
		require.Equal(t, 10003, int(objBidScopes[0].endBid))

		time.Sleep(100 * time.Millisecond)
		objBidScopes11, err := bid.alloc(ctx, 3)
		require.NoError(t, err)
		require.Equal(t, 10004, int(objBidScopes11[0].startBid))
		require.Equal(t, 10006, int(objBidScopes11[0].endBid))

		time.Sleep(100 * time.Millisecond)
		objBidScopes2, err := bid.alloc(ctx, 9994)
		require.NoError(t, err)
		require.Equal(t, 10007, int(objBidScopes2[0].startBid))
		require.Equal(t, 20000, int(objBidScopes2[0].endBid))

		objBidScopes3, err := bid.alloc(ctx, 6)
		require.NoError(t, err)
		require.Equal(t, 10001, int(objBidScopes3[0].startBid))
		require.Equal(t, 10006, int(objBidScopes3[0].endBid))

		time.Sleep(100 * time.Millisecond)
		objBidScopes4, err := bid.alloc(ctx, 9998)
		require.NoError(t, err)
		require.Equal(t, 10007, int(objBidScopes4[0].startBid))
		require.Equal(t, 20000, int(objBidScopes4[0].endBid))
		require.Equal(t, 10001, int(objBidScopes4[1].startBid))
		require.Equal(t, 10004, int(objBidScopes4[1].endBid))
	}
}

func BenchmarkAllocBid(b *testing.B) {
	_, clean := runBidServer(mockBidMgr{})
	defer clean()

	tp := newMockBidTp(b)

	ctx := context.Background()
	bid, err := newBidMgr(ctx, BlobConfig{BidAllocNums: 10000}, tp)
	require.NoError(b, err)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bid.alloc(ctx, 2)
	}
}
