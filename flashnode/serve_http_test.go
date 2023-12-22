// Copyright 2023 The CubeFS Authors.
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

package flashnode

import (
	"testing"

	"github.com/cubefs/cubefs/flashnode/cachengine"
	"github.com/cubefs/cubefs/sdk/httpclient"
	"github.com/stretchr/testify/require"
)

var httpCli = httpclient.New()

func testHTTP(t *testing.T) {
	t.Run("Stat", testHTTPStat)
	t.Run("EvictVol", testHTTPEvictVol)
	t.Run("EvictAll", testHTTPEvictAll)
}

func testHTTPStat(t *testing.T) {
	st, err := httpCli.WithAddr(httpServer.Addr).FlashNode().Stat()
	require.NoError(t, err)
	t.Logf("node  status %+v", st)
	t.Logf("cache status %+v", *st.CacheStatus)
}

func testHTTPEvictVol(t *testing.T) {
	require.Error(t, httpCli.WithAddr(httpServer.Addr).FlashNode().EvictVol(""))
	st, err := httpCli.WithAddr(httpServer.Addr).FlashNode().Stat()
	require.NoError(t, err)
	require.Equal(t, 1, len(st.CacheStatus.Keys))
	require.Equal(t, cachengine.GenCacheBlockKey(_volume, _inode, _offset, _version), st.CacheStatus.Keys[0])
	t.Logf("cache status before evicted, %+v", *st.CacheStatus)

	require.NoError(t, httpCli.WithAddr(httpServer.Addr).FlashNode().EvictVol(_volume))
	st, err = httpCli.WithAddr(httpServer.Addr).FlashNode().Stat()
	require.NoError(t, err)
	require.Equal(t, 0, len(st.CacheStatus.Keys))
	t.Logf("cache status after  evicted, %+v", *st.CacheStatus)
}

func testHTTPEvictAll(t *testing.T) {
	require.NoError(t, httpCli.WithAddr(httpServer.Addr).FlashNode().EvictAll())
}
