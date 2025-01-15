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
	st, err := httpCli.Addr(httpServer.Addr).FlashNode().Stat()
	require.NoError(t, err)
	t.Logf("node  status %+v", st)
	for _, s := range st.CacheStatus {
		t.Logf("cache status %+v", *s)
	}
}

func testHTTPEvictVol(t *testing.T) {
	require.Error(t, httpCli.Addr(httpServer.Addr).FlashNode().EvictVol(""))
	st, err := httpCli.Addr(httpServer.Addr).FlashNode().Stat()
	require.NoError(t, err)
	index := -1
	keyNum := 0
	for i, s := range st.CacheStatus {
		if len(s.Keys) != 0 {
			keyNum += len(s.Keys)
			index = i
		}
	}
	require.Equal(t, 1, keyNum)
	require.NotEqual(t, -1, index)
	require.Equal(t, cachengine.GenCacheBlockKey(_volume, _inode, _offset, _version), st.CacheStatus[index].Keys[0])
	for _, s := range st.CacheStatus {
		t.Logf("cache status before evicted, %+v", *s)
	}

	require.NoError(t, httpCli.Addr(httpServer.Addr).FlashNode().EvictVol(_volume))
	st, err = httpCli.Addr(httpServer.Addr).FlashNode().Stat()
	require.NoError(t, err)
	index = -1
	keyNum = 0
	for i, s := range st.CacheStatus {
		if len(s.Keys) != 0 {
			keyNum += len(s.Keys)
			index = i
		}
	}
	require.Equal(t, 0, keyNum)
	require.Equal(t, -1, index)
	for _, s := range st.CacheStatus {
		t.Logf("cache status after evicted, %+v", *s)
	}
}

func testHTTPEvictAll(t *testing.T) {
	require.NoError(t, httpCli.Addr(httpServer.Addr).FlashNode().EvictAll())
}
