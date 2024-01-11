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

package iputil_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/cubefs/cubefs/util/iputil"
	"github.com/stretchr/testify/require"
)

func TestGetRealIp(t *testing.T) {
	request, err := http.NewRequest("GET", "/", nil)
	if err != nil {
		t.Errorf("failed new http request")
		return
	}
	// cubefs.io
	ip := "13.250.168.211"
	request.RemoteAddr = ip
	require.Equal(t, iputil.RealIP(request), ip)
	request.RemoteAddr = "192.168.0.1"
	request.Header.Add("X-Forwarded-For", ip)
	require.Equal(t, iputil.RealIP(request), ip)
	for k := range request.Header {
		delete(request.Header, k)
	}
	request.Header.Add("X-Real-Ip", ip)
	require.Equal(t, iputil.RealIP(request), ip)
}

func TestPing(t *testing.T) {
	_, err := iputil.PingWithTimeout("not-domain-x/", 10, time.Second)
	require.Error(t, err)
	rtt, err := iputil.PingWithTimeout("127.0.0.1", 10, time.Second)
	require.NoError(t, err)
	t.Log(rtt)
}
