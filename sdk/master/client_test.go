// Copyright 2026 The CubeFS Authors.
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

package master

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRequestParamReachesMasterIntact sends a request through the client and
// reads the parameter back the way the master does (ParseForm / FormValue).
func TestRequestParamReachesMasterIntact(t *testing.T) {
	var received atomic.Value
	received.Store("")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got := ""
		if err := r.ParseForm(); err == nil {
			got = r.FormValue("description")
		}
		received.Store(got)
		_, _ = w.Write([]byte(`{"code":0,"msg":"success","data":null}`))
	}))
	defer srv.Close()
	c := NewMasterClient([]string{srv.Listener.Addr().String()}, false)

	descriptions := []string{
		"plain",
		"my volume",
		"R&D",
		"C++",
		"project#1",
		"50%",
		"50%25",
		"a=b",
		"zone1,zone2",
		"data/logs?x=1",
	}
	for _, description := range descriptions {
		t.Run(description, func(t *testing.T) {
			req := newRequest(get, "/vol/update")
			req.addParam("name", "vol1")
			req.addParam("description", description)
			require.NoError(t, c.request(req))
			require.Equal(t, description, received.Load())
		})
	}
}
