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

package auth

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/common/rpc/auth/proto"
)

var (
	testServer *httptest.Server
	testSecret = "testSecret"
)

func init() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		token := r.Header.Get(proto.TokenHeaderKey)
		if token == "" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if err := proto.Decode(token, proto.ParamFromRequest(r), []byte(testSecret)); err != nil {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		w.WriteHeader(200)
	})
	testServer = httptest.NewServer(http.DefaultServeMux)
}

func TestTransport(t *testing.T) {
	// invalid secret
	tc := New(&http.Transport{}, &proto.Config{EnableAuth: true, Secret: "wrongSecret"})
	client := http.Client{Transport: tc}
	req, _ := http.NewRequest("POST", testServer.URL+"/", nil)
	response, _ := client.Do(req)
	require.Equal(t, http.StatusForbidden, response.StatusCode)
	response.Body.Close()

	// valid secret
	tc = New(&http.Transport{}, &proto.Config{EnableAuth: true, Secret: testSecret})
	client = http.Client{Transport: tc}
	req, _ = http.NewRequest("POST", testServer.URL+"/", nil)
	response, _ = client.Do(req)
	require.Equal(t, http.StatusOK, response.StatusCode)
	response.Body.Close()

	// test empty token
	tc = New(&http.Transport{}, &proto.Config{EnableAuth: true, Secret: ""})
	client = http.Client{Transport: tc}
	req, _ = http.NewRequest("POST", testServer.URL+"/", nil)
	response, _ = client.Do(req)
	require.Equal(t, http.StatusForbidden, response.StatusCode)
	response.Body.Close()

	// test token failed
	req, _ = http.NewRequest("POST", testServer.URL+"/", nil)
	req.Header.Set(proto.TokenHeaderKey, "#$@%DF#$@#$")
	response, _ = client.Do(req)
	require.Equal(t, http.StatusForbidden, response.StatusCode)
	response.Body.Close()

	testServer.Close()
}
