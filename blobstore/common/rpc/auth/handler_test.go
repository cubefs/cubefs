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
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/rpc/auth/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc2"
)

var (
	testServer  *httptest.Server
	testSecret  = "testSecret"
	authHandler interface {
		rpc2.Interceptor
		rpc.ProgressHandler
	}
)

func init() {
	authHandler = New(&proto.Config{EnableAuth: true, Secret: testSecret})
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		authHandler.Handler(w, r, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(200)
		})
	})
	testServer = httptest.NewServer(http.DefaultServeMux)
}

func TestHandlerRpc(t *testing.T) {
	require.Panics(t, func() { New(&proto.Config{EnableAuth: false, Secret: testSecret}) })
	require.Panics(t, func() { New(&proto.Config{EnableAuth: true, Secret: ""}) })

	client := http.Client{}
	req, _ := http.NewRequest("POST", testServer.URL+"/", nil)
	token := proto.Encode(time.Now().Unix(), proto.ParamFromRequest(req), []byte("wrongSecret"))
	req.Header.Set(proto.TokenHeaderKey, token)
	response, _ := client.Do(req)
	require.Equal(t, http.StatusForbidden, response.StatusCode)
	response.Body.Close()

	req, _ = http.NewRequest("POST", testServer.URL+"/", nil)
	token = proto.Encode(time.Now().Unix(), proto.ParamFromRequest(req), []byte(testSecret))
	req.Header.Set(proto.TokenHeaderKey, token)
	response, _ = client.Do(req)
	require.Equal(t, http.StatusOK, response.StatusCode)
	response.Body.Close()

	testServer.Close()
}

func TestHandlerRpc2(t *testing.T) {
	req, _ := rpc2.NewRequest(context.Background(), "/", "/path", nil, nil)
	token := proto.Encode(time.Now().Unix(), []byte(req.RemotePath), []byte("wrongSecret"))
	req.Header.Set(proto.TokenHeaderKey, token)
	err := authHandler.Handle(nil, req, func(rpc2.ResponseWriter, *rpc2.Request) error { return nil })
	require.Equal(t, http.StatusForbidden, rpc.DetectStatusCode(err))

	token = proto.Encode(time.Now().Unix(), []byte(req.RemotePath), []byte(testSecret))
	req.Header.Set(proto.TokenHeaderKey, token)
	err = authHandler.Handle(nil, req, func(rpc2.ResponseWriter, *rpc2.Request) error { return nil })
	require.Equal(t, http.StatusOK, rpc.DetectStatusCode(err))
}
