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

package rpc

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/common/crc32block"
	"github.com/cubefs/cubefs/blobstore/common/rpc/auth"
)

var (
	testServer *httptest.Server
	first      = true
)

type handler struct{}

func (s *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	uri := r.URL.RequestURI()
	switch uri {
	case "/retry":
		b, err := ioutil.ReadAll(r.Body)
		if err != nil || int64(len(b)) != r.ContentLength || first {
			ReplyErr(w, 500, "test retry")
			first = false
		}
	case "/crc":
		doAfterCrc(w, r)
	case "/crcbody":
		w.Header().Set(HeaderAckCrcEncoded, "1")
		w.WriteHeader(500)
	case "/json":
		callWithJSON(w, r)
	case "/timeout":
		timeout(w, r)
	case "/notfound":
		ReplyWith(w, 404, "", []byte("404 page not found"))
	default:
		{
			marshal, err := json.Marshal(ret{Name: "Test_GetWith"})
			if err == nil {
				w.Write(marshal)
				return
			}
		}
	}
}

type testTimeoutReader struct{}

func (t *testTimeoutReader) Read(p []byte) (n int, err error) {
	time.Sleep(time.Millisecond * 20)
	return 0, nil
}

func doAfterCrc(w http.ResponseWriter, req *http.Request) {
	dec := crc32block.NewBodyDecoder(req.Body)
	defer dec.Close()
	dataByte := make([]byte, 1024)
	read, err := dec.Read(dataByte)
	w.Header().Set(HeaderAckCrcEncoded, "1")
	if err != nil {
		return
	}
	w.Write(dataByte[:read])
}

func callWithJSON(w http.ResponseWriter, req *http.Request) {
	r := &ret{}
	err := json.NewDecoder(req.Body).Decode(r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	r.Name = r.Name + "+Test"
	marshal, err := json.Marshal(r)
	if err != nil {
		w.WriteHeader(http.StatusGatewayTimeout)
		return
	}
	w.Write(marshal)
}

func timeout(w http.ResponseWriter, req *http.Request) {
	r := &ret{}
	err := json.NewDecoder(req.Body).Decode(r)
	if err != nil {
		return
	}
	for i := 0; i < 10*1024*1024*1024; i++ {
		w.Write([]byte("time out"))
	}
}

var simpleCfg = &Config{
	ClientTimeoutMs: 10000,
	Tc: TransportConfig{
		DialTimeoutMs:           1000,
		ResponseHeaderTimeoutMs: 3000,
		MaxConnsPerHost:         100,
		MaxIdleConns:            1000,
		MaxIdleConnsPerHost:     10,
		IdleConnTimeoutMs:       60000,
		DisableCompression:      true,
		Auth: auth.Config{
			EnableAuth: true,
			Secret:     "test",
		},
	},
}
var simpleClient = NewClient(simpleCfg)

func init() {
	testServer = httptest.NewServer(&handler{})
}

func TestClient_NewClient(t *testing.T) {
	NewClient(nil)
	{
		cfg := Config{}
		NewClient(&cfg)
		require.Equal(t, 10, cfg.Tc.MaxConnsPerHost)
		require.Equal(t, int64(0), cfg.Tc.DialTimeoutMs)
		require.False(t, cfg.Tc.Auth.EnableAuth)
	}
	{
		cfg := Config{}
		cfg.Tc.Auth.EnableAuth = true
		cfg.Tc.Auth.Secret = "true"
		NewClient(&cfg)
		require.Equal(t, 10, cfg.Tc.MaxConnsPerHost)
		require.Equal(t, int64(0), cfg.Tc.DialTimeoutMs)
		require.True(t, cfg.Tc.Auth.EnableAuth)
		require.Equal(t, "true", cfg.Tc.Auth.Secret)
	}
	{
		cfg := Config{}
		cfg.Tc.MaxConnsPerHost = 4
		NewClient(&cfg)
		require.Equal(t, 4, cfg.Tc.MaxConnsPerHost)
		require.Equal(t, 0, cfg.Tc.MaxIdleConnsPerHost)
		require.Equal(t, int64(0), cfg.Tc.DialTimeoutMs)
		require.False(t, cfg.Tc.Auth.EnableAuth)
	}
}

func TestClient_GetWith(t *testing.T) {
	ctx := context.Background()
	result := &ret{}
	err := simpleClient.GetWith(ctx, testServer.URL, result)
	require.NoError(t, err)
	require.NotNil(t, result)
}

func TestClient_PostWithCrc(t *testing.T) {
	ctx := context.Background()
	result := &ret{}
	err := simpleClient.PostWith(ctx, testServer.URL+"/crc", result,
		&ret{Name: "TestClient_PostWithCrc"}, WithCrcEncode())
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "TestClient_PostWithCrc", result.Name)
}

func TestClient_PostWithNoCrc(t *testing.T) {
	ctx := context.Background()
	result := &ret{}
	err := simpleClient.PostWith(ctx, testServer.URL+"/json", result, &ret{Name: "TestClient_PostWithNoCrc"})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "TestClient_PostWithNoCrc+Test", result.Name)
}

func TestClient_PostWithNoneBody(t *testing.T) {
	ctx := context.Background()
	err := simpleClient.PostWith(ctx, testServer.URL+"/json", nil, NoneBody)
	require.Equal(t, DetectStatusCode(err), http.StatusBadRequest)
}

func TestClient_Delete(t *testing.T) {
	ctx := context.Background()
	resp, err := simpleClient.Delete(ctx, testServer.URL)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestClient_PutWithNoCrc(t *testing.T) {
	ctx := context.Background()
	result := &ret{}
	err := simpleClient.PutWith(ctx, testServer.URL+"/json", result, &ret{Name: "TestClient_PutWithNoCrc"})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "TestClient_PutWithNoCrc+Test", result.Name)
}

func TestClient_PutWithCrc(t *testing.T) {
	ctx := context.Background()
	result := &ret{}
	err := simpleClient.PostWith(ctx, testServer.URL+"/crc", result,
		&ret{Name: "TestClient_PutWithCrc"}, WithCrcEncode())
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "TestClient_PutWithCrc", result.Name)
}

func TestClient_Post(t *testing.T) {
	ctx := context.Background()
	result := &ret{}
	resp, err := simpleClient.Post(ctx, testServer.URL+"/json", &ret{Name: "TestClient_Post"})
	require.NoError(t, err)
	err = ParseData(resp, result)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "TestClient_Post+Test", result.Name)
}

func TestClient_PostWithReadResponseTimeout(t *testing.T) {
	simpleCfg.BodyBaseTimeoutMs = 50
	simpleCfg.BodyBandwidthMBPs = 1
	simpleCli := NewClient(simpleCfg)
	ctx := context.Background()
	resp, err := simpleCli.Post(ctx, testServer.URL+"/timeout", &ret{Name: "TestClient_Post"})
	require.NoError(t, err)
	defer resp.Body.Close()
	cache := make([]byte, 128)
	_, err = resp.Body.Read(cache)
	for err == nil {
		_, err = resp.Body.Read(cache)
	}
	require.Equal(t, ErrBodyReadTimeout, err)
}

func TestTimeoutReadCloser_Read(t *testing.T) {
	// test for read data for input buffer timeout
	readCloser := timeoutReadCloser{
		timeoutMs: 1,
		body:      ioutil.NopCloser(&testTimeoutReader{}),
	}
	res := make([]byte, 30)
	_, err := readCloser.Read(res)
	require.Equal(t, ErrBodyReadTimeout, err)
}

func TestClient_Put(t *testing.T) {
	ctx := context.Background()
	result := &ret{}
	resp, err := simpleClient.Put(ctx, testServer.URL+"/json", &ret{Name: "TestClient_Put"})
	require.NoError(t, err)
	err = ParseData(resp, result)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "TestClient_Put+Test", result.Name)
}

func TestClient_Close(t *testing.T) {
	simpleClient.Close()
}

func TestClient_Head(t *testing.T) {
	ctx := context.Background()
	resp, err := simpleClient.Head(ctx, testServer.URL)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestClient_DoWithNoCrc(t *testing.T) {
	ctx := context.Background()
	result := &ret{}
	request, err := http.NewRequest(http.MethodPost, testServer.URL, nil)
	require.NoError(t, err)
	err = simpleClient.DoWith(ctx, request, result)
	require.NoError(t, err)
	require.NotNil(t, result)
}

func TestClient_DoWithCrc(t *testing.T) {
	ctx := context.Background()
	result := &ret{}
	request, err := http.NewRequest(http.MethodPost, testServer.URL+"/crc", nil)
	require.NoError(t, err)
	err = simpleClient.DoWith(ctx, request, result, WithCrcEncode())
	require.NoError(t, err)
	require.NotNil(t, result)
}

func TestClient_Form(t *testing.T) {
	m := make(map[string][]string)
	m["test"] = []string{"test_lb_Form"}
	ctx := context.Background()
	resp, err := simpleClient.Form(ctx, http.MethodPost, testServer.URL, m)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestClient_ParseDataWithContentLengthZero(t *testing.T) {
	m := make(map[string][]string)
	m["test"] = []string{"yest_lb_Form"}
	ctx := context.Background()
	result := &ret{}
	resp, err := simpleClient.Form(ctx, http.MethodPost, testServer.URL, m)
	require.NoError(t, err)
	resp.StatusCode = 400
	resp.ContentLength = 0
	err = ParseData(resp, result)
	require.Error(t, err)
}

func TestClient_ParseData(t *testing.T) {
	m := make(map[string][]string)
	m["test"] = []string{"yest_lb_Form"}
	ctx := context.Background()
	result := &ret{}
	resp, err := simpleClient.Form(ctx, http.MethodPost, testServer.URL, m)
	require.NoError(t, err)
	resp.StatusCode = 400
	resp.ContentLength = 12
	err = ParseData(resp, result)
	require.Error(t, err)
}

func TestClient_DoContextCancel(t *testing.T) {
	ctx := context.Background()
	result := &ret{}
	cancel, cancelFunc := context.WithCancel(ctx)
	request, err := http.NewRequest(http.MethodPost, testServer.URL, nil)
	require.NoError(t, err)
	cancelFunc()
	err = simpleClient.DoWith(cancel, request, result)
	require.Error(t, err)
}

func TestClient_ResponseClose(t *testing.T) {
	ctx := context.Background()
	result := &ret{}
	resp, err := simpleClient.Post(ctx, testServer.URL+"/json",
		&ret{Name: "TestClient_ResponseClose"})
	require.NoError(t, err)
	resp.Body.Close()
	err = ParseData(resp, result)
	require.Error(t, err)
}

func TestClient_PostWithOnServerNotAck(t *testing.T) {
	request, err := http.NewRequest(http.MethodPost, testServer.URL, nil)
	require.NoError(t, err)
	request.Header.Set(HeaderCrcEncoded, "1")
	response, err := simpleClient.Do(context.Background(), request)
	require.NoError(t, err)
	err = serverCrcEncodeCheck(context.Background(), request, response)
	response.Body.Close()
	require.NotNil(t, err)
}

func TestClient_PageNotFound(t *testing.T) {
	ctx := context.Background()
	result := &ret{}
	err := simpleClient.GetWith(ctx, testServer.URL+"/notfound", result)
	require.Error(t, err)
	require.Equal(t, "", result.Name)
}
