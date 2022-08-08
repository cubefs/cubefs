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

	"github.com/stretchr/testify/assert"

	"github.com/cubefs/cubefs/blobstore/common/crc32block"
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
			w.WriteHeader(500)
			w.Write([]byte("test retry"))
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
		w.WriteHeader(404)
		w.Write([]byte("404 page not found"))
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
		return
	}
	r.Name = r.Name + "+Test"
	marshal, err := json.Marshal(r)
	if err != nil {
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
		MaxIdleConns:            100,
		MaxIdleConnsPerHost:     10,
		IdleConnTimeoutMs:       60000,
		DisableCompression:      true,
	},
}
var simpleClient = NewClient(simpleCfg)

func init() {
	testServer = httptest.NewServer(&handler{})
}

func TestClient_GetWith(t *testing.T) {
	ctx := context.Background()
	result := &ret{}
	err := simpleClient.GetWith(ctx, testServer.URL, result)
	assert.NoError(t, err)
	assert.NotNil(t, result)
}

func TestClient_PostWithCrc(t *testing.T) {
	ctx := context.Background()
	result := &ret{}
	err := simpleClient.PostWith(ctx, testServer.URL+"/crc", result,
		&ret{Name: "TestClient_PostWithCrc"}, WithCrcEncode())
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "TestClient_PostWithCrc", result.Name)
}

func TestClient_PostWithNoCrc(t *testing.T) {
	ctx := context.Background()
	result := &ret{}
	err := simpleClient.PostWith(ctx, testServer.URL+"/json", result, &ret{Name: "TestClient_PostWithNoCrc"})
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "TestClient_PostWithNoCrc+Test", result.Name)
}

func TestClient_Delete(t *testing.T) {
	ctx := context.Background()
	resp, err := simpleClient.Delete(ctx, testServer.URL)
	assert.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestClient_PutWithNoCrc(t *testing.T) {
	ctx := context.Background()
	result := &ret{}
	err := simpleClient.PutWith(ctx, testServer.URL+"/json", result, &ret{Name: "TestClient_PutWithNoCrc"})
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "TestClient_PutWithNoCrc+Test", result.Name)
}

func TestClient_PutWithCrc(t *testing.T) {
	ctx := context.Background()
	result := &ret{}
	err := simpleClient.PostWith(ctx, testServer.URL+"/crc", result,
		&ret{Name: "TestClient_PutWithCrc"}, WithCrcEncode())
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "TestClient_PutWithCrc", result.Name)
}

func TestClient_Post(t *testing.T) {
	ctx := context.Background()
	result := &ret{}
	resp, err := simpleClient.Post(ctx, testServer.URL+"/json", &ret{Name: "TestClient_Post"})
	assert.NoError(t, err)
	err = ParseData(resp, result)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "TestClient_Post+Test", result.Name)
}

func TestClient_PostWithReadResponseTimeout(t *testing.T) {
	simpleCfg.BodyBaseTimeoutMs = 50
	simpleCfg.BodyBandwidthMBPs = 1
	simpleCli := NewClient(simpleCfg)
	ctx := context.Background()
	resp, err := simpleCli.Post(ctx, testServer.URL+"/timeout", &ret{Name: "TestClient_Post"})
	assert.NoError(t, err)
	defer resp.Body.Close()
	cache := make([]byte, 128)
	for err == nil {
		if _, e := resp.Body.Read(cache); e != nil {
			assert.Equal(t, "read body timeout", e.Error())
			return
		}
	}
}

func TestClient_Put(t *testing.T) {
	ctx := context.Background()
	result := &ret{}
	resp, err := simpleClient.Put(ctx, testServer.URL+"/json", &ret{Name: "TestClient_Put"})
	assert.NoError(t, err)
	err = ParseData(resp, result)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "TestClient_Put+Test", result.Name)
}

func TestClient_Close(t *testing.T) {
	simpleClient.Close()
}

func TestClient_Head(t *testing.T) {
	ctx := context.Background()
	resp, err := simpleClient.Head(ctx, testServer.URL)
	assert.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestClient_DoWithNoCrc(t *testing.T) {
	ctx := context.Background()
	result := &ret{}
	request, err := http.NewRequest(http.MethodPost, testServer.URL, nil)
	assert.NoError(t, err)
	err = simpleClient.DoWith(ctx, request, result)
	assert.NoError(t, err)
	assert.NotNil(t, result)
}

func TestClient_DoWithCrc(t *testing.T) {
	ctx := context.Background()
	result := &ret{}
	request, err := http.NewRequest(http.MethodPost, testServer.URL+"/crc", nil)
	assert.NoError(t, err)
	err = simpleClient.DoWith(ctx, request, result, WithCrcEncode())
	assert.NoError(t, err)
	assert.NotNil(t, result)
}

func TestClient_Form(t *testing.T) {
	m := make(map[string][]string)
	m["test"] = []string{"test_lb_Form"}
	ctx := context.Background()
	resp, err := simpleClient.Form(ctx, http.MethodPost, testServer.URL, m)
	assert.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestClient_ParseDataWithContentLengthZero(t *testing.T) {
	m := make(map[string][]string)
	m["test"] = []string{"yest_lb_Form"}
	ctx := context.Background()
	result := &ret{}
	resp, err := simpleClient.Form(ctx, http.MethodPost, testServer.URL, m)
	assert.NoError(t, err)
	resp.StatusCode = 400
	resp.ContentLength = 0
	err = ParseData(resp, result)
	assert.Error(t, err)
}

func TestClient_ParseData(t *testing.T) {
	m := make(map[string][]string)
	m["test"] = []string{"yest_lb_Form"}
	ctx := context.Background()
	result := &ret{}
	resp, err := simpleClient.Form(ctx, http.MethodPost, testServer.URL, m)
	assert.NoError(t, err)
	resp.StatusCode = 400
	resp.ContentLength = 12
	err = ParseData(resp, result)
	assert.Error(t, err)
}

func TestClient_DoContextCancel(t *testing.T) {
	ctx := context.Background()
	result := &ret{}
	cancel, cancelFunc := context.WithCancel(ctx)
	request, err := http.NewRequest(http.MethodPost, testServer.URL, nil)
	assert.NoError(t, err)
	cancelFunc()
	err = simpleClient.DoWith(cancel, request, result)
	assert.Error(t, err)
}

func TestClient_ResponseClose(t *testing.T) {
	ctx := context.Background()
	result := &ret{}
	resp, err := simpleClient.Post(ctx, testServer.URL+"/json",
		&ret{Name: "TestClient_ResponseClose"})
	assert.NoError(t, err)
	resp.Body.Close()
	err = ParseData(resp, result)
	assert.Error(t, err)
}

func TestClient_PostWithOnServerNotAck(t *testing.T) {
	request, err := http.NewRequest(http.MethodPost, testServer.URL, nil)
	assert.NoError(t, err)
	request.Header.Set(HeaderCrcEncoded, "1")
	response, err := simpleClient.Do(context.Background(), request)
	assert.NoError(t, err)
	err = serverCrcEncodeCheck(context.Background(), request, response)
	response.Body.Close()
	assert.NotNil(t, err)
}

func TestClient_PageNotFound(t *testing.T) {
	ctx := context.Background()
	result := &ret{}
	err := simpleClient.GetWith(ctx, testServer.URL+"/notfound", result)
	assert.Error(t, err)
	assert.Equal(t, "", result.Name)
}
