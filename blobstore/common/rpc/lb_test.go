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
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/cubefs/cubefs/blobstore/util/log"
)

type ret struct {
	Name string `json:"name"`
}

type testReader struct {
	i    int64
	data []byte
}

func (r *testReader) Read(p []byte) (n int, err error) {
	if r.data == nil {
		return 0, errors.New("reader closed")
	}
	if r.i >= int64(len(r.data)) {
		return 0, io.EOF
	}
	n = copy(p, r.data[r.i:])
	r.i += int64(n)
	return
}

func (r *testReader) Close() {
	r.data = nil
}

func newTestReader(data []byte) *testReader {
	if data == nil {
		return nil
	}
	t := &testReader{data: data, i: 0}
	return t
}

func newCfg(hosts, backupHosts []string) *LbConfig {
	return &LbConfig{
		Hosts:       hosts,
		BackupHosts: backupHosts,
		Config: Config{
			Tc: TransportConfig{
				DialTimeoutMs:           1000,
				ResponseHeaderTimeoutMs: 3000,
				MaxConnsPerHost:         100,
				MaxIdleConns:            100,
				MaxIdleConnsPerHost:     10,
				IdleConnTimeoutMs:       60000,
				DisableCompression:      true,
			},
		},
	}
}

func TestLbClient_DefaultConfig(t *testing.T) {
	cfg := &LbConfig{
		Hosts:       []string{testServer.URL},
		BackupHosts: []string{testServer.URL},
	}
	client := NewLbClient(cfg, nil)
	defer client.Close()

	resp, err := client.Head(context.Background(), "/get/name")
	assert.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestLbClient_GetWithNoHost(t *testing.T) {
	now := time.Now().UnixNano() / 1e6
	log.SetOutputLevel(log.Lwarn)
	cfg := newCfg([]string{"http://127.0.0.1:8898", "http://127.0.0.1:8888"}, nil)
	cfg.FailRetryIntervalS = 5
	client := NewLbClient(cfg, nil)

	count := int64(0)
	wg := sync.WaitGroup{}
	var number int = 100
	wg.Add(number)
	for i := 0; i < number; i++ {
		go func() {
			defer wg.Done()
			ctx := context.Background()
			result := &ret{}
			err := client.GetWith(ctx, "/get/name?id="+strconv.Itoa(122), result)
			assert.Error(t, err)
			assert.NotNil(t, result)
			atomic.AddInt64(&count, 1)
		}()
	}
	wg.Wait()
	allTime := time.Now().UnixNano()/1e6 - now
	t.Logf("each request time is: %f ms", float64(allTime)/float64(number))
	client.Close()
}

func TestLbClient_Put(t *testing.T) {
	cfg := newCfg([]string{testServer.URL}, []string{testServer.URL})
	client := NewLbClient(cfg, nil)
	ctx := context.Background()
	data := &ret{Name: "TestLbClient_Put"}
	resp, err := client.Put(ctx, "/get/name?id="+strconv.Itoa(122), data)
	assert.NoError(t, err)
	resp.Body.Close()
	assert.NotNil(t, resp)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.NoError(t, err)
	client.Close()
}

func TestLbClient_GetWith(t *testing.T) {
	now := time.Now().UnixNano() / 1e6
	log.SetOutputLevel(log.Ldebug)
	cfg := newCfg([]string{testServer.URL, "http://127.0.0.1:8898", "http://127.0.0.1:12345"},
		[]string{testServer.URL})
	cfg.FailRetryIntervalS = 5
	client := NewLbClient(cfg, nil)
	wg := sync.WaitGroup{}
	var number int = 50
	wg.Add(number)
	for i := 0; i < number; i++ {
		go func() {
			defer wg.Done()
			ctx := context.Background()
			result := &ret{}
			err := client.GetWith(ctx, "/get/name?id="+strconv.Itoa(122), result)
			assert.NoError(t, err)
			assert.Equal(t, "Test_GetWith", result.Name)
		}()
	}
	wg.Wait()
	allTime := time.Now().UnixNano()/1e6 - now
	t.Logf("each request time is: %f ms", float64(allTime)/float64(number))
	client.Close()
}

func TestLbClient_Delete(t *testing.T) {
	cfg := newCfg([]string{testServer.URL}, []string{testServer.URL})
	client := NewLbClient(cfg, nil)
	ctx := context.Background()
	result := &ret{}
	resp, err := client.Delete(ctx, "/get/name?id="+strconv.Itoa(122))
	assert.NoError(t, err)
	err = ParseData(resp, result)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	client.Close()
}

func TestLbClient_PostWithCrc(t *testing.T) {
	cfg := newCfg([]string{"http://127.0.0.1:8889"}, []string{testServer.URL})
	client := NewLbClient(cfg, nil)
	ctx := context.Background()
	result := &ret{}
	err := client.PostWith(ctx, "/crc", result,
		&ret{Name: "Test_lb_PostWithCrc"}, WithCrcEncode())
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, result, &ret{Name: "Test_lb_PostWithCrc"})
	client.Close()
}

func TestLbClient_Head(t *testing.T) {
	cfg := newCfg([]string{testServer.URL}, []string{testServer.URL})
	client := NewLbClient(cfg, nil)

	ctx := context.Background()
	resp, err := client.Head(ctx, "/get/name")
	assert.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	client.Close()
}

func TestLbClient_PutWithNoCrc(t *testing.T) {
	cfg := newCfg([]string{testServer.URL}, nil)
	client := NewLbClient(cfg, nil)

	ctx := context.Background()
	result := &ret{}
	err := client.PutWith(ctx, "/json",
		result, &ret{Name: "Test_lb_PutWithNoCrc"})
	assert.NoError(t, err)
	assert.NotNil(t, result)
	client.Close()
}

func TestLbClient_PutWithCrc(t *testing.T) {
	cfg := newCfg([]string{"http://127.0.0.1:8889"}, []string{testServer.URL})
	client := NewLbClient(cfg, nil)
	ctx := context.Background()
	result := &ret{}
	err := client.PutWith(ctx, "/crc", result,
		&ret{Name: "Test_lb_PutWithCrc"}, WithCrcEncode())
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, &ret{Name: "Test_lb_PutWithCrc"}, result)
	client.Close()
}

func TestLbClient_PostWithNoCrc(t *testing.T) {
	cfg := newCfg([]string{testServer.URL}, nil)
	client := NewLbClient(cfg, nil)
	ctx := context.Background()
	result := &ret{}
	err := client.PostWith(ctx, "/json",
		result, &ret{Name: "Test_lb_PostJSONWith"})
	assert.NoError(t, err)
	assert.NotNil(t, result)
	client.Close()
}

func TestLbClient_Form(t *testing.T) {
	cfg := newCfg([]string{testServer.URL}, []string{testServer.URL})
	client := NewLbClient(cfg, nil)
	ctx := context.Background()
	m := make(map[string][]string)
	m["test"] = []string{"yest_lb_Form"}
	resp, err := client.Form(ctx, http.MethodPost, "/get/name?id="+strconv.Itoa(122), m)
	assert.NoError(t, err)
	resp.Body.Close()
	client.Close()
}

func TestLbClient_RetryWithBody(t *testing.T) {
	cfg := newCfg([]string{testServer.URL}, []string{testServer.URL})
	client := NewLbClient(cfg, nil)
	ctx := context.Background()
	request, err := http.NewRequest(http.MethodPost, "/retry", strings.NewReader("hello"))
	assert.NoError(t, err)

	resp, err := client.Do(ctx, request)
	assert.NoError(t, err)
	assert.Equal(t, resp.StatusCode, 200)
	resp.Body.Close()

	request, err = http.NewRequest(http.MethodPost, "/retry", newTestReader([]byte("hello")))
	assert.NoError(t, err)
	resp, err = client.Do(ctx, request)
	assert.NoError(t, err)
	assert.Equal(t, resp.StatusCode, 500)
	resp.Body.Close()

	client.Close()
}

func TestLbClient_RetryCrcBodyGetter(t *testing.T) {
	for _, try := range []int{1, 2, 3, 5, 7, 10} {
		cfg := newCfg([]string{}, []string{testServer.URL})
		cfg.RequestTryTimes = try
		cfg.ShouldRetry = func(code int, err error) bool {
			assert.Equal(t, 500, code)
			return true
		}
		client := NewLbClient(cfg, nil)
		err := client.PutWith(context.Background(), "/crcbody", nil,
			&ret{Name: "RetryCrcBodyGetter"}, WithCrcEncode())
		assert.Error(t, err)
		client.Close()
	}
}

func TestLbClient_DoWithNoCrc(t *testing.T) {
	cfg := newCfg([]string{testServer.URL}, []string{testServer.URL})
	client := NewLbClient(cfg, nil)
	result := &ret{}
	ctx := context.Background()
	request, err := http.NewRequest(http.MethodPost, "", nil)
	assert.NoError(t, err)
	err = client.DoWith(ctx, request, result)
	client.Close()
	assert.NoError(t, err)
	assert.NotNil(t, result)
}

func TestLbClient_DoWithCrc(t *testing.T) {
	cfg := newCfg([]string{"http://127.0.0.1:8889"}, []string{testServer.URL})
	client := NewLbClient(cfg, nil)
	result := &ret{}
	ctx := context.Background()
	request, err := http.NewRequest(http.MethodPost, "/crc", nil)
	assert.NoError(t, err)
	err = client.DoWith(ctx, request, result, WithCrcEncode())
	client.Close()
	assert.NoError(t, err)
	assert.NotNil(t, result)
}

func TestLbClient_Post(t *testing.T) {
	cfg := newCfg([]string{testServer.URL}, []string{testServer.URL})
	client := NewLbClient(cfg, nil)
	result := &ret{}
	ctx := context.Background()
	check, err := client.Post(ctx, "", ret{Name: "test_lb_PostJSON"})
	assert.NoError(t, err)
	client.Close()
	err = ParseData(check, result)
	assert.NoError(t, err)
	assert.NotNil(t, result)
}

func TestLbClient_New(t *testing.T) {
	cfg := newCfg([]string{testServer.URL}, []string{testServer.URL})
	NewLbClient(cfg, nil)
}

func TestLbClient_EnableHost(t *testing.T) {
	cfg := newCfg([]string{"http://127.0.0.1:8898"}, []string{testServer.URL, "http://127.0.0.1:8888"})
	cfg.FailRetryIntervalS = 1
	log.SetOutputLevel(log.Lwarn)
	client := NewLbClient(cfg, nil)
	wg := sync.WaitGroup{}
	var number int = 1
	wg.Add(number)
	for i := 0; i < number; i++ {
		go func() {
			defer wg.Done()
			ctx := context.Background()
			result := &ret{}
			err := client.GetWith(ctx, "/get/name?id="+strconv.Itoa(122), result)
			assert.NoError(t, err)
			assert.Equal(t, "Test_GetWith", result.Name)
		}()
	}
	wg.Wait()
	t.Log("waiting the failRetryIntervalS...")
	after := time.After(time.Second * time.Duration(cfg.FailRetryIntervalS+1))
	<-after
	resp, err := client.Head(context.Background(), "/get/name?id="+strconv.Itoa(122))
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	resp.Body.Close()
	client.Close()
}

func TestLbClient_OneHostWithNotConfigTryTimes(t *testing.T) {
	cfg := newCfg([]string{testServer.URL}, nil)
	cfg.HostTryTimes = 0
	cfg.RequestTryTimes = 0
	client := NewLbClient(cfg, nil)
	ctx := context.Background()
	result := &ret{}
	err := client.PostWith(ctx, "/json",
		result, &ret{Name: "Test_lb_PostJSONWith"})
	assert.NoError(t, err)
	assert.NotNil(t, result)
	client.Close()
}
