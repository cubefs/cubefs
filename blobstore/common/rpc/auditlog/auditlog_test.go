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

package auditlog

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

type testRespData struct {
	Result string `json:"result"`
}

var (
	server *httptest.Server
	tmpDir string
	lc     LogCloser
)

func initServer(t *testing.T) {
	moduleName := "TESTMOULE"
	tracer := trace.NewTracer(moduleName)
	trace.SetGlobalTracer(tracer)

	tmpDir = os.TempDir() + "/test-audit-log" + strconv.FormatInt(time.Now().Unix(), 10) + strconv.Itoa(rand.Intn(100000))
	err := os.Mkdir(tmpDir, 0o755)
	require.NoError(t, err)

	var ah rpc.ProgressHandler
	ah, lc, err = Open(moduleName, &Config{
		LogDir: tmpDir, ChunkBits: 29,
		KeywordsFilter: []string{"Get"},
	})
	require.NoError(t, err)
	require.NotNil(t, ah)
	require.NotNil(t, lc)

	bussinessHandler := func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("testh1", "testh1value")
		w.Header().Set("Content-Type", rpc.MIMEJSON)
		w.WriteHeader(http.StatusOK)
		data, err := json.Marshal(testRespData{Result: "success"})
		require.NoError(t, err)
		w.Write(data)
	}
	entryHandler := func(w http.ResponseWriter, req *http.Request) {
		ah.Handler(w, req, bussinessHandler)
	}

	server = httptest.NewServer(http.HandlerFunc(entryHandler))
}

func close() {
	server.Close()
	os.RemoveAll(tmpDir)
	lc.Close()
}

func TestOpen(t *testing.T) {
	initServer(t)
	defer close()

	url := server.URL
	client := http.DefaultClient

	// test keywords filter
	req, err := http.NewRequest(http.MethodGet, url, nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	b, err := ioutil.ReadAll(resp.Body)
	require.NoError(t, err)
	respData := &testRespData{}
	err = json.Unmarshal(b, respData)
	require.NoError(t, err)
	require.Equal(t, "success", respData.Result)
	resp.Body.Close()

	open, err := os.Open(tmpDir)
	require.NoError(t, err)
	dirEntries, err := open.ReadDir(-1)
	require.NoError(t, err)
	require.Equal(t, 0, len(dirEntries))
	require.NoError(t, open.Close())

	for _, method := range []string{http.MethodPost, http.MethodDelete, http.MethodPut} {
		req, err := http.NewRequest(method, url, nil)
		require.NoError(t, err)
		resp, err := client.Do(req)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		b, err := ioutil.ReadAll(resp.Body)
		require.NoError(t, err)
		respData := &testRespData{}
		err = json.Unmarshal(b, respData)
		require.NoError(t, err)
		require.Equal(t, "success", respData.Result)
		resp.Body.Close()
	}

	open, err = os.Open(tmpDir)
	require.NoError(t, err)
	dirEntries, err = open.ReadDir(-1)
	require.NoError(t, err)
	require.Greater(t, len(dirEntries), 0)
}

func TestBodylimit(t *testing.T) {
	for _, limit := range []struct {
		actual, expected int
	}{
		{-100, 0},
		{-1, 0},
		{0, defaultReadBodyBuffLength},
		{10, 10},
		{defaultReadBodyBuffLength, defaultReadBodyBuffLength},
		{maxReadBodyBuffLength, maxReadBodyBuffLength},
		{maxReadBodyBuffLength + 1, maxReadBodyBuffLength},
	} {
		cfg := Config{BodyLimit: limit.actual}
		cfg.MetricConfig.Idc = fmt.Sprint(limit)
		_, lc, err := Open("name", &cfg)
		require.NoError(t, err)
		require.NoError(t, lc.Close())
		require.Equal(t, limit.expected, cfg.BodyLimit)
	}
}

func Benchmark_RowParser(b *testing.B) {
	line := strings.Join([]string{
		"REQ", "BENCH", "16866434380042975", "POST", "/bench/mark/test",
		`{"Host":"127.0.0.1:9500","IP":"10.10.10.10","RawQuery":"size=1751\u0026hashes=4","X-Crc-Encoded":"1"}`, "200",
		`{"Blobstore-Tracer-Traceid":"0e34ac5020793b24","Content-Length":"195","Content-Type":"application/json",` +
			`"Trace-Log":["PROXY","a_0_r_19_w_2","ACCESS:22"],"Trace-Tags":["http.method:POST"],"X-Ack-Crc-Encoded":"1"}`,
		"199", "22348",
	}, "\t")
	buff := []byte(line)
	sender := NewPrometheusSender(PrometheusConfig{Idc: "Benchmark_RowParser" + strconv.Itoa(rand.Intn(100000))})
	b.ResetTimer()
	for ii := 0; ii < b.N; ii++ {
		sender.Send(buff)
	}
}
