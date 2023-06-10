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
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

type mockMetricSender struct {
	data []byte
}

func (m *mockMetricSender) Send(raw []byte) error {
	m.data = append(m.data, raw...)
	return nil
}

type mockLogFile struct {
	logs bytes.Buffer
}

func (m *mockLogFile) Log(data []byte) error {
	m.logs.Write(data)
	return nil
}

func (m *mockLogFile) Close() error {
	return nil
}

func TestHandler(t *testing.T) {
	// Create a mock metric sender and log file
	var (
		metricSender     = &mockMetricSender{}
		logFile          = &mockLogFile{}
		mockHttpRespJson = "{\"msg\": \"OK\"}"
		mockContentType  = "application/json"
		testModule       = "testModule"
	)

	cfg := &Config{
		BodyLimit: 13,
	}
	decoder := &defaultDecoder{}
	j := &jsonAuditlog{
		module:       testModule,
		metricSender: metricSender,
		logFile:      logFile,
		cfg:          cfg,
		decoder:      decoder,
		logPool: sync.Pool{
			New: func() interface{} {
				return new(bytes.Buffer)
			},
		},
		bodyPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, cfg.BodyLimit)
			},
		},
	}

	// Define a test handler
	testHandler := func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(mockHttpRespJson))
		w.Header().Set("Content-Type", mockContentType)
		w.Header().Set("Content-Length", strconv.Itoa(len(mockHttpRespJson)))
		assert.NoError(t, err)
	}

	// Create a test request with a body
	req := httptest.NewRequest("GET", "https://example.com/test", strings.NewReader("{\"text\": \"hello\"}"))
	req.Header.Set("Content-Type", mockContentType)

	// Record the response
	respRec := httptest.NewRecorder()

	// Call the Handler function
	j.Handler(respRec, req, testHandler)

	// Check the response
	resp := respRec.Result()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.Equal(t, mockHttpRespJson, string(body))

	// Check the log file
	var logEntry AuditLog
	err = json.Unmarshal(logFile.logs.Bytes(), &logEntry)
	assert.NoError(t, err)

	assert.Equal(t, "REQ", logEntry.ReqType)
	assert.Equal(t, testModule, logEntry.Module)
	assert.True(t, logEntry.StartTime > 0)
	assert.Equal(t, "GET", logEntry.Method)
	assert.Equal(t, "/test", logEntry.Path)
	assert.NotNil(t, logEntry.ReqHeader)
	assert.NotNil(t, logEntry.RespHeader)
	assert.Equal(t, "example.com", logEntry.ReqHeader["Host"])
	assert.True(t, logEntry.ReqHeader["IP"] != "")
	assert.Equal(t, logEntry.ReqParams["text"], "hello")
	assert.Equal(t, http.StatusOK, logEntry.StatusCode)
	assert.Contains(t, logEntry.RespHeader, "Blobstore-Tracer-Traceid")
	assert.True(t, fmt.Sprintf("%v", logEntry.RespHeader["Trace-Log"]) != "")
	assert.Equal(t, []interface{}{"span.kind:server"}, logEntry.RespHeader["Trace-Tags"])
	assert.Equal(t, mockHttpRespJson, logEntry.RespBody)
	assert.Equal(t, int64(len(mockHttpRespJson)), logEntry.BodyWritten)
	assert.True(t, logEntry.Duration > 0)
}
