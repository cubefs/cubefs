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
	"encoding/xml"
	"fmt"
	"io"
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

type testErrorRespData struct {
	XMLName xml.Name `xml:"Error"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`
}

func initServer(t *testing.T, name string, cfg Config) (server *httptest.Server, tmpDir string, lc LogCloser) {
	tracer := trace.NewTracer(name)
	trace.SetGlobalTracer(tracer)

	tmpDir = fmt.Sprintf("%s/%s-auditlog-%s%s", os.TempDir(), name, strconv.FormatInt(time.Now().Unix(), 10), strconv.Itoa(rand.Intn(100000)))
	err := os.Mkdir(tmpDir, 0o755)
	require.NoError(t, err)

	cfg.LogDir = tmpDir
	cfg.MetricConfig = PrometheusConfig{Idc: name}
	cfg.Filters = []FilterConfig{{Must: Conditions{"term": {"method": "GET"}}}}

	var ah rpc.ProgressHandler
	ah, lc, err = Open(name, &cfg)
	require.NoError(t, err)
	require.NotNil(t, ah)
	require.NotNil(t, lc)

	bussinessHandler := func(w http.ResponseWriter, req *http.Request) {
		_, err := ioutil.ReadAll(req.Body)
		require.NoError(t, err)
		w.Header().Set("testh1", "testh1value")
		w.Header().Add("testh1", "testh1value2")
		w.Header().Set("Content-Type", rpc.MIMEJSON)
		w.Header()["ETag"] = []string{"etag value"}

		extraHeader := ExtraHeader(w)
		extraHeader.Set("extra-header1", "header1 value")
		extraHeader.Set("Extra-header2", "header2 value")
		extraHeader.Add("Extra-header2", "header2 value2")

		data, err := json.Marshal(testRespData{Result: "success"})
		require.NoError(t, err)
		w.Write(data)

		rw := w.(*responseWriter)
		if !rw.no2xxBody && rw.bodyLimit > 0 {
			require.Equal(t, len(data), rw.n)
			require.Equal(t, data, rw.body[:rw.n])
		} else {
			require.Equal(t, 0, rw.n)
		}
	}

	streamHandler := func(w http.ResponseWriter, req *http.Request) {
		size := int64(64 * 1024)
		buffer := make([]byte, size)
		for range [1024]struct{}{} {
			_, err := io.CopyN(w, bytes.NewReader(buffer), size)
			require.NoError(t, err)
		}

		span := trace.SpanFromContextSafe(req.Context())
		span.SetTag("response", "readfrom")
		span.AppendRPCTrackLog([]string{"stream"})

		rw := w.(*responseWriter)
		require.Equal(t, 0, rw.n)
		require.Equal(t, 1024*size, rw.bodyWritten)
	}

	errorResponseHandler := func(w http.ResponseWriter, req *http.Request) {
		errResp := testErrorRespData{
			Code:    "ErrorTestResponseCode",
			Message: "error test response message",
		}
		data, err := xml.Marshal(errResp)
		require.NoError(t, err)
		w.Header().Set("Content-Type", rpc.MIMEXML)
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.WriteHeader(http.StatusBadRequest)
		w.Write(data)
	}

	entryHandler := func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/":
			ah.Handler(w, req, bussinessHandler)
		case "/stream":
			ah.Handler(w, req, streamHandler)
		case "/error-response":
			ah.Handler(w, req, errorResponseHandler)
		default:
			w.WriteHeader(http.StatusNotImplemented)
		}
	}

	server = httptest.NewServer(http.HandlerFunc(entryHandler))
	return server, tmpDir, lc
}

func initNoContentLengthServer(t *testing.T) (server *httptest.Server, tmpDir string, lc LogCloser) {
	moduleName := "TESTNOCONTENTLENGTHMOULE"
	tracer := trace.NewTracer(moduleName)
	trace.SetGlobalTracer(tracer)

	tmpDir = os.TempDir() + "/test-NoContentLength-log" + strconv.FormatInt(time.Now().Unix(), 10) + strconv.Itoa(rand.Intn(100000))
	err := os.Mkdir(tmpDir, 0o755)
	require.NoError(t, err)

	var ah rpc.ProgressHandler
	ah, lc, err = Open(moduleName, &Config{
		LogDir: tmpDir, ChunkBits: 29,
		MetricConfig: PrometheusConfig{
			Idc: moduleName,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, ah)
	require.NotNil(t, lc)

	noContentLengthHandler := func(w http.ResponseWriter, req *http.Request) {
		buffered, err := ioutil.ReadAll(req.Body)
		require.NoError(t, err)
		bodySize := req.Body.(*reqBodyReadCloser).bodyRead
		readSting := string(buffered[:bodySize])
		w.Header().Set("Content-Type", rpc.MIMEJSON)
		w.WriteHeader(http.StatusOK)
		data, err := json.Marshal(testRespData{Result: readSting})
		require.NoError(t, err)
		w.Write(data)
	}
	entryHandler := func(w http.ResponseWriter, req *http.Request) {
		ah.Handler(w, req, noContentLengthHandler)
	}

	server = httptest.NewServer(http.HandlerFunc(entryHandler))
	return server, tmpDir, lc
}

func TestOpen(t *testing.T) {
	cfg := Config{
		Filters: []FilterConfig{{Must: Conditions{"term": {"method": "GET"}}}},
	}
	server, tmpDir, lc := initServer(t, "testOpen", cfg)
	defer func() {
		server.Close()
		os.RemoveAll(tmpDir)
		lc.Close()
	}()

	url := server.URL
	client := http.DefaultClient

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

func TestNoContentLength(t *testing.T) {
	server, tmpDir, lc := initNoContentLengthServer(t)
	defer func() {
		server.Close()
		os.RemoveAll(tmpDir)
		lc.Close()
	}()

	url := server.URL
	client := http.DefaultClient

	body := strings.NewReader("{\"test1\":\"test1value\"}")
	req, err := http.NewRequest(http.MethodPost, url, body)
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	b, err := ioutil.ReadAll(resp.Body)
	require.NoError(t, err)
	respData := &testRespData{}
	err = json.Unmarshal(b, respData)
	require.NoError(t, err)
	require.Equal(t, "{\"test1\":\"test1value\"}", respData.Result)
	resp.Body.Close()
}

func TestNoLogBody(t *testing.T) {
	cfg := Config{
		No2xxBody: true,
	}
	server, tmpDir, lc := initServer(t, "testNoLogBody", cfg)
	defer func() {
		server.Close()
		os.RemoveAll(tmpDir)
		lc.Close()
	}()

	url := server.URL
	client := http.DefaultClient

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

	req, err = http.NewRequest(http.MethodPut, url+"/error-response", nil)
	require.NoError(t, err)
	resp, err = client.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	b, err = ioutil.ReadAll(resp.Body)
	require.NoError(t, err)
	respData2 := &testErrorRespData{}
	err = xml.Unmarshal(b, respData2)
	require.NoError(t, err)
	require.Equal(t, "ErrorTestResponseCode", respData2.Code)
	resp.Body.Close()

	open, err := os.Open(tmpDir)
	require.NoError(t, err)
	dirEntries, err := open.ReadDir(-1)
	require.NoError(t, err)
	require.Greater(t, len(dirEntries), 0)
	require.NoError(t, open.Close())
}

func TestResponseReadFrom(t *testing.T) {
	server, tmpDir, lc := initServer(t, "testResponseReadFrom", Config{LogFormat: LogFormatJSON})
	defer func() {
		server.Close()
		os.RemoveAll(tmpDir)
		lc.Close()
	}()

	req, err := http.NewRequest(http.MethodGet, server.URL+"/stream", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	_, err = io.Copy(io.Discard, resp.Body)
	require.NoError(t, err)
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
