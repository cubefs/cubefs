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
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
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
		_, err := io.ReadAll(req.Body)
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
		buffered, err := io.ReadAll(req.Body)
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

func TestSamePrometheusConfig(t *testing.T) {
	cfg := Config{MetricConfig: PrometheusConfig{Idc: "same-idc"}}
	_, _, err := Open("prometheus", &cfg)
	require.NoError(t, err)
	_, _, err = Open("prometheus", &cfg)
	require.NoError(t, err)
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
	b, err := io.ReadAll(resp.Body)
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
		req, err = http.NewRequest(method, url, nil)
		require.NoError(t, err)
		resp, err = client.Do(req)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		b, err = io.ReadAll(resp.Body)
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
	b, err := io.ReadAll(resp.Body)
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
	b, err := io.ReadAll(resp.Body)
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
	b, err = io.ReadAll(resp.Body)
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

func TestPrometheusSizeTag(t *testing.T) {
	sender := NewPrometheusSender(PrometheusConfig{
		Idc:         "TestPrometheusSizeTag",
		SizeBuckets: []int64{0, 3, 7, 11},
	})
	cases := []struct {
		size int
		exp  string
	}{
		{-1, "0"},
		{0, "0"},
		{1, "0_3"},
		{3, "0_3"},
		{5, "3_7"},
		{7, "3_7"},
		{10, "7_11"},
		{11, "7_11"},
		{13, "11_"},
	}
	for _, cs := range cases {
		require.Equal(t, cs.exp, sender.getSizeTag(int64(cs.size)))
	}
}

func Benchmark_ParserRow(b *testing.B) {
	line := strings.Join([]string{
		"REQ", "BENCH", "16866434380042975", "POST", "/bench/mark/test",
		`{"Host":"127.0.0.1:9500","IP":"10.10.10.10","BodySize":1024,"RawQuery":"size=1751\u0026hashes=4","X-Crc-Encoded":"1"}`,
		"{}", "200",
		`{"Blobstore-Tracer-Traceid":"0e34ac5020793b24","Content-Length":"195","Content-Type":"application/json",` +
			`"Trace-Log":["PROXY","a_0_r_19_w_2","ACCESS:22"],"Trace-Tags":["http.method:POST"],"X-Ack-Crc-Encoded":"1"}`,
		"{}", "199", "262144",
	}, "\t")
	buff := []byte(line)
	sender := NewPrometheusSender(PrometheusConfig{
		Idc:                 "Benchmark_ParserRow" + strconv.Itoa(rand.Intn(100000)),
		Service:             "bench",
		Tag:                 "tag",
		Team:                "team",
		EnableHttpMethod:    true,
		EnableReqLengthCnt:  true,
		EnableRespLengthCnt: true,
		EnableRespDuration:  true,
		MaxApiLevel:         1,
		SizeBuckets:         []int64{0, 65536, 131072, 262144},
	})
	b.ResetTimer()
	for ii := 0; ii < b.N; ii++ {
		sender.Send(buff)
	}
}

func Benchmark_ParserAuditlog(b *testing.B) {
	entry := &auditLogEntry{log: &AuditLog{
		ReqType:    "REQ",
		Module:     "BENCH",
		StartTime:  16866434380042975,
		Method:     "POST",
		Path:       "/bench/mark/test",
		ReqHeader:  M{"Host": "127.0.0.1:9500", "IP": "10.10.10.10", "BodySize": int64(1024)},
		ReqParams:  "{}",
		StatusCode: 200,
		RespHeader: M{"Blobstore-Tracer-Traceid": "0e34ac5020793b24", "Content-Length": "195"},
		RespBody:   "{}",
		RespLength: 262144,
		Duration:   22348,
	}}
	sender := NewPrometheusSender(PrometheusConfig{
		Idc:                 "Benchmark_ParserAuditlog" + strconv.Itoa(rand.Intn(100000)),
		Service:             "bench",
		Tag:                 "tag",
		Team:                "team",
		EnableHttpMethod:    true,
		EnableReqLengthCnt:  true,
		EnableRespLengthCnt: true,
		EnableRespDuration:  true,
		MaxApiLevel:         1,
		SizeBuckets:         []int64{0, 65536, 131072, 262144},
	})
	b.ResetTimer()
	for ii := 0; ii < b.N; ii++ {
		sender.SendEntry(entry)
	}
}

func TestAuditLogEntry_Basic(t *testing.T) {
	moduleName := "auditor_test"
	tmpDir := t.TempDir()

	cfg := &Config{
		LogDir:    tmpDir,
		LogFormat: LogFormatJSON,
		MetricConfig: PrometheusConfig{
			Idc: moduleName,
		},
	}

	oh, lc, err := Open(moduleName, cfg)
	require.NoError(t, err)
	defer lc.Close()

	tracer := trace.NewTracer(moduleName)
	trace.SetGlobalTracer(tracer)

	offset, size := int64(1024), int64(4096)
	expectedReqParams := fmt.Sprintf(`{"offset":%d,"size":%d}`, offset, size)

	ctx := context.Background()
	err = oh.Audit(ctx, func(ctx context.Context, auditline *AuditLog) error {
		auditline.Method = "BlobGet"
		auditline.ReqParams = expectedReqParams

		time.Sleep(1 * time.Millisecond)

		span := trace.SpanFromContext(ctx)
		span.AppendRPCTrackLog([]string{"ACCESS", "DISK_READ"})
		span.SetTag("user", "test_user")
		return nil
	})
	require.NoError(t, err)

	infos, err := os.ReadDir(tmpDir)
	require.NoError(t, err)
	require.Len(t, infos, 1)

	content, err := os.ReadFile(filepath.Join(tmpDir, infos[0].Name()))
	t.Logf("audit log content: %s\n", string(content))
	require.NoError(t, err)

	var audit AuditLog
	err = json.Unmarshal(content, &audit)
	require.NoError(t, err)

	require.Equal(t, "AUDIT", audit.ReqType)
	require.Equal(t, moduleName, audit.Module)
	require.Equal(t, "BlobGet", audit.Method)
	require.Equal(t, expectedReqParams, audit.ReqParams)
	require.Equal(t, 200, audit.StatusCode)
	require.Greater(t, audit.Duration, int64(1000))

	require.Contains(t, audit.RespHeader, rpc.HeaderTraceLog)
	rawLogs := audit.RespHeader[rpc.HeaderTraceLog]
	logsBytes, _ := json.Marshal(rawLogs)
	require.Equal(t, `["ACCESS","DISK_READ"]`, string(logsBytes))

	require.Contains(t, audit.RespHeader[rpc.HeaderTraceTags], "user:test_user")
}

func requireMetric(t *testing.T, g prometheus.Gatherer, idc string, want map[string]string) {
	t.Helper()
	got, err := g.Gather()
	require.NoError(t, err)
	for _, mf := range got {
		if mf.GetName() != "service_response_code" {
			continue
		}
		for _, m := range mf.GetMetric() {
			labels := map[string]string{}
			for _, lp := range m.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			if labels["idc"] != idc {
				continue
			}
			match := true
			for k, v := range want {
				if labels[k] != v {
					match = false
					break
				}
			}
			if match {
				return
			}
		}
	}
	require.Failf(t, "metric not found", "idc=%s want=%v", idc, want)
}

func TestRespHeader(t *testing.T) {
	entry := &auditLogEntry{log: &AuditLog{RespHeader: M{"cluster": "3", "api": "put"}}}
	require.Equal(t, []string{"3"}, entry.RespHeader("cluster"))
	require.Equal(t, []string{"put"}, entry.RespHeader("api"))
	require.Nil(t, entry.RespHeader("absent"))

	entry = &auditLogEntry{log: &AuditLog{RespHeader: M{"Label-Cluster": "1"}}}
	require.Equal(t, []string{"1"}, entry.RespHeader("label-cluster"))
	require.Equal(t, []string{"1"}, entry.RespHeader("Label-Cluster"))

	line := strings.Join([]string{
		"REQ", "UL", "1", "POST", "/ul/test", `{"Host":"127.0.0.1"}`, "{}", "200",
		`{"cluster":"7","api":"ul.test"}`, "{}", "1", "1",
	}, "\t")
	row, err := ParseReqlogToAdrow(line)
	require.NoError(t, err)
	require.Nil(t, row.RespHeader("cluster"))
	require.Nil(t, row.RespHeader("api"))
}

func TestAuditLogEntryMatchesRequestRow(t *testing.T) {
	alog := &AuditLog{
		ReqType:    "REQ",
		Module:     "UL",
		StartTime:  1,
		Method:     "POST",
		Path:       "/ul/test",
		ReqHeader:  M{"Host": "127.0.0.1", "BodySize": int64(1024), "Token": M{"uid": uint32(9)}},
		ReqParams:  "{}",
		StatusCode: 599,
		RespHeader: M{
			"api": "put", "X-Log": []string{"ACCESS"}, "X-Warn": []string{"slow"},
			"SToken": M{"uid": uint32(7), "tbl": "b"}, "cluster": "3",
		},
		RespBody: "{}", RespLength: 10, Duration: 20,
	}
	entry := &auditLogEntry{log: alog}
	row, err := ParseReqlogToAdrow(string(alog.ToBytesWithTab(bytes.NewBuffer(nil))))
	require.NoError(t, err)
	require.Equal(t, row.Service(), entry.Service())
	require.Equal(t, row.Method(), entry.Method())
	require.Equal(t, row.Path(), entry.Path())
	require.Equal(t, row.Code(), entry.Code())
	require.Equal(t, row.ReqHost(), entry.ReqHost())
	require.Equal(t, row.ReqLength(), entry.ReqLength())
	require.Equal(t, row.RespLength(), entry.RespLength())
	require.Equal(t, row.RespTime(), entry.RespTime())
	require.Equal(t, row.ApiName(), entry.ApiName())
	require.Equal(t, row.Xlogs(), entry.Xlogs())
	require.Equal(t, row.XWarns(), entry.XWarns())
	require.Equal(t, uint32(0), entry.Uid())
	require.Equal(t, uint32(7), row.Uid())
	require.Equal(t, []string{"3"}, entry.RespHeader("cluster"))
	require.Nil(t, row.RespHeader("cluster"))
}

func TestUserLabelsSendEntry(t *testing.T) {
	reg := prometheus.NewRegistry()
	idc := "TestUserLabelsSendEntry"
	sender := NewPrometheusSender(PrometheusConfig{
		Idc: idc, Service: "ul", UserLabels: map[string]string{"cluster": "cluster"},
		registerer: reg,
	})
	sender.SendEntry(&auditLogEntry{log: &AuditLog{
		Module: "UL", Method: "POST", Path: "/ul/test", StatusCode: 200,
		RespHeader: M{"cluster": "5"},
	}})
	requireMetric(t, reg, idc, map[string]string{"cluster": "5", "code": "200"})

	idc = "TestUserLabelsRequestRow"
	sender = NewPrometheusSender(PrometheusConfig{
		Idc: idc, Service: "ulrow", UserLabels: map[string]string{"cluster": "cluster"},
		registerer: reg,
	})
	line := strings.Join([]string{
		"REQ", "ULROW", "1", "POST", "/ulrow/test", `{"Host":"127.0.0.1"}`, "{}", "200",
		`{"cluster":"9"}`, "{}", "1", "1",
	}, "\t")
	require.NoError(t, sender.Send([]byte(line)))
	requireMetric(t, reg, idc, map[string]string{"cluster": "", "code": "200"})
}

func TestUserLabelsNormalize(t *testing.T) {
	constLabels := map[string]string{"idc": "i", "host": "h", "service": "s", "team": "t"}
	cases := []struct {
		in          map[string]string
		keys, names []string
	}{
		{map[string]string{"": "skipped", "cluster": "cluster"}, []string{"cluster"}, []string{"cluster"}},
		{map[string]string{"cluster": ""}, []string{"cluster"}, []string{"cluster"}},
		{map[string]string{"code": "cluster"}, []string{"code"}, []string{"cluster"}},
		{map[string]string{"label-cluster": ""}, nil, nil},
		{map[string]string{"label-cluster": "cluster"}, []string{"label-cluster"}, []string{"cluster"}},
		{map[string]string{"label-cluster": "cid", "bad-name": "ok", "foo.bar": ""}, []string{"bad-name", "label-cluster"}, []string{"ok", "cid"}},
	}
	for _, tc := range cases {
		keys, names := sortedUserLabels(tc.in, constLabels)
		require.Equal(t, tc.keys, keys)
		require.Equal(t, tc.names, names)
	}
	for _, in := range []map[string]string{
		{"loc": "country"},
		{"b": "dup", "a": "dup"},
	} {
		require.Panics(t, func() { sortedUserLabels(in, constLabels) })
	}
}
