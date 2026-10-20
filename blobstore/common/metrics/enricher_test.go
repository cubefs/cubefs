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

package metrics

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	auth_proto "github.com/cubefs/cubefs/blobstore/common/rpc/auth/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc2"
	"github.com/cubefs/cubefs/blobstore/common/rpc2/transport"
)

func TestEnricherConfiguredLabelsAndAutoHostIP(t *testing.T) {
	reg := prometheus.NewRegistry()
	g := prometheus.NewGauge(prometheus.GaugeOpts{Name: "test_enrich_gauge", Help: "test"})
	require.NoError(t, reg.Register(g))
	g.Set(1)

	e, err := NewEnricher(&Config{
		Labels: map[string]string{
			"m_region":     "cn-south",
			"ocs_region_m": "cn-south",
			"m_team":       "ebs",
			"cluster_id":   "1",
			"idc":          "z0",
			"y_host":       autoHostValue,
			"x_ip":         autoIPValue,
		},
	})
	require.NoError(t, err)
	e.gatherer = reg

	mfs, err := e.Gather()
	require.NoError(t, err)
	m := mfs[0].Metric[0]
	require.Equal(t, "cn-south", labelValue(m, "m_region"))
	require.Equal(t, "ebs", labelValue(m, "m_team"))
	require.Equal(t, "1", labelValue(m, "cluster_id"))
	require.Equal(t, "z0", labelValue(m, "idc"))

	host, err := os.Hostname()
	require.NoError(t, err)
	require.Equal(t, host, labelValue(m, "y_host"))
	require.NotEmpty(t, labelValue(m, "x_ip"))
	require.NotEqual(t, autoIPValue, labelValue(m, "x_ip"))
}

func TestEnricherSamePlaceholderOnMultipleKeys(t *testing.T) {
	reg := prometheus.NewRegistry()
	g := prometheus.NewGauge(prometheus.GaugeOpts{Name: "test_same_placeholder", Help: "test"})
	require.NoError(t, reg.Register(g))

	e, err := NewEnricher(&Config{
		Labels: map[string]string{"m_ip": autoIPValue, "ip": autoIPValue},
	})
	require.NoError(t, err)
	e.gatherer = reg

	mfs, err := e.Gather()
	require.NoError(t, err)
	m := mfs[0].Metric[0]
	require.NotEmpty(t, labelValue(m, "m_ip"))
	require.Equal(t, labelValue(m, "m_ip"), labelValue(m, "ip"))
}

func TestEnricherKeepsUnknownPlaceholderLiteral(t *testing.T) {
	reg := prometheus.NewRegistry()
	g := prometheus.NewGauge(prometheus.GaugeOpts{Name: "test_unknown_placeholder", Help: "test"})
	require.NoError(t, reg.Register(g))

	e, err := NewEnricher(&Config{
		Labels: map[string]string{"m_host": "__auto_hosts__", "m_team": "ebs"},
	})
	require.NoError(t, err)
	e.gatherer = reg

	mfs, err := e.Gather()
	require.NoError(t, err)
	m := mfs[0].Metric[0]
	require.Equal(t, "__auto_hosts__", labelValue(m, "m_host"))
	require.Equal(t, "ebs", labelValue(m, "m_team"))
}

func TestEnricherOmitsUnconfiguredLabels(t *testing.T) {
	reg := prometheus.NewRegistry()
	g := prometheus.NewGauge(prometheus.GaugeOpts{Name: "test_sparse", Help: "test"})
	require.NoError(t, reg.Register(g))

	e, err := NewEnricher(&Config{})
	require.NoError(t, err)
	e.gatherer = reg

	mfs, err := e.Gather()
	require.NoError(t, err)
	require.Empty(t, mfs[0].Metric[0].Label)
}

func TestEnricherDoesNotOverwriteExisting(t *testing.T) {
	reg := prometheus.NewRegistry()
	g := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "test_no_overwrite", Help: "test"}, []string{"idc", "m_service", "cluster_id"})
	require.NoError(t, reg.Register(g))
	g.WithLabelValues("keep-idc", "KEEP_SVC", "9").Set(2)

	e, err := NewEnricher(&Config{
		Labels: map[string]string{
			"idc":        "z0",
			"m_service":  "BLOBNODE",
			"cluster_id": "1",
		},
	})
	require.NoError(t, err)
	e.gatherer = reg

	mfs, err := e.Gather()
	require.NoError(t, err)
	m := mfs[0].Metric[0]
	require.Equal(t, "keep-idc", labelValue(m, "idc"))
	require.Equal(t, "KEEP_SVC", labelValue(m, "m_service"))
	require.Equal(t, "9", labelValue(m, "cluster_id"))
}

func TestEnricherKeepsEmptyConfiguredValue(t *testing.T) {
	reg := prometheus.NewRegistry()
	g := prometheus.NewGauge(prometheus.GaugeOpts{Name: "test_empty_val", Help: "test"})
	require.NoError(t, reg.Register(g))

	e, err := NewEnricher(&Config{
		Labels: map[string]string{"m_tag": "", "m_team": "ebs"},
	})
	require.NoError(t, err)
	e.gatherer = reg

	mfs, err := e.Gather()
	require.NoError(t, err)
	m := mfs[0].Metric[0]
	require.Contains(t, labelNames(m), "m_tag")
	require.Equal(t, "", labelValue(m, "m_tag"))
	require.Equal(t, "ebs", labelValue(m, "m_team"))
}

func TestEnricherSkipsInvalidLabelKey(t *testing.T) {
	reg := prometheus.NewRegistry()
	g := prometheus.NewGauge(prometheus.GaugeOpts{Name: "test_invalid_key", Help: "test"})
	require.NoError(t, reg.Register(g))

	e, err := NewEnricher(&Config{
		Labels: map[string]string{
			"m-region": "x",
			"m_team":   "ebs",
		},
	})
	require.NoError(t, err)
	e.gatherer = reg

	mfs, err := e.Gather()
	require.NoError(t, err)
	m := mfs[0].Metric[0]
	require.NotContains(t, labelNames(m), "m-region")
	require.Equal(t, "ebs", labelValue(m, "m_team"))
}

func TestEnricherKeepsConfiguredHostIP(t *testing.T) {
	reg := prometheus.NewRegistry()
	g := prometheus.NewGauge(prometheus.GaugeOpts{Name: "test_cfg_host_ip", Help: "test"})
	require.NoError(t, reg.Register(g))

	e, err := NewEnricher(&Config{
		Labels: map[string]string{
			"m_host": "fixed-host",
			"m_ip":   "10.0.0.1",
		},
	})
	require.NoError(t, err)
	e.gatherer = reg

	mfs, err := e.Gather()
	require.NoError(t, err)
	m := mfs[0].Metric[0]
	require.Equal(t, "fixed-host", labelValue(m, "m_host"))
	require.Equal(t, "10.0.0.1", labelValue(m, "m_ip"))
}

func TestEnricherPreservesBusinessCluster(t *testing.T) {
	reg := prometheus.NewRegistry()
	g := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "test_access_cluster", Help: "test"},
		[]string{"cluster"},
	)
	require.NoError(t, reg.Register(g))
	g.WithLabelValues("42").Set(1)

	e, err := NewEnricher(&Config{})
	require.NoError(t, err)
	e.gatherer = reg

	mfs, err := e.Gather()
	require.NoError(t, err)
	m := mfs[0].Metric[0]
	require.Equal(t, "42", labelValue(m, "cluster"))
	require.NotContains(t, labelNames(m), "cluster_id")
}

func TestEnricherNoTargetLabel(t *testing.T) {
	reg := prometheus.NewRegistry()
	g := prometheus.NewGauge(prometheus.GaugeOpts{Name: "test_no_target", Help: "test"})
	require.NoError(t, reg.Register(g))

	e, err := NewEnricher(&Config{Labels: map[string]string{"cluster_id": "1"}})
	require.NoError(t, err)
	e.gatherer = reg

	mfs, err := e.Gather()
	require.NoError(t, err)
	m := mfs[0].Metric[0]
	require.NotContains(t, labelNames(m), "target")
}

func TestNewHandler_GetPrometheusOK(t *testing.T) {
	h, err := NewHandler(&Config{
		Labels: map[string]string{
			"m_team":     "ebs",
			"m_service":  "BLOBNODE",
			"cluster_id": "1",
			"m_host":     autoHostValue,
		},
	})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, PathPrometheus, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	body, err := io.ReadAll(rec.Body)
	require.NoError(t, err)
	text := string(body)
	require.Contains(t, text, "m_host=")
	require.Contains(t, text, "m_team=\"ebs\"")
	require.Contains(t, text, "cluster_id=\"1\"")
	require.NotContains(t, text, "target=")
}

func TestNewHandler_MethodNotAllowed(t *testing.T) {
	h, err := NewHandler(&Config{})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, PathPrometheus, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestNewHandler_UnknownPathNotFound(t *testing.T) {
	h, err := NewHandler(&Config{})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code)
}

func TestNewProgressHandler(t *testing.T) {
	h, err := New(&Config{Labels: map[string]string{"cluster_id": "1"}})
	require.NoError(t, err)

	tests := []struct {
		name       string
		method     string
		path       string
		wantStatus int
		wantNext   bool
	}{
		{name: "prometheus", method: http.MethodGet, path: PathPrometheus, wantStatus: http.StatusOK},
		{name: "method not allowed", method: http.MethodPost, path: PathPrometheus, wantStatus: http.StatusMethodNotAllowed},
		{name: "pass through", method: http.MethodGet, path: "/service", wantStatus: http.StatusNoContent, wantNext: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, tt.path, nil)
			rec := httptest.NewRecorder()
			nextCalled := false
			h.Handler(rec, req, func(w http.ResponseWriter, r *http.Request) {
				nextCalled = true
				w.WriteHeader(http.StatusNoContent)
			})
			require.Equal(t, tt.wantNext, nextCalled)
			require.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestNewProgressHandlerAuth(t *testing.T) {
	h, err := New(&Config{
		Labels: map[string]string{"cluster_id": "1"},
		Auth:   auth_proto.Config{EnableAuth: true, Secret: "test-secret"},
	})
	require.NoError(t, err)

	// without token: forbidden
	req := httptest.NewRequest(http.MethodGet, PathPrometheus, nil)
	rec := httptest.NewRecorder()
	h.Handler(rec, req, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
	require.Equal(t, http.StatusForbidden, rec.Code)

	// with valid token: ok
	token := auth_proto.Encode(time.Now().Unix(), []byte(PathPrometheus), []byte("test-secret"))
	req = httptest.NewRequest(http.MethodGet, PathPrometheus, nil)
	req.Header.Set(auth_proto.TokenHeaderKey, token)
	rec = httptest.NewRecorder()
	h.Handler(rec, req, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
	require.Equal(t, http.StatusOK, rec.Code)

	// pass through request is not affected by auth
	req = httptest.NewRequest(http.MethodGet, "/service", nil)
	rec = httptest.NewRecorder()
	nextCalled := false
	h.Handler(rec, req, func(w http.ResponseWriter, r *http.Request) {
		nextCalled = true
		w.WriteHeader(http.StatusNoContent)
	})
	require.True(t, nextCalled)
	require.Equal(t, http.StatusNoContent, rec.Code)
}

func TestNewWithoutConfig(t *testing.T) {
	// nil config: no handler initialized
	h, err := New(nil)
	require.NoError(t, err)
	require.Nil(t, h)

	// empty labels means metrics not configured, even if auth is set
	h, err = New(&Config{
		Auth: auth_proto.Config{EnableAuth: true, Secret: "test-secret"},
	})
	require.NoError(t, err)
	require.Nil(t, h)
}

func labelNames(m *dto.Metric) []string {
	var names []string
	for _, lp := range m.Label {
		names = append(names, lp.GetName())
	}
	return names
}

func labelValue(m *dto.Metric, name string) string {
	for _, lp := range m.Label {
		if lp.GetName() == name {
			return lp.GetValue()
		}
	}
	return ""
}

type mockRpc2Writer struct {
	body []byte
}

func (w *mockRpc2Writer) SetContentLength(int64)                {}
func (w *mockRpc2Writer) Header() *rpc2.Header                  { return &rpc2.Header{} }
func (w *mockRpc2Writer) Trailer() *rpc2.FixedHeader            { return &rpc2.FixedHeader{} }
func (w *mockRpc2Writer) WriteHeader(int, rpc2.Marshaler) error { return nil }
func (w *mockRpc2Writer) WriteOK(obj rpc2.Marshaler) error {
	b, err := obj.Marshal()
	w.body = append(w.body, b...)
	return err
}
func (w *mockRpc2Writer) SetError(error) {}
func (w *mockRpc2Writer) Flush() error   { return nil }
func (w *mockRpc2Writer) ReadFrom(r io.Reader) (int64, error) {
	b, err := io.ReadAll(r)
	w.body = append(w.body, b...)
	return int64(len(b)), err
}

func (w *mockRpc2Writer) Write(p []byte) (int, error) {
	w.body = append(w.body, p...)
	return len(p), nil
}

func (w *mockRpc2Writer) WriteBody(func(rpc2.ChecksumBlock, *transport.Stream) (int64, error)) (int64, error) {
	return 0, nil
}

func (w *mockRpc2Writer) AfterBody(func() error) {}

func newRpc2Request(path string) *rpc2.Request {
	return &rpc2.Request{RequestHeader: rpc2.RequestHeader{RemotePath: path}}
}

func TestNewRpc2MetricsHandler(t *testing.T) {
	h, err := New(&Config{Labels: map[string]string{"cluster_id": "1"}})
	require.NoError(t, err)

	// pass through request is not affected
	w := &mockRpc2Writer{}
	nextCalled := false
	err = h.Handle(w, newRpc2Request("/service"), func(w rpc2.ResponseWriter, req *rpc2.Request) error {
		nextCalled = true
		return nil
	})
	require.NoError(t, err)
	require.True(t, nextCalled)

	// metrics path without auth: served with metrics text
	w = &mockRpc2Writer{}
	err = h.Handle(w, newRpc2Request(PathPrometheus), func(w rpc2.ResponseWriter, req *rpc2.Request) error { return nil })
	require.NoError(t, err)
	require.Contains(t, string(w.body), "go_goroutines")

	// auth enabled with labels: without token -> forbidden
	ha, err := New(&Config{
		Labels: map[string]string{"cluster_id": "1"},
		Auth:   auth_proto.Config{EnableAuth: true, Secret: "test-secret"},
	})
	require.NoError(t, err)

	w = &mockRpc2Writer{}
	err = ha.Handle(w, newRpc2Request(PathPrometheus), func(w rpc2.ResponseWriter, req *rpc2.Request) error { return nil })
	require.Error(t, err)
	status, _, _ := rpc2.DetectError(err)
	require.Equal(t, http.StatusForbidden, status)

	// with valid token: ok
	token := auth_proto.Encode(time.Now().Unix(), []byte(PathPrometheus), []byte("test-secret"))
	req := newRpc2Request(PathPrometheus)
	req.Header.Set(auth_proto.TokenHeaderKey, token)
	w = &mockRpc2Writer{}
	err = ha.Handle(w, req, func(w rpc2.ResponseWriter, req *rpc2.Request) error { return nil })
	require.NoError(t, err)
	require.Contains(t, string(w.body), "cluster_id=\"1\"")
}

func TestHandleDropsOversizedBody(t *testing.T) {
	// handler producing a payload larger than maxMetricsBodySize
	big := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(make([]byte, maxMetricsBodySize+1))
	})
	h := &metricsHandler{handler: big}

	w := &mockRpc2Writer{}
	err := h.Handle(w, newRpc2Request(PathPrometheus), func(w rpc2.ResponseWriter, req *rpc2.Request) error { return nil })
	require.Error(t, err)
	status, _, _ := rpc2.DetectError(err)
	require.Equal(t, http.StatusRequestEntityTooLarge, status)
	require.Empty(t, w.body)
}
