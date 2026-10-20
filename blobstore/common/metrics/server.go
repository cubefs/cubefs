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
	"net/http"
	"net/http/httptest"

	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/rpc/auth"
	auth_proto "github.com/cubefs/cubefs/blobstore/common/rpc/auth/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc2"
)

const (
	PathPrometheus = "/metrics/prometheus"

	// maxMetricsBodySize limits the metrics payload served on rpc2;
	maxMetricsBodySize = 16 << 20

	// Placeholder label values resolved at startup to the local hostname / IPv4.
	autoHostValue = "__auto_host__"
	autoIPValue   = "__auto_ip__"
)

// Config is the metrics scrape settings embedded in service JSON.
// Prometheus metrics are always served on the service bind_addr via /metrics/prometheus.
type Config struct {
	Labels map[string]string `json:"labels"`
	Auth   auth_proto.Config `json:"auth"`
}

// metricsHandler serves PathPrometheus on both http and rpc2 protocols.
// It acts as a middleware / interceptor: requests to other paths pass
// through, the metrics path is served and the chain terminates.
type metricsHandler struct {
	handler http.Handler
	auth    interface {
		rpc2.Interceptor
		rpc.ProgressHandler
	}
}

func (h *metricsHandler) Handler(w http.ResponseWriter, r *http.Request, next func(http.ResponseWriter, *http.Request)) {
	if r.URL.Path != PathPrometheus {
		next(w, r)
		return
	}
	if h.auth != nil {
		h.auth.Handler(w, r, func(w http.ResponseWriter, r *http.Request) { h.handler.ServeHTTP(w, r) })
		return
	}
	h.handler.ServeHTTP(w, r)
}

func (h *metricsHandler) Handle(w rpc2.ResponseWriter, req *rpc2.Request, next rpc2.Handle) error {
	if req.RemotePath != PathPrometheus {
		return next(w, req)
	}
	serve := func(w rpc2.ResponseWriter, req *rpc2.Request) error {
		rec := httptest.NewRecorder()
		h.handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, PathPrometheus, nil))
		if rec.Code != http.StatusOK {
			return rpc2.NewError(int32(rec.Code), "Metrics", http.StatusText(rec.Code))
		}
		if rec.Body.Len() > maxMetricsBodySize {
			return rpc2.NewError(http.StatusRequestEntityTooLarge, "Metrics", "metrics body too large")
		}
		w.SetContentLength(int64(rec.Body.Len()))
		_, err := w.ReadFrom(rec.Body)
		return err
	}
	if h.auth != nil {
		return h.auth.Handle(w, req, serve)
	}
	return serve(w, req)
}

// New builds a handler serving PathPrometheus on both rpc2 and http
// protocols, passing other requests through.
// It returns nil if metrics is not configured.
func New(cfg *Config) (interface {
	rpc2.Interceptor
	rpc.ProgressHandler
}, error,
) {
	if cfg == nil || len(cfg.Labels) == 0 {
		return nil, nil
	}
	handler, err := NewHandler(cfg)
	if err != nil {
		return nil, err
	}
	h := &metricsHandler{handler: handler}
	if cfg.Auth.EnableAuth && cfg.Auth.Secret != "" {
		h.auth = auth.New(&cfg.Auth)
	}
	return h, nil
}
