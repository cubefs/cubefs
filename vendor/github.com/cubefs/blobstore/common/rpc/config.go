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
	"net"
	"net/http"
	"time"

	"github.com/cubefs/blobstore/common/rpc/auth"
)

// TransportConfig http transport config
type TransportConfig struct {
	// DialTimeoutMs dial timeout in milliseconds
	DialTimeoutMs int64 `json:"dial_timeout_ms"`
	// ResponseHeaderTimeoutMs response header timeout after send the request
	ResponseHeaderTimeoutMs int64 `json:"response_header_timeout_ms"`

	MaxConnsPerHost     int `json:"max_conns_per_host"`
	MaxIdleConns        int `json:"max_idle_conns"`
	MaxIdleConnsPerHost int `json:"max_idle_conns_per_host"`
	// IdleConnTimeout is the maximum amount of time an idle
	// (keep-alive) connection will remain idle before closing
	// itself.Zero means no limit.
	IdleConnTimeoutMs int64 `json:"idle_conn_timeout_ms"`
	// DisableCompression, if true, prevents the Transport from
	// requesting compression with an "Accept-Encoding: gzip"
	DisableCompression bool `json:"disable_compression"`

	// auth config
	Auth auth.Config `json:"auth"`
}

// NewTransport returns http transport
func NewTransport(cfg *TransportConfig) http.RoundTripper {
	tr := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		MaxConnsPerHost:       cfg.MaxConnsPerHost,
		MaxIdleConns:          cfg.MaxIdleConns,
		MaxIdleConnsPerHost:   cfg.MaxIdleConnsPerHost,
		IdleConnTimeout:       time.Duration(cfg.IdleConnTimeoutMs) * time.Millisecond,
		ResponseHeaderTimeout: time.Duration(cfg.ResponseHeaderTimeoutMs) * time.Millisecond,
		DisableCompression:    cfg.DisableCompression,
		WriteBufferSize:       1 << 16,
		ReadBufferSize:        1 << 16,
	}
	tr.DialContext = (&net.Dialer{
		Timeout:   time.Duration(cfg.DialTimeoutMs) * time.Millisecond,
		KeepAlive: 30 * time.Second,
	}).DialContext

	if cfg.Auth.EnableAuth {
		authTr := auth.NewAuthTransport(tr, &cfg.Auth)
		if authTr != nil {
			return authTr
		}
	}
	return tr
}
