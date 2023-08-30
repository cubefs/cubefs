// Copyright 2023 The CubeFS Authors.
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

package objectnode

import (
	"crypto/tls"
	"net"
	"net/http"
	"time"
)

type TransportConfig struct {
	// DialTimeoutMs dial timeout in milliseconds
	DialTimeoutMs int64 `json:"dial_timeout_ms"`
	// ResponseHeaderTimeoutMs response header timeout after send the request
	ResponseHeaderTimeoutMs int64 `json:"response_header_timeout_ms"`

	MaxConnsPerHost     int `json:"max_conns_per_host"`
	MaxIdleConns        int `json:"max_idle_conns"`
	MaxIdleConnsPerHost int `json:"max_idle_conns_per_host"`
	// IdleConnTimeout is the maximum amount of time an idle (keep-alive)
	// connection will remain idle before closing itself.
	IdleConnTimeoutMs int64 `json:"idle_conn_timeout_ms"`
	// DisableCompression, if true, prevents the Transport from
	// requesting compression with an "Accept-Encoding: gzip"
	DisableCompression bool `json:"disable_compression"`

	// RootRAFile is the root CA file path
	RootRAFile string `json:"root_ra_file"`
}

func (c *TransportConfig) FixConfig() error {
	if c.DialTimeoutMs <= 0 {
		c.DialTimeoutMs = 5 * 1000
	}
	if c.ResponseHeaderTimeoutMs <= 0 {
		c.ResponseHeaderTimeoutMs = 60 * 1000
	}
	if c.MaxConnsPerHost <= 0 {
		c.MaxConnsPerHost = 1024
	}
	if c.MaxIdleConns <= 0 {
		c.MaxIdleConns = 1024
	}
	if c.MaxIdleConnsPerHost <= 0 {
		c.MaxIdleConnsPerHost = 10
	}
	if c.IdleConnTimeoutMs <= 0 {
		c.IdleConnTimeoutMs = 15 * 1000
	}
	if !c.DisableCompression {
		c.DisableCompression = true
	}

	return nil
}

// BuildTransport builds an HTTP transport based on the TransportConfig.
func (c *TransportConfig) BuildTransport() (*http.Transport, error) {
	var tlsConfig tls.Config
	if c.RootRAFile != "" {
		rootCAs, err := GetRootCAs(c.RootRAFile)
		if err != nil {
			return nil, err
		}
		tlsConfig.RootCAs = rootCAs
	}
	dialer := &net.Dialer{
		KeepAlive: 30 * time.Second,
		Timeout:   time.Duration(c.DialTimeoutMs) * time.Millisecond,
	}

	return &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		MaxConnsPerHost:       c.MaxConnsPerHost,
		MaxIdleConns:          c.MaxIdleConns,
		MaxIdleConnsPerHost:   c.MaxIdleConnsPerHost,
		IdleConnTimeout:       time.Duration(c.IdleConnTimeoutMs) * time.Millisecond,
		ResponseHeaderTimeout: time.Duration(c.ResponseHeaderTimeoutMs) * time.Millisecond,
		DisableCompression:    c.DisableCompression,
		WriteBufferSize:       1 << 16,
		ReadBufferSize:        1 << 16,
		TLSClientConfig:       &tlsConfig,
		DialContext:           dialer.DialContext,
	}, nil
}
