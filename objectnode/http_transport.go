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
}

func (c *TransportConfig) CheckAndFix() {
	if c.DialTimeoutMs <= 0 {
		c.DialTimeoutMs = 5 * 1000
	}
	if c.ResponseHeaderTimeoutMs <= 0 {
		c.ResponseHeaderTimeoutMs = 60 * 1000
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
}

func (c *TransportConfig) BuildTransport() *http.Transport {
	dialer := &net.Dialer{
		KeepAlive: 30 * time.Second,
		Timeout:   time.Duration(c.DialTimeoutMs) * time.Millisecond,
	}

	return &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		MaxConnsPerHost:       c.MaxConnsPerHost,
		MaxIdleConnsPerHost:   c.MaxIdleConnsPerHost,
		IdleConnTimeout:       time.Duration(c.IdleConnTimeoutMs) * time.Millisecond,
		ResponseHeaderTimeout: time.Duration(c.ResponseHeaderTimeoutMs) * time.Millisecond,
		DisableCompression:    true,
		WriteBufferSize:       1 << 16,
		ReadBufferSize:        1 << 16,
		DialContext:           dialer.DialContext,
	}
}
