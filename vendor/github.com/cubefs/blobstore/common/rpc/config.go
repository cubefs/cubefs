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
