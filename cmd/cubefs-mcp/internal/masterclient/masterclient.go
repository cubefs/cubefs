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

// Package masterclient is a deliberately tiny HTTP client used by the
// cubefs-mcp tools to probe the cubefs master REST API.
//
// S1.1 only needed a reachability probe (Ping). S1.2 / S1.3 add generic
// Get / Post helpers that the bench / sync / cluster tools use to forward
// JSON payloads from master straight through to the LLM client.
//
// Design notes:
//   - The client is constructed once at startup and shared by all tool
//     handlers; it must therefore be safe for concurrent use (net/http
//     does this for us).
//   - Every call accepts a context.Context; tools are responsible for
//     attaching their own deadline (e.g. ping uses 5s, list/get 10s,
//     trigger/cancel 30s).
//   - Get / Post return the raw response body on 2xx and a typed
//     *HTTPError on 4xx/5xx so callers can render the status + body
//     verbatim into a structured tool error without re-fetching.
package masterclient

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// defaultHTTPTimeout is the absolute upper bound on a single master call.
// Tool handlers should additionally pass a per-call ctx deadline; this
// transport-level value only protects against fully-hung sockets.
const defaultHTTPTimeout = 30 * time.Second

// Client wraps a configured http.Client plus the base URL / auth token.
// Pass by pointer; the struct is safe for concurrent use.
type Client struct {
	baseURL    string
	authToken  string
	httpClient *http.Client
}

// New constructs a Client. baseURL must already be normalised (no trailing
// slash); see config.Load.
func New(baseURL, authToken string) *Client {
	return &Client{
		baseURL:   baseURL,
		authToken: authToken,
		httpClient: &http.Client{
			Timeout: defaultHTTPTimeout,
		},
	}
}

// BaseURL exposes the configured master endpoint so tools can echo it back
// to the caller (the ping tool surfaces it for visibility).
func (c *Client) BaseURL() string {
	return c.baseURL
}

// PingResult is returned by Ping; latency is measured wall-clock between
// request send and full response body read (the probe reads + discards the
// body to count the full round trip, not just the headers).
type PingResult struct {
	Reachable bool
	LatencyMs int64
	HTTPCode  int
	Err       error
}

// HTTPError represents a non-2xx response from master. It carries enough
// context for the tool layer to render a structured error result without
// having to make a second request to fetch the body.
type HTTPError struct {
	StatusCode int
	Body       string
}

// Error implements the error interface. The format is consumed verbatim by
// the tool error wrappers ("status=503 body=upstream timeout").
func (e *HTTPError) Error() string {
	return fmt.Sprintf("status=%d body=%s", e.StatusCode, e.Body)
}

// Get issues GET {base}{path}?{query} with the optional Authorization
// header attached. The full response body is buffered (master responses are
// small JSON envelopes) and returned. Non-2xx responses become *HTTPError.
//
// path must start with "/"; query may be nil.
func (c *Client) Get(ctx context.Context, path string, query url.Values) ([]byte, error) {
	return c.do(ctx, http.MethodGet, path, query, nil)
}

// Post issues POST {base}{path}?{query} with body (may be nil). Master's
// admin / bench / sync endpoints use query-string for parameters even on
// POST, so callers usually pass body=nil.
func (c *Client) Post(ctx context.Context, path string, query url.Values, body []byte) ([]byte, error) {
	return c.do(ctx, http.MethodPost, path, query, body)
}

// do is the single send/receive primitive shared by Get / Post. It applies
// the bearer header when configured, drains and buffers the body, and
// promotes non-2xx into HTTPError so the call sites stay short.
func (c *Client) do(ctx context.Context, method, path string, query url.Values, body []byte) ([]byte, error) {
	target := c.baseURL + path
	if len(query) > 0 {
		target += "?" + query.Encode()
	}
	var reqBody io.Reader
	if len(body) > 0 {
		reqBody = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, target, reqBody)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	if c.authToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.authToken)
	}
	if len(body) > 0 {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	buf, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, &HTTPError{StatusCode: resp.StatusCode, Body: string(buf)}
	}
	return buf, nil
}

// Ping probes GET {base}/admin/getCluster and reports reachability. It never
// returns an error: a transport failure becomes Reachable=false + Err set,
// because callers need a structured result either way.
func (c *Client) Ping(ctx context.Context) PingResult {
	target := c.baseURL + "/admin/getCluster"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return PingResult{Err: fmt.Errorf("build request: %w", err)}
	}
	if c.authToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.authToken)
	}

	start := time.Now()
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return PingResult{
			LatencyMs: time.Since(start).Milliseconds(),
			Err:       err,
		}
	}
	defer resp.Body.Close()
	// Drain the body so the connection can be reused and the latency
	// number reflects the real end-to-end cost.
	_, _ = io.Copy(io.Discard, resp.Body)
	latency := time.Since(start).Milliseconds()

	return PingResult{
		Reachable: resp.StatusCode >= 200 && resp.StatusCode < 500,
		LatencyMs: latency,
		HTTPCode:  resp.StatusCode,
	}
}
