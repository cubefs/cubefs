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

package tools

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/cubefs/cubefs/cmd/cubefs-mcp/internal/masterclient"
	"github.com/mark3labs/mcp-go/mcp"
)

// resultText flattens the text content of an MCP CallToolResult so each
// assertion can substring-match without dancing around the Content[] slice.
// The forward* helpers in this package only ever emit a single TextContent,
// so empty result / multi-content cases are treated as test bugs.
func resultText(t *testing.T, res *mcp.CallToolResult) string {
	t.Helper()
	if res == nil {
		t.Fatalf("expected non-nil CallToolResult")
	}
	if len(res.Content) == 0 {
		t.Fatalf("expected at least one content block")
	}
	tc, ok := res.Content[0].(mcp.TextContent)
	if !ok {
		t.Fatalf("expected TextContent, got %T", res.Content[0])
	}
	return tc.Text
}

// TestForwardPostJSON_EmptyBody verifies the guard rail: an empty body
// must short-circuit before the HTTP layer is touched. We point the
// client at an unreachable URL so any accidental call would surface as a
// transport error and fail this assertion.
func TestForwardPostJSON_EmptyBody(t *testing.T) {
	mc := masterclient.New("http://127.0.0.1:1", "")
	res, err := forwardPostJSON(context.Background(), mc, "/x", nil, "")
	if err != nil {
		t.Fatalf("unexpected transport error: %v", err)
	}
	if !res.IsError {
		t.Fatalf("expected IsError=true for empty body")
	}
	if got := resultText(t, res); !strings.Contains(got, "empty body") {
		t.Fatalf("expected 'empty body' in error text, got %q", got)
	}
}

// TestForwardPostJSON_5xxBody verifies a non-2xx response from master is
// promoted to HTTPError by masterclient and rendered into the structured
// tool error envelope with status + body intact.
func TestForwardPostJSON_5xxBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("upstream timeout"))
	}))
	defer srv.Close()

	mc := masterclient.New(srv.URL, "")
	res, err := forwardPostJSON(context.Background(), mc, "/x", nil, `{"a":1}`)
	if err != nil {
		t.Fatalf("unexpected transport error: %v", err)
	}
	if !res.IsError {
		t.Fatalf("expected IsError=true for 5xx response")
	}
	got := resultText(t, res)
	if !strings.Contains(got, "status=503") {
		t.Fatalf("expected 'status=503' in error text, got %q", got)
	}
	if !strings.Contains(got, "upstream timeout") {
		t.Fatalf("expected upstream body echoed in error text, got %q", got)
	}
}

// TestForwardGetText_NonJSON verifies the NDJSON/text path: a successful
// 2xx response with non-JSON content (the canonical case is
// /syncTask/export emitting newline-delimited JSON) must pass through
// verbatim without the json.Valid gate that rawJSONResult applies.
func TestForwardGetText_NonJSON(t *testing.T) {
	body := "line1\nline2\nline3\n"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	mc := masterclient.New(srv.URL, "")
	res, err := forwardGetText(context.Background(), mc, "/export", url.Values{"since": {"2026-05-22T00:00:00Z"}})
	if err != nil {
		t.Fatalf("unexpected transport error: %v", err)
	}
	if res.IsError {
		t.Fatalf("expected IsError=false for non-JSON 2xx body; got error text %q", resultText(t, res))
	}
	if got := resultText(t, res); got != body {
		t.Fatalf("expected body passthrough %q, got %q", body, got)
	}
}

// TestForwardGetText_CtxCancel verifies that a cancelled context routes
// through forwardError as a transport failure, not as a panic and not as
// a silent success. The unreachable target ensures any failure of the
// cancel-check would surface as a different error string.
func TestForwardGetText_CtxCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	mc := masterclient.New("http://127.0.0.1:1", "")
	res, err := forwardGetText(ctx, mc, "/x", nil)
	if err != nil {
		t.Fatalf("unexpected transport error: %v", err)
	}
	if !res.IsError {
		t.Fatalf("expected IsError=true for cancelled context")
	}
	if got := resultText(t, res); !strings.Contains(got, "master call failed") {
		t.Fatalf("expected forwardError envelope, got %q", got)
	}
}
