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

// Package tools owns every MCP tool exposed by cubefs-mcp.
//
// At S1.1 the package only contains the link-validation pair (ping, version).
// Bench / sync / cluster tools land in follow-up tasks (S1.2 / S1.3): each
// should drop into its own file here, expose a single registerXxx(server, deps)
// function, and get wired up from Register below. Keeping every tool in this
// package gives main.go a single import and a single Register call.
package tools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/cubefs/cubefs/cmd/cubefs-mcp/internal/masterclient"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

// Per-call deadlines for master REST forwards. List / get / health calls
// finish quickly so 10s catches a hung backend without surfacing flaky
// transient failures; trigger / cancel may queue work on master and are
// given 30s.
const (
	readTimeout  = 10 * time.Second
	writeTimeout = 30 * time.Second
)

// BuildInfo carries the ldflags-injected metadata surfaced by the version tool.
// Passing it through Register keeps main.go responsible for ldflags wiring
// and leaves this package free of build-system globals.
type BuildInfo struct {
	Version   string
	Commit    string
	BuildTime string
}

// Register attaches every cubefs-mcp tool to the given MCP server. Call
// exactly once during startup, before server.ServeStdio.
//
// Adding a new tool: drop a `registerXxx(s, ...)` line below and implement
// the registrar in its own file under this package.
func Register(s *server.MCPServer, mc *masterclient.Client, build BuildInfo) {
	registerPing(s, mc)
	registerVersion(s, mergeBuildInfo(build))

	// Bench tools (S1.2). Read-only listing + a single trigger / cancel pair.
	// Create / update / delete are intentionally NOT exposed; rule lifecycle
	// stays in human-driven UIs.
	registerBenchRuleList(s, mc)
	registerBenchRuleGet(s, mc)
	registerBenchRuleTrigger(s, mc)
	registerBenchTaskList(s, mc)
	registerBenchTaskGet(s, mc)
	registerBenchTaskCancel(s, mc)

	// Sync tools (S1.3). Same shape: read + cancel only.
	registerSyncTaskList(s, mc)
	registerSyncTaskGet(s, mc)
	registerSyncTaskCancel(s, mc)
	registerSyncNodeList(s, mc)

	// Cluster tools (S1.3).
	registerClusterStat(s, mc)
	registerClusterHealth(s, mc)
}

// mergeBuildInfo lets callers omit fields and fall back to versionDefaults.
// This avoids leaking empty strings to the tool consumer when only one of
// the three ldflags is set.
func mergeBuildInfo(in BuildInfo) versionInfo {
	out := versionDefaults
	if in.Version != "" {
		out.Version = in.Version
	}
	if in.Commit != "" {
		out.Commit = in.Commit
	}
	if in.BuildTime != "" {
		out.BuildTime = in.BuildTime
	}
	return out
}

// jsonResult is the shared helper for tools that return a structured object.
// It marshals to compact JSON and wraps the string with NewToolResultText,
// which is the MCP-go convention at v0.48.x for tool results.
//
// On marshal failure the helper returns a tool-level error (nil transport
// error) so Claude receives a structured error instead of a protocol fault.
func jsonResult(v any) (*mcp.CallToolResult, error) {
	buf, err := json.Marshal(v)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("marshal result: %v", err)), nil
	}
	return mcp.NewToolResultText(string(buf)), nil
}

// rawJSONResult forwards the master response body verbatim to the LLM.
// Master endpoints already speak JSON; re-marshalling would only flatten
// the envelope and lose context. We still validate it parses, so the LLM
// never sees binary garbage on the rare HTML error page.
func rawJSONResult(body []byte) (*mcp.CallToolResult, error) {
	if !json.Valid(body) {
		return mcp.NewToolResultErrorf("master returned non-JSON body: %s", truncate(string(body), 256)), nil
	}
	return mcp.NewToolResultText(string(body)), nil
}

// forwardError converts a masterclient call error into a structured tool
// error result. HTTPError is rendered with its status + body so the LLM
// can reason about why the call failed; transport errors are forwarded as
// "transport: ...". The returned `error` is always nil — a tool failure
// should never break the MCP transport.
func forwardError(err error) (*mcp.CallToolResult, error) {
	if err == nil {
		return mcp.NewToolResultError("unknown error"), nil
	}
	var httpErr *masterclient.HTTPError
	if errors.As(err, &httpErr) {
		return mcp.NewToolResultErrorf(
			"master HTTP error: status=%d body=%s",
			httpErr.StatusCode, truncate(httpErr.Body, 1024),
		), nil
	}
	return mcp.NewToolResultErrorf("master call failed: %v", err), nil
}

// forwardGet is the read-only master forwarder shared by every list / get
// tool. Centralising the timeout + error rendering keeps each tool file at
// roughly thirty lines.
func forwardGet(ctx context.Context, mc *masterclient.Client, path string, query url.Values) (*mcp.CallToolResult, error) {
	callCtx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()
	body, err := mc.Get(callCtx, path, query)
	if err != nil {
		return forwardError(err)
	}
	return rawJSONResult(body)
}

// forwardPost is the mutating sibling of forwardGet. It is reserved for
// trigger / cancel verbs and uses the longer writeTimeout because the
// underlying master handler typically enqueues work synchronously.
func forwardPost(ctx context.Context, mc *masterclient.Client, path string, query url.Values) (*mcp.CallToolResult, error) {
	callCtx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()
	body, err := mc.Post(callCtx, path, query, nil)
	if err != nil {
		return forwardError(err)
	}
	return rawJSONResult(body)
}

// truncate keeps error blobs from blowing up the LLM context. Master 5xx
// bodies are sometimes multi-KB HTML pages from a misbehaving proxy.
func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "...(truncated)"
}
