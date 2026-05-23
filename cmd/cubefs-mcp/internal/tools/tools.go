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
// Adding a new tool: drop a `registerXxx(s, ...)` line below in the right
// category section and implement the registrar in its own file under this
// package. Each new file should be ~30-50 lines and reuse the shared
// forward* helpers; do not add direct masterclient calls in tool files.
func Register(s *server.MCPServer, mc *masterclient.Client, build BuildInfo) {
	// Meta. Link-validation + build info.
	registerPing(s, mc)
	registerVersion(s, mergeBuildInfo(build))

	// Cluster.
	registerClusterStat(s, mc)
	registerClusterHealth(s, mc)

	// Bench Rule. CRUD + trigger; rule lifecycle is exposed since the LLM
	// is expected to drive bench scenarios end-to-end without falling back
	// to dashboard / curl. Write ops are flagged MUTATES / DESTRUCTIVE.
	registerBenchRuleList(s, mc)
	registerBenchRuleGet(s, mc)
	registerBenchRuleCreate(s, mc)
	registerBenchRuleUpdate(s, mc)
	registerBenchRuleDelete(s, mc)
	registerBenchRuleTrigger(s, mc)

	// Bench Task. Read + cancel/retry/delete. master rejects retry/delete
	// based on state, errors are forwarded verbatim.
	registerBenchTaskList(s, mc)
	registerBenchTaskGet(s, mc)
	registerBenchTaskCancel(s, mc)
	registerBenchTaskRetry(s, mc)
	registerBenchTaskDelete(s, mc)

	// Sync Rule. Full CRUD + pause/resume/trigger; same rationale as bench
	// rule above.
	registerSyncRuleList(s, mc)
	registerSyncRuleGet(s, mc)
	registerSyncRuleCreate(s, mc)
	registerSyncRuleUpdate(s, mc)
	registerSyncRuleDelete(s, mc)
	registerSyncRulePause(s, mc)
	registerSyncRuleResume(s, mc)
	registerSyncRuleTrigger(s, mc)

	// Sync Task. Read + cancel/retry/delete/export. export is NDJSON via
	// forwardGetText.
	registerSyncTaskList(s, mc)
	registerSyncTaskGet(s, mc)
	registerSyncTaskCancel(s, mc)
	registerSyncTaskRetry(s, mc)
	registerSyncTaskDelete(s, mc)
	registerSyncTaskExport(s, mc)

	// Sync Node. Inventory + dispatch-ring management. decommission carries
	// the DESTRUCTIVE warning since force=true orphans in-flight tasks.
	registerSyncNodeList(s, mc)
	registerSyncNodeTasks(s, mc)
	registerSyncNodeDrain(s, mc)
	registerSyncNodeRestore(s, mc)
	registerSyncNodeDecommission(s, mc)
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

// redactedMask is the placeholder substituted for any sensitive JSON value
// surfaced through cubefs-mcp. The same constant is used master-side
// (master/sync_rule_view.go) so callers can rely on a single signal —
// when they see "***" in a field, do NOT echo it back into an update.
const redactedMask = "***"

// sensitiveJSONKeys names the JSON keys whose string values must be
// redacted before forwarding to the LLM. The list is intentionally tight:
// only credential-bearing fields (S3 access/secret keys) — extending it
// requires explicit review because false positives mask legitimate data.
//
// Matching is case-sensitive against the canonical wire shape; both
// camelCase (LLM/dashboard) and PascalCase (master internal struct dump)
// variants are listed in case any handler accidentally emits the Go field
// name instead of the json tag.
var sensitiveJSONKeys = map[string]struct{}{
	"accessKey": {}, "AccessKey": {},
	"secretKey": {}, "SecretKey": {},
}

// redactSensitiveJSON walks an arbitrary JSON document and replaces every
// non-empty string value whose key matches sensitiveJSONKeys with
// redactedMask. The document is round-tripped through encoding/json so
// formatting (whitespace, key order) may change — callers must not rely
// on byte-equality with the master body.
//
// Defense-in-depth layer (see plan doc docs/plan/mcp/healthcheck-findings-fixes.md
// §P0): even though master is the primary redactor, this catches leaks
// from any path that bypasses the master view layer (future endpoints,
// raw debug dumps, accidental struct printing). On marshal/unmarshal
// failure the original body is returned unchanged — the caller's
// json.Valid gate already rejected non-JSON bodies upstream.
func redactSensitiveJSON(body []byte) []byte {
	var doc any
	if err := json.Unmarshal(body, &doc); err != nil {
		return body
	}
	redactJSONValue(doc)
	out, err := json.Marshal(doc)
	if err != nil {
		return body
	}
	return out
}

// redactJSONValue recurses through maps and slices, replacing matching
// keys' string values with redactedMask. Non-string values keyed by a
// sensitive name are left alone (an int / object under "accessKey" is
// schema corruption, not a leak — surfacing it intact aids debugging).
// Empty strings are also left alone so the omitempty contract is
// preserved on the wire.
func redactJSONValue(v any) {
	switch t := v.(type) {
	case map[string]any:
		for k, val := range t {
			if _, ok := sensitiveJSONKeys[k]; ok {
				if s, isStr := val.(string); isStr && s != "" {
					t[k] = redactedMask
					continue
				}
			}
			redactJSONValue(val)
		}
	case []any:
		for _, e := range t {
			redactJSONValue(e)
		}
	}
}

// redactedJSONResult is the credential-aware counterpart of rawJSONResult.
// Use for any tool whose master endpoint can carry AK/SK in its envelope
// (sync rules today; bench rules reserved for the future). The body must
// already be valid JSON — non-JSON 2xx bodies are rare from master and
// are handled by rawJSONResult's gate when callers fall back to it.
func redactedJSONResult(body []byte) (*mcp.CallToolResult, error) {
	if !json.Valid(body) {
		return mcp.NewToolResultErrorf("master returned non-JSON body: %s", truncate(string(body), 256)), nil
	}
	return mcp.NewToolResultText(string(redactSensitiveJSON(body))), nil
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

// forwardGetText is the read-only forwarder for endpoints that return
// non-JSON payloads (e.g. /syncTask/export emits NDJSON). The body is
// surfaced verbatim as MCP text without json.Valid() gating, since
// NDJSON would always fail that check.
//
// Callers must reserve this for paths whose content-type we control;
// any HTML 5xx body from a misbehaving proxy still flows through, but
// the masterclient already promoted non-2xx into HTTPError so only 2xx
// responses reach this branch.
func forwardGetText(ctx context.Context, mc *masterclient.Client, path string, query url.Values) (*mcp.CallToolResult, error) {
	callCtx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()
	body, err := mc.Get(callCtx, path, query)
	if err != nil {
		return forwardError(err)
	}
	return mcp.NewToolResultText(string(body)), nil
}

// forwardPostJSON is the mutating forwarder for endpoints that accept a
// JSON document body (e.g. /benchRule/create, /syncRule/update). The
// raw body string is forwarded byte-for-byte; master applies its own
// schema validation (DisallowUnknownFields on the bench side, spec
// package on the sync side), so we only validate that the caller did
// not hand us an empty body.
func forwardPostJSON(ctx context.Context, mc *masterclient.Client, path string, query url.Values, jsonBody string) (*mcp.CallToolResult, error) {
	if jsonBody == "" {
		return mcp.NewToolResultError("empty body: this endpoint requires a JSON document"), nil
	}
	callCtx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()
	body, err := mc.Post(callCtx, path, query, []byte(jsonBody))
	if err != nil {
		return forwardError(err)
	}
	return rawJSONResult(body)
}

// forwardGetRedacted is forwardGet plus redactSensitiveJSON on the body.
// Use this for any read-only endpoint that can carry AK/SK (sync_rule
// list/get today). Keeping a distinct entry point — rather than baking
// redaction into rawJSONResult — preserves byte-faithful forwarding for
// callers that don't need it (export NDJSON, cluster stats, etc.).
func forwardGetRedacted(ctx context.Context, mc *masterclient.Client, path string, query url.Values) (*mcp.CallToolResult, error) {
	callCtx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()
	body, err := mc.Get(callCtx, path, query)
	if err != nil {
		return forwardError(err)
	}
	return redactedJSONResult(body)
}

// forwardPostRedacted is forwardPost plus redactSensitiveJSON on the
// body. Used by sync_rule pause/resume whose master response carries the
// updated rule envelope.
func forwardPostRedacted(ctx context.Context, mc *masterclient.Client, path string, query url.Values) (*mcp.CallToolResult, error) {
	callCtx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()
	body, err := mc.Post(callCtx, path, query, nil)
	if err != nil {
		return forwardError(err)
	}
	return redactedJSONResult(body)
}

// forwardPostJSONRedacted is forwardPostJSON plus redactSensitiveJSON.
// Used by sync_rule create/update whose master response echoes back the
// stored rule envelope.
func forwardPostJSONRedacted(ctx context.Context, mc *masterclient.Client, path string, query url.Values, jsonBody string) (*mcp.CallToolResult, error) {
	if jsonBody == "" {
		return mcp.NewToolResultError("empty body: this endpoint requires a JSON document"), nil
	}
	callCtx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()
	body, err := mc.Post(callCtx, path, query, []byte(jsonBody))
	if err != nil {
		return forwardError(err)
	}
	return redactedJSONResult(body)
}

// truncate keeps error blobs from blowing up the LLM context. Master 5xx
// bodies are sometimes multi-KB HTML pages from a misbehaving proxy.
func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "...(truncated)"
}
