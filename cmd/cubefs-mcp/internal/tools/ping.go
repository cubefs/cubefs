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
	"time"

	"github.com/cubefs/cubefs/cmd/cubefs-mcp/internal/masterclient"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

// pingProbeTimeout bounds the per-call deadline for the reachability probe.
// 5s is chosen to align with the plan doc (S1.1: "通过 GET /admin/getCluster
// 探测，5s 超时").
const pingProbeTimeout = 5 * time.Second

// pingResultPayload is the structured result returned by the ping tool. It
// mirrors the contract documented in the S1.1 plan and is intentionally
// flat so the JSON shape is trivially consumable by Claude.
type pingResultPayload struct {
	Echo       string `json:"echo"`
	MasterAddr string `json:"master_addr"`
	Reachable  bool   `json:"reachable"`
	LatencyMs  int64  `json:"latency_ms"`
	HTTPCode   int    `json:"http_code,omitempty"`
	Error      string `json:"error,omitempty"`
}

// registerPing wires the ping tool onto the MCP server. Kept package-private:
// the only entry point is Register in tools.go.
func registerPing(s *server.MCPServer, mc *masterclient.Client) {
	tool := mcp.NewTool(
		"ping",
		mcp.WithDescription(
			"Echo a message and probe the configured CubeFS master "+
				"(GET /admin/getCluster, 5s timeout). Useful as a "+
				"first-step health check from Claude.",
		),
		mcp.WithString("message",
			mcp.Description(
				"Optional arbitrary string echoed back in the response. "+
					"Defaults to \"healthcheck\" when omitted so callers "+
					"can run a pure reachability probe without crafting a payload.",
			),
		),
	)

	s.AddTool(tool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		msg := req.GetString("message", "healthcheck")

		probeCtx, cancel := context.WithTimeout(ctx, pingProbeTimeout)
		defer cancel()
		probe := mc.Ping(probeCtx)

		payload := pingResultPayload{
			Echo:       msg,
			MasterAddr: mc.BaseURL(),
			Reachable:  probe.Reachable,
			LatencyMs:  probe.LatencyMs,
			HTTPCode:   probe.HTTPCode,
		}
		if probe.Err != nil {
			payload.Error = probe.Err.Error()
		}
		return jsonResult(payload)
	})
}
