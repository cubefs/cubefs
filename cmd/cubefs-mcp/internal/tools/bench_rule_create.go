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

	"github.com/cubefs/cubefs/cmd/cubefs-mcp/internal/masterclient"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

// registerBenchRuleCreate wires the bench_rule_create tool. Master applies
// DisallowUnknownFields on the decoder so any typo in the JSON document
// surfaces as a structured 4xx; we forward it verbatim without local
// re-validation.
func registerBenchRuleCreate(s *server.MCPServer, mc *masterclient.Client) {
	tool := mcp.NewTool(
		"bench_rule_create",
		mcp.WithDescription(
			"MUTATES: Create a CubeFS bench rule (POST /benchRule/create). "+
				"Body must be the full spec.BenchRule JSON document; master "+
				"rejects unknown fields. The created rule envelope is "+
				"returned verbatim.",
		),
		mcp.WithString("body",
			mcp.Required(),
			mcp.Description("Full spec.BenchRule JSON document."),
		),
	)

	s.AddTool(tool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		body, err := req.RequireString("body")
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		return forwardPostJSON(ctx, mc, "/benchRule/create", nil, body)
	})
}
