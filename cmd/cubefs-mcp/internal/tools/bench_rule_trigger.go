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
	"net/url"

	"github.com/cubefs/cubefs/cmd/cubefs-mcp/internal/masterclient"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

// registerBenchRuleTrigger wires the bench_rule_trigger tool. This is the
// only bench-rule write surface exposed to the LLM: it asks master to
// materialise a new bench_task from an existing rule. Master decides
// dispatch / scheduling; we forward whatever envelope it returns.
func registerBenchRuleTrigger(s *server.MCPServer, mc *masterclient.Client) {
	tool := mcp.NewTool(
		"bench_rule_trigger",
		mcp.WithDescription(
			"Trigger a CubeFS bench rule (POST /benchRule/trigger). "+
				"Master enqueues a new bench_task derived from the rule. "+
				"The created task's metadata is returned verbatim.",
		),
		mcp.WithString("id",
			mcp.Required(),
			mcp.Description("Bench rule id to trigger."),
		),
	)

	s.AddTool(tool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		id, err := req.RequireString("id")
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		q := url.Values{"id": {id}}
		return forwardPost(ctx, mc, "/benchRule/trigger", q)
	})
}
