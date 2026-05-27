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

// registerBenchRuleList wires the bench_rule_list tool. The optional `id`
// filter is forwarded as a query param so master decides the semantics
// (returning a single rule when set, all rules otherwise); we do not
// re-shape the response.
func registerBenchRuleList(s *server.MCPServer, mc *masterclient.Client) {
	tool := mcp.NewTool(
		"bench_rule_list",
		mcp.WithDescription(
			"List CubeFS bench rules via GET /benchRule/list. "+
				"When `id` is supplied master returns only the matching rule. "+
				"Read-only; the response JSON is forwarded verbatim.",
		),
		mcp.WithString("id",
			mcp.Description("Optional rule id filter. Omit to list all rules."),
		),
	)

	s.AddTool(tool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		q := url.Values{}
		if id := req.GetString("id", ""); id != "" {
			q.Set("id", id)
		}
		return forwardGet(ctx, mc, "/benchRule/list", q)
	})
}
