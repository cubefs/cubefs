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

// registerSyncRuleList wires the sync_rule_list tool. The `state` filter is
// optional (active / paused / degraded) and the parameter is forwarded
// as-is; master owns the enum.
func registerSyncRuleList(s *server.MCPServer, mc *masterclient.Client) {
	tool := mcp.NewTool(
		"sync_rule_list",
		mcp.WithDescription(
			"List CubeFS sync rules via GET /syncRule/list. "+
				"Optional filter: `state` (active|paused|degraded). "+
				"Read-only; response forwarded verbatim.",
		),
		mcp.WithString("state",
			mcp.Description("Optional rule state filter (active|paused|degraded)."),
		),
	)

	s.AddTool(tool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		q := url.Values{}
		if v := req.GetString("state", ""); v != "" {
			q.Set("state", v)
		}
		return forwardGetRedacted(ctx, mc, "/syncRule/list", q)
	})
}
