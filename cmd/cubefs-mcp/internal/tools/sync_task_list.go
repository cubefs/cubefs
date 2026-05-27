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

// registerSyncTaskList wires the sync_task_list tool. All three filters
// (status / ruleID / owner) are optional and server-defined; the tool
// stays a thin pass-through so master can evolve the filter set without a
// matching schema bump here.
func registerSyncTaskList(s *server.MCPServer, mc *masterclient.Client) {
	tool := mcp.NewTool(
		"sync_task_list",
		mcp.WithDescription(
			"List CubeFS sync tasks via GET /syncTask/list. "+
				"Optional filters: `status`, `ruleID`, `owner`. "+
				"Read-only; response forwarded verbatim.",
		),
		mcp.WithString("status",
			mcp.Description("Optional task status filter (master-defined enum)."),
		),
		mcp.WithString("ruleID",
			mcp.Description("Optional sync rule id filter."),
		),
		mcp.WithString("owner",
			mcp.Description("Optional owner / tenant filter."),
		),
	)

	s.AddTool(tool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		q := url.Values{}
		if v := req.GetString("status", ""); v != "" {
			q.Set("status", v)
		}
		if v := req.GetString("ruleID", ""); v != "" {
			q.Set("ruleID", v)
		}
		if v := req.GetString("owner", ""); v != "" {
			q.Set("owner", v)
		}
		return forwardGet(ctx, mc, "/syncTask/list", q)
	})
}
