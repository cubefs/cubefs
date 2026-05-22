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

// registerBenchTaskList wires the bench_task_list tool. Both `ruleID` and
// `status` are optional filters; the server ignores empty query keys, so
// we only set them when the caller actually provided a value.
func registerBenchTaskList(s *server.MCPServer, mc *masterclient.Client) {
	tool := mcp.NewTool(
		"bench_task_list",
		mcp.WithDescription(
			"List CubeFS bench tasks via GET /benchTask/list. "+
				"Optional filters: `ruleID` (only tasks derived from one rule), "+
				"`status` (e.g. pending/running/done/failed/cancelled — see master docs). "+
				"Read-only; response forwarded verbatim.",
		),
		mcp.WithString("ruleID",
			mcp.Description("Optional bench rule id filter."),
		),
		mcp.WithString("status",
			mcp.Description("Optional task status filter (master-defined enum)."),
		),
	)

	s.AddTool(tool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		q := url.Values{}
		if v := req.GetString("ruleID", ""); v != "" {
			q.Set("ruleID", v)
		}
		if v := req.GetString("status", ""); v != "" {
			q.Set("status", v)
		}
		return forwardGet(ctx, mc, "/benchTask/list", q)
	})
}
