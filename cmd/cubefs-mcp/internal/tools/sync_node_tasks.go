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

// registerSyncNodeTasks wires the sync_node_tasks tool. Pulls the tasks
// currently bound to a syncnode from master's dispatch ledger; this is the
// canonical way to investigate "which tasks would survive draining this
// node" without scanning the full sync task list.
func registerSyncNodeTasks(s *server.MCPServer, mc *masterclient.Client) {
	tool := mcp.NewTool(
		"sync_node_tasks",
		mcp.WithDescription(
			"List tasks currently bound to a CubeFS syncnode via GET /syncNode/tasks. "+
				"Required `addr`; optional `status` filter (master-defined enum). "+
				"Read-only.",
		),
		mcp.WithString("addr",
			mcp.Required(),
			mcp.Description("Syncnode address (host:port)."),
		),
		mcp.WithString("status",
			mcp.Description("Optional task status filter (master-defined enum)."),
		),
	)

	s.AddTool(tool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		addr, err := req.RequireString("addr")
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		q := url.Values{"addr": {addr}}
		if v := req.GetString("status", ""); v != "" {
			q.Set("status", v)
		}
		return forwardGet(ctx, mc, "/syncNode/tasks", q)
	})
}
