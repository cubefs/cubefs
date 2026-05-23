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

// registerSyncNodeDecommission wires the sync_node_decommission tool.
// Master drops the syncnode from the dispatch ring; with force=false, any
// running task on the node blocks the call. force=true skips the in-flight
// check and orphans those tasks — they continue to run locally but stop
// reporting progress.
func registerSyncNodeDecommission(s *server.MCPServer, mc *masterclient.Client) {
	tool := mcp.NewTool(
		"sync_node_decommission",
		mcp.WithDescription(
			"DESTRUCTIVE: Decommission a CubeFS syncnode (POST /syncNode/decommission?addr=&force=). "+
				"Drops the node from master's dispatch ring. "+
				"force=true skips the running-task check and orphans in-flight "+
				"tasks (they keep running locally but stop reporting).",
		),
		mcp.WithString("addr",
			mcp.Required(),
			mcp.Description("Syncnode address (host:port) to decommission."),
		),
		mcp.WithString("force",
			mcp.Description("Optional 'true' to skip in-flight task check. Defaults to false."),
		),
	)

	s.AddTool(tool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		addr, err := req.RequireString("addr")
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		q := url.Values{"addr": {addr}}
		if v := req.GetString("force", ""); v != "" {
			q.Set("force", v)
		}
		return forwardPost(ctx, mc, "/syncNode/decommission", q)
	})
}
