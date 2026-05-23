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

// registerSyncNodeRestore wires the sync_node_restore tool. Master flips
// the syncnode back to a dispatch-eligible state, reversing a previous
// drain.
func registerSyncNodeRestore(s *server.MCPServer, mc *masterclient.Client) {
	tool := mcp.NewTool(
		"sync_node_restore",
		mcp.WithDescription(
			"MUTATES: Restore a drained CubeFS syncnode (POST /syncNode/restore?addr=). "+
				"Re-enables dispatch to the node, reversing a previous "+
				"sync_node_drain.",
		),
		mcp.WithString("addr",
			mcp.Required(),
			mcp.Description("Syncnode address (host:port) to restore."),
		),
	)

	s.AddTool(tool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		addr, err := req.RequireString("addr")
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		q := url.Values{"addr": {addr}}
		return forwardPost(ctx, mc, "/syncNode/restore", q)
	})
}
