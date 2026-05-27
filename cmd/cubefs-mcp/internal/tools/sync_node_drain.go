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

// registerSyncNodeDrain wires the sync_node_drain tool. Master stops
// dispatching new tasks to the syncnode while letting current tasks
// finish; reverse with sync_node_restore.
func registerSyncNodeDrain(s *server.MCPServer, mc *masterclient.Client) {
	tool := mcp.NewTool(
		"sync_node_drain",
		mcp.WithDescription(
			"MUTATES: Drain a CubeFS syncnode (POST /syncNode/drain?addr=). "+
				"Master stops dispatching new tasks to the node while letting "+
				"current tasks finish. Reverse with sync_node_restore.",
		),
		mcp.WithString("addr",
			mcp.Required(),
			mcp.Description("Syncnode address (host:port) to drain."),
		),
	)

	s.AddTool(tool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		addr, err := req.RequireString("addr")
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		q := url.Values{"addr": {addr}}
		return forwardPost(ctx, mc, "/syncNode/drain", q)
	})
}
