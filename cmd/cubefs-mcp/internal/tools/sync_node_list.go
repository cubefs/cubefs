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

// registerSyncNodeList wires the sync_node_list tool. No parameters: the
// endpoint always returns the full fleet of syncnode DaemonSet workers so
// the LLM can see which pods are currently registered, their heartbeat
// state and queue depth.
func registerSyncNodeList(s *server.MCPServer, mc *masterclient.Client) {
	tool := mcp.NewTool(
		"sync_node_list",
		mcp.WithDescription(
			"List all registered CubeFS sync nodes via GET /syncNode/list. "+
				"Useful for inspecting which syncnode pods master currently "+
				"considers healthy. No parameters.",
		),
	)

	s.AddTool(tool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return forwardGet(ctx, mc, "/syncNode/list", nil)
	})
}
