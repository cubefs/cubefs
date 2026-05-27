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

// registerClusterHealth wires the cluster_health tool. Backed by the same
// /admin/getCluster endpoint that ping probes, but here we forward the
// full response (master leader, metanode / datanode rosters, vol list).
// ping is for transport sanity; cluster_health is for inspecting the
// detailed topology.
func registerClusterHealth(s *server.MCPServer, mc *masterclient.Client) {
	tool := mcp.NewTool(
		"cluster_health",
		mcp.WithDescription(
			"Return the full CubeFS cluster snapshot via GET /admin/getCluster. "+
				"Master leader, metanode / datanode / object node rosters and "+
				"volume summaries are forwarded verbatim. Use `ping` for "+
				"transport-level reachability only.",
		),
	)

	s.AddTool(tool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return forwardGet(ctx, mc, "/admin/getCluster", nil)
	})
}
