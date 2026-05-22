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

// registerClusterStat wires the cluster_stat tool. Surfaces aggregate
// counters (volume / dp / mp / used+total bytes) that the LLM uses to
// answer capacity questions without needing to enumerate volumes.
func registerClusterStat(s *server.MCPServer, mc *masterclient.Client) {
	tool := mcp.NewTool(
		"cluster_stat",
		mcp.WithDescription(
			"Return aggregate CubeFS cluster statistics via GET /cluster/stat. "+
				"Includes capacity, dp/mp/volume counts and zone summaries. "+
				"No parameters; response forwarded verbatim.",
		),
	)

	s.AddTool(tool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return forwardGet(ctx, mc, "/cluster/stat", nil)
	})
}
