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

// registerSyncRuleUpdate wires the sync_rule_update tool. The body must
// carry the full spec.SyncRule JSON document including its id; master
// performs an upsert keyed on id and returns the updated envelope.
func registerSyncRuleUpdate(s *server.MCPServer, mc *masterclient.Client) {
	tool := mcp.NewTool(
		"sync_rule_update",
		mcp.WithDescription(
			"MUTATES: Update an existing CubeFS sync rule (POST /syncRule/update). "+
				"Body must be the full spec.SyncRule JSON document with the "+
				"id field set; master validates via the spec package.",
		),
		mcp.WithString("body",
			mcp.Required(),
			mcp.Description("Full spec.SyncRule JSON document (id required)."),
		),
	)

	s.AddTool(tool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		body, err := req.RequireString("body")
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		return forwardPostJSONRedacted(ctx, mc, "/syncRule/update", nil, body)
	})
}
