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

// registerSyncTaskGet wires the sync_task_get tool.
func registerSyncTaskGet(s *server.MCPServer, mc *masterclient.Client) {
	tool := mcp.NewTool(
		"sync_task_get",
		mcp.WithDescription(
			"Fetch a single CubeFS sync task by id via GET /syncTask/get. "+
				"Returns the full task envelope (status, src/dst, progress). "+
				"Read-only.",
		),
		mcp.WithString("id",
			mcp.Required(),
			mcp.Description("Sync task id."),
		),
	)

	s.AddTool(tool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		id, err := req.RequireString("id")
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		q := url.Values{"id": {id}}
		return forwardGet(ctx, mc, "/syncTask/get", q)
	})
}
