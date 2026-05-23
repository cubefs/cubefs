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

// registerSyncTaskRetry wires the sync_task_retry tool. Master rejects
// retries on tasks still in a running / queued state; only terminal
// (failed / cancelled) tasks are eligible.
func registerSyncTaskRetry(s *server.MCPServer, mc *masterclient.Client) {
	tool := mcp.NewTool(
		"sync_task_retry",
		mcp.WithDescription(
			"MUTATES: Retry a CubeFS sync task (POST /syncTask/retry?id=). "+
				"Only terminal (failed / cancelled) tasks are eligible; "+
				"master rejects retries on running / queued tasks.",
		),
		mcp.WithString("id",
			mcp.Required(),
			mcp.Description("Sync task id to retry."),
		),
	)

	s.AddTool(tool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		id, err := req.RequireString("id")
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		q := url.Values{"id": {id}}
		return forwardPost(ctx, mc, "/syncTask/retry", q)
	})
}
