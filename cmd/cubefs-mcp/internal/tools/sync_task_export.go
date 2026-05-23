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

// registerSyncTaskExport wires the sync_task_export tool. Master streams
// the response as NDJSON (one task envelope per line), so we forward the
// body verbatim via forwardGetText without the json.Valid() gate. The
// masterclient currently buffers the full body — acceptable for the
// expected scale (10k tasks ≈ a few MB).
func registerSyncTaskExport(s *server.MCPServer, mc *masterclient.Client) {
	tool := mcp.NewTool(
		"sync_task_export",
		mcp.WithDescription(
			"Export CubeFS sync tasks as NDJSON via GET /syncTask/export. "+
				"Optional `since` (RFC3339) filters by last-update timestamp. "+
				"Each line is one task envelope; the body is forwarded as "+
				"text since NDJSON is not single-document JSON.",
		),
		mcp.WithString("since",
			mcp.Description("Optional RFC3339 timestamp; only tasks updated at or after this point are returned."),
		),
	)

	s.AddTool(tool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		q := url.Values{}
		if v := req.GetString("since", ""); v != "" {
			q.Set("since", v)
		}
		return forwardGetText(ctx, mc, "/syncTask/export", q)
	})
}
