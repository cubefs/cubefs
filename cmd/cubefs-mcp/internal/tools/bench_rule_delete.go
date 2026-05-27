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

// registerBenchRuleDelete wires the bench_rule_delete tool. Marked
// DESTRUCTIVE because master removes the rule from the in-memory ledger
// without a soft-delete shadow; running tasks derived from the rule are
// not affected but new triggers will fail.
func registerBenchRuleDelete(s *server.MCPServer, mc *masterclient.Client) {
	tool := mcp.NewTool(
		"bench_rule_delete",
		mcp.WithDescription(
			"DESTRUCTIVE: Delete a CubeFS bench rule (POST /benchRule/delete?id=). "+
				"Removal is permanent; in-flight tasks derived from the rule "+
				"continue to completion but future triggers will fail.",
		),
		mcp.WithString("id",
			mcp.Required(),
			mcp.Description("Bench rule id to delete."),
		),
	)

	s.AddTool(tool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		id, err := req.RequireString("id")
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		q := url.Values{"id": {id}}
		return forwardPost(ctx, mc, "/benchRule/delete", q)
	})
}
