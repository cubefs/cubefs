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

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

// versionInfo captures the static build metadata returned by the version
// tool. Fields default to "dev" / "unknown" so the binary is still useful
// when built without -ldflags injection (e.g. `go run`).
type versionInfo struct {
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	BuildTime string `json:"build_time"`
}

// VersionInfo is the package-level singleton populated by Register via the
// ldflags injected by the build system. Exposed so main.go can override
// defaults from its own ldflags-injected vars if needed; today main passes
// values straight into Register.
var versionDefaults = versionInfo{
	Version:   "dev",
	Commit:    "unknown",
	BuildTime: "unknown",
}

// registerVersion wires the version tool. The triple is captured once at
// startup; the handler is a pure read so it is safe under concurrent calls.
func registerVersion(s *server.MCPServer, info versionInfo) {
	tool := mcp.NewTool(
		"version",
		mcp.WithDescription(
			"Return the cubefs-mcp build metadata (version / commit / build_time). "+
				"Useful for confirming which binary Claude is talking to.",
		),
	)

	s.AddTool(tool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return jsonResult(info)
	})
}
