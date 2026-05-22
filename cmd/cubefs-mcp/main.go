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

// cubefs-mcp is a stdio MCP server that exposes CubeFS bench / sync /
// cluster operations to LLM clients (Claude Desktop, Claude Code, ...).
//
// At the S1.1 milestone this binary only ships the link-validation pair
// (ping, version); bench / sync / cluster tools land in follow-up tasks
// without changing main.go: new tools attach themselves via
// internal/tools.Register.
//
// Configuration:
//
//	CUBEFS_MASTER_ADDR  required, e.g. http://master.cfs.svc:17010
//	CUBEFS_AUTH_TOKEN   optional bearer token for master REST calls
//
// Runtime contract:
//
//   - stdout/stdin carry the MCP JSON-RPC framing; do NOT write logs to
//     stdout under any circumstance.
//   - stderr is used for human-readable diagnostics (startup, fatal errors).
package main

import (
	"fmt"
	"os"

	"github.com/cubefs/cubefs/cmd/cubefs-mcp/internal/config"
	"github.com/cubefs/cubefs/cmd/cubefs-mcp/internal/masterclient"
	"github.com/cubefs/cubefs/cmd/cubefs-mcp/internal/tools"
	"github.com/mark3labs/mcp-go/server"
)

// Build metadata injected via -ldflags at build time, e.g.
//
//	go build -ldflags "-X main.version=v0.1.0 -X main.commit=$(git rev-parse --short HEAD) \
//	                   -X main.buildTime=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
//	         ./cmd/cubefs-mcp
//
// Defaults make `go run` / `go build` without ldflags still produce a
// runnable, self-describing binary.
var (
	version   = "dev"
	commit    = "unknown"
	buildTime = "unknown"
)

// serverName / serverVersion are the values surfaced by the MCP `initialize`
// handshake; clients (Claude Desktop) display these to the user.
const (
	serverName    = "cubefs-mcp"
	serverVersion = "0.1.0"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "cubefs-mcp: %v\n", err)
		os.Exit(1)
	}
}

// run holds the real entry-point so main() stays defer/return-friendly and
// every error path goes through the same stderr formatter + non-zero exit.
func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	mc := masterclient.New(cfg.MasterAddr, cfg.AuthToken)

	srv := server.NewMCPServer(
		serverName,
		serverVersion,
		server.WithToolCapabilities(false),
	)

	tools.Register(srv, mc, tools.BuildInfo{
		Version:   version,
		Commit:    commit,
		BuildTime: buildTime,
	})

	// stderr-only startup banner. Stdout is reserved for the MCP JSON-RPC
	// frame and must never carry log lines.
	fmt.Fprintf(os.Stderr,
		"cubefs-mcp %s (commit=%s built=%s) ready, master=%s\n",
		version, commit, buildTime, cfg.MasterAddr,
	)

	// ServeStdio blocks until stdin is closed or an unrecoverable transport
	// error occurs.
	if err := server.ServeStdio(srv); err != nil {
		return fmt.Errorf("stdio server: %w", err)
	}
	return nil
}
