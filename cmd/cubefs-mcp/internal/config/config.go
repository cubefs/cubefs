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

// Package config carries the runtime configuration loaded from environment
// variables for the cubefs-mcp server. Configuration is intentionally minimal
// at the S1.1 stage: only the master endpoint and an optional auth token.
package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
)

// Environment variable names consumed by cubefs-mcp. Keep them stable: they
// are part of the public deploy contract used by Claude Desktop / Code stdio
// integrations (see plan doc §3 "MCP - 工具范围").
const (
	EnvMasterAddr = "CUBEFS_MASTER_ADDR"
	EnvAuthToken  = "CUBEFS_AUTH_TOKEN"
)

// Config is immutable after Load returns; pass by value to tool handlers.
type Config struct {
	// MasterAddr is the full base URL of the cubefs master REST endpoint,
	// e.g. "http://master.cfs.svc:17010". No trailing slash.
	MasterAddr string

	// AuthToken is optional. When non-empty it is sent as an Authorization
	// bearer header on every master HTTP request.
	AuthToken string
}

// Load reads the environment, validates required fields, and returns the
// resulting Config. Returns an error suitable for direct logging on startup.
func Load() (Config, error) {
	addr := strings.TrimSpace(os.Getenv(EnvMasterAddr))
	if addr == "" {
		return Config{}, fmt.Errorf("%s is required (e.g. http://master.cfs.svc:17010)", EnvMasterAddr)
	}
	if err := validateMasterAddr(addr); err != nil {
		return Config{}, fmt.Errorf("%s invalid: %w", EnvMasterAddr, err)
	}
	addr = strings.TrimRight(addr, "/")

	return Config{
		MasterAddr: addr,
		AuthToken:  strings.TrimSpace(os.Getenv(EnvAuthToken)),
	}, nil
}

// validateMasterAddr enforces that the value parses as an absolute http(s)
// URL with a host component. This catches the common mistake of passing
// "master.cfs.svc:17010" without scheme.
func validateMasterAddr(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return err
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return errors.New("must start with http:// or https://")
	}
	if u.Host == "" {
		return errors.New("missing host")
	}
	return nil
}
