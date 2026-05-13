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

package syncnode

import (
	"regexp"

	"github.com/cubefs/cubefs/proto"
)

// Module / role identity.
const (
	// ModuleName is the role string used by cfs-server -c sync.json + Master
	// registration. Intentionally short ("sync", not "syncnode"); see design
	// doc §3.2.
	ModuleName = "sync"

	// RoleName is an alias kept for consistency with other roles' naming.
	RoleName = ModuleName
)

// Config key strings used by parseConfig / config.Config.Get*.
const (
	configListen       = proto.ListenPort
	configMasterAddr   = proto.MasterAddr
	configHTTPListen   = "httpListen"
	configLogDir       = "logDir"
	configLogLevel     = "logLevel"
	configDataDir      = "dataDir"
	configWarnLogDir   = "warnLogDir"
	configExporterPort = "exporterPort"

	configS3Defaults  = "s3Defaults"
	configPosix       = "posix"
	configConcurrency = "concurrency"
	configRules       = "rules"
)

// Default values.
const (
	defaultListen             = "17710"
	defaultHTTPListen         = "17711"
	defaultExporterPort       = 17712
	defaultMaxConcurrentTasks = 8
	defaultTransfersPerTask   = 4
	defaultBandwidthLimitMBps = 0 // 0 = unlimited
	defaultMaxDirDepth        = 20
	defaultBufferSizeKiB      = 4096
	defaultLogLevel           = "info"

	heartbeatInterval         = 10 // seconds
	metricsRefreshInterval    = 10 // seconds
)

// Allowed enum values for rule fields.
var (
	validRuleTypes        = map[string]bool{"sync": true, "load": true, "check": true}
	validBackendKinds     = map[string]bool{"cfs": true, "s3": true, "local": true}
	validAfterCopy        = map[string]bool{"": true, "keep": true, "verify_then_delete_src": true}
	validDownloadStrategy = map[string]bool{"": true, "temp_rename": true, "direct": true}
	validOnMismatch       = map[string]bool{"": true, "alert": true, "auto_fix": true, "ignore": true}
)

// regexpListen matches the listen-port string ("digits only").
var regexpListen = regexp.MustCompile(`^\d+$`)

// regexpSize matches "<N><unit>" where unit ∈ {KB, MB, GB, KiB, MiB, GiB}.
var regexpSize = regexp.MustCompile(`^\d+(KB|MB|GB|KiB|MiB|GiB|B)$`)

// regexpDuration matches "<N><unit>" where unit ∈ {s, m, h, d, w}.
var regexpDuration = regexp.MustCompile(`^\d+(s|m|h|d|w)$`)
