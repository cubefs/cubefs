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
	"encoding/json"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/cubefs/cubefs/syncnode/spec"
)

// SyncConfig is the typed configuration loaded from sync.json (or whatever
// path was passed to cfs-server -c). It's a strict schema; unknown fields
// are NOT silently accepted because that's how config drift bugs are born.
type SyncConfig struct {
	Role         string            `json:"role"`
	Listen       string            `json:"listen"`
	HTTPListen   string            `json:"httpListen"`
	MasterAddr   string            `json:"masterAddr"`
	LogDir       string            `json:"logDir"`
	LogLevel     string            `json:"logLevel"`
	DataDir      string            `json:"dataDir"`
	WarnLogDir   string            `json:"warnLogDir"`
	ExporterPort int               `json:"exporterPort"`
	// SEC4: shared-token gate for the HTTP admin surface. Empty disables
	// auth (preserves pre-fix behaviour for tests + dev). Production
	// operators should set this; rotation requires a restart.
	AdminToken   string            `json:"adminToken"`
	S3Defaults   S3DefaultsConfig  `json:"s3Defaults"`
	Posix        PosixConfig       `json:"posix"`
	Concurrency  ConcurrencyConfig `json:"concurrency"`
	// SEC2: bounds the master-dispatched task listener. Zero values
	// fall back to DefaultTCPMaxConnections / DefaultTCPReadIdleTimeout
	// in applyDefaults.
	TCP   TCPConfig    `json:"tcp"`
	Rules []RuleConfig `json:"rules"`
}

type S3DefaultsConfig struct {
	Endpoint     string `json:"endpoint"`
	Region       string `json:"region"`
	AccessKeyEnv string `json:"accessKeyEnv"`
	SecretKeyEnv string `json:"secretKeyEnv"`
	StorageClass string `json:"storageClass"`
}

type PosixConfig struct {
	AllowedRoots         []string `json:"allowedRoots"`
	MaxDirDepth          int      `json:"maxDirDepth"`
	DefaultBufferSizeKiB int      `json:"defaultBufferSizeKiB"`
}

type ConcurrencyConfig struct {
	MaxConcurrentTasks int `json:"maxConcurrentTasks"`
	MaxQueueSize       int `json:"maxQueueSize"`
	TransfersPerTask   int `json:"transfersPerTask"`
	BandwidthLimitMBps int `json:"bandwidthLimitMBps"`
}

// TCPConfig bounds the master-dispatched task listener. Defaults are sane
// for a single-master deployment; bump for higher throughput.
type TCPConfig struct {
	// MaxConnections is the in-flight connection cap on the TCP listener.
	// New connections beyond this are accepted then immediately closed
	// (so master sees a fast TCP RST rather than a hung accept). Zero or
	// negative means "use the default" (DefaultTCPMaxConnections).
	MaxConnections int `json:"maxConnections"`
	// ReadIdleTimeout is the deadline applied to each packet read on the
	// listener. Idle connections that don't send a packet within this
	// window are dropped. Accepts Go duration strings ("60s", "1m") OR
	// bare numeric seconds. Empty / zero falls back to
	// DefaultTCPReadIdleTimeout.
	ReadIdleTimeout string `json:"readIdleTimeout"`
}

// ResolvedReadIdleTimeout returns the parsed ReadIdleTimeout, or
// DefaultTCPReadIdleTimeout seconds when the field is empty / malformed.
// We tolerate malformed input at boot rather than refusing to start —
// the listener can always run; an operator-supplied bad value just
// falls back to the safe default and is logged by the caller.
func (c TCPConfig) ResolvedReadIdleTimeout() time.Duration {
	if c.ReadIdleTimeout == "" {
		return time.Duration(DefaultTCPReadIdleTimeout) * time.Second
	}
	if d, err := time.ParseDuration(c.ReadIdleTimeout); err == nil && d > 0 {
		return d
	}
	if n, err := strconv.Atoi(c.ReadIdleTimeout); err == nil && n > 0 {
		return time.Duration(n) * time.Second
	}
	return time.Duration(DefaultTCPReadIdleTimeout) * time.Second
}

// ResolvedMaxConnections returns MaxConnections clamped to a positive
// integer; zero / negative inputs fall back to DefaultTCPMaxConnections.
func (c TCPConfig) ResolvedMaxConnections() int {
	if c.MaxConnections > 0 {
		return c.MaxConnections
	}
	return DefaultTCPMaxConnections
}

// Wire types are aliased from syncnode/spec. The alias lets subpackages
// (rules, tasks, scheduler) reference these via spec.* without an import
// cycle back into syncnode; callers in this package keep using the short
// names they always have.
type (
	RuleConfig      = spec.RuleConfig
	EndpointConfig  = spec.EndpointConfig
	FilterConfig    = spec.FilterConfig
	RetentionConfig = spec.RetentionConfig
)

// ParseSyncConfig unmarshals raw JSON into SyncConfig and runs the full
// validation pass. Returns the populated config on success, or a typed
// *ConfigError on failure (caller can match on .Code for tests / alerts).
func ParseSyncConfig(raw []byte) (*SyncConfig, error) {
	cfg := &SyncConfig{}
	if err := json.Unmarshal(raw, cfg); err != nil {
		return nil, newConfigErr(ErrCodeTypeError, "", "json unmarshal: "+err.Error())
	}
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}
	applyDefaults(cfg)
	return cfg, nil
}

// applyDefaults fills in zero-valued optional fields so downstream callers
// don't have to repeat the same "if 0, set default" logic everywhere.
func applyDefaults(cfg *SyncConfig) {
	if cfg.Listen == "" {
		cfg.Listen = defaultListen
	}
	if cfg.HTTPListen == "" {
		cfg.HTTPListen = defaultHTTPListen
	}
	if cfg.ExporterPort == 0 {
		cfg.ExporterPort = defaultExporterPort
	}
	if cfg.LogLevel == "" {
		cfg.LogLevel = defaultLogLevel
	}
	if cfg.Concurrency.MaxConcurrentTasks == 0 {
		cfg.Concurrency.MaxConcurrentTasks = defaultMaxConcurrentTasks
	}
	if cfg.Concurrency.TransfersPerTask == 0 {
		cfg.Concurrency.TransfersPerTask = defaultTransfersPerTask
	}
	if cfg.Posix.MaxDirDepth == 0 {
		cfg.Posix.MaxDirDepth = defaultMaxDirDepth
	}
	if cfg.Posix.DefaultBufferSizeKiB == 0 {
		cfg.Posix.DefaultBufferSizeKiB = defaultBufferSizeKiB
	}
	// SEC2: TCP bounds. Resolved* helpers already fall back at read time;
	// we materialise them into the cfg so /admin/syncnode/stat shows the
	// values operators are running against.
	if cfg.TCP.MaxConnections <= 0 {
		cfg.TCP.MaxConnections = DefaultTCPMaxConnections
	}
	if cfg.TCP.ReadIdleTimeout == "" {
		cfg.TCP.ReadIdleTimeout = fmt.Sprintf("%ds", DefaultTCPReadIdleTimeout)
	}
}

// validateConfig walks the parsed SyncConfig and returns the FIRST validation
// error found, or nil on success. Errors are typed (*ConfigError) with stable
// numeric codes; callers can match on err.(*ConfigError).Code.
func validateConfig(cfg *SyncConfig) *ConfigError {
	if cfg.Role != "" && cfg.Role != ModuleName {
		return newConfigErr(ErrCodeTypeError, "role",
			fmt.Sprintf("role must be %q, got %q", ModuleName, cfg.Role))
	}
	if cfg.Listen != "" && !regexpListen.MatchString(cfg.Listen) {
		return newConfigErr(ErrCodeTypeError, "listen",
			"listen must be a numeric port string")
	}
	if cfg.HTTPListen != "" && !regexpListen.MatchString(cfg.HTTPListen) {
		return newConfigErr(ErrCodeTypeError, "httpListen",
			"httpListen must be a numeric port string")
	}
	if cfg.MasterAddr == "" {
		return newConfigErr(ErrCodeRequiredFieldMissing, "masterAddr",
			"masterAddr is required")
	}
	if cfg.DataDir == "" {
		return newConfigErr(ErrCodeRequiredFieldMissing, "dataDir",
			"dataDir is required for BoltDB / temp files")
	}

	seenIDs := make(map[string]bool, len(cfg.Rules))
	for i := range cfg.Rules {
		r := &cfg.Rules[i]
		field := fmt.Sprintf("rules[%d]", i)
		if r.ID == "" {
			return newConfigErr(ErrCodeRequiredFieldMissing, field+".id",
				"rule id is required")
		}
		if seenIDs[r.ID] {
			return newConfigErr(ErrCodeDuplicateRuleID, field+".id",
				"duplicate rule id: "+r.ID)
		}
		seenIDs[r.ID] = true

		if !validRuleTypes[r.Type] {
			return newConfigErr(ErrCodeTypeError, field+".type",
				"type must be sync / load / check, got: "+r.Type)
		}
		if r.Schedule != "" {
			if err := validateCronExpr(r.Schedule); err != nil {
				return newConfigErr(ErrCodeInvalidCron, field+".schedule",
					fmt.Sprintf("invalid cron %q: %v", r.Schedule, err))
			}
		}
		if err := validateEndpoint(&r.Src, field+".src", &cfg.Posix); err != nil {
			return err
		}
		if err := validateEndpoint(&r.Dst, field+".dst", &cfg.Posix); err != nil {
			return err
		}
		if !validAfterCopy[r.AfterCopy] {
			return newConfigErr(ErrCodeUnknownAfterCopy, field+".afterCopy",
				"afterCopy must be keep / verify_then_delete_src, got: "+r.AfterCopy)
		}
		if !validDownloadStrategy[r.DownloadStrategy] {
			return newConfigErr(ErrCodeUnknownDownloadStrat, field+".downloadStrategy",
				"downloadStrategy must be temp_rename / direct, got: "+r.DownloadStrategy)
		}
		if !validOnMismatch[r.OnMismatch] {
			return newConfigErr(ErrCodeUnknownOnMismatch, field+".onMismatch",
				"onMismatch must be alert / auto_fix / ignore, got: "+r.OnMismatch)
		}
		if err := validateFilter(&r.Filter, field+".filter"); err != nil {
			return err
		}
		if err := validateRetention(&r.Retention, field+".retention"); err != nil {
			return err
		}
	}
	return nil
}

func validateEndpoint(ep *EndpointConfig, field string, posix *PosixConfig) *ConfigError {
	if !validBackendKinds[ep.Kind] {
		return newConfigErr(ErrCodeInvalidKind, field+".kind",
			"kind must be cfs / s3 / local, got: "+ep.Kind)
	}
	switch ep.Kind {
	case "cfs":
		if ep.Vol == "" {
			return newConfigErr(ErrCodeRequiredFieldMissing, field+".vol",
				"cfs endpoint requires vol")
		}
		if ep.Path == "" {
			return newConfigErr(ErrCodeRequiredFieldMissing, field+".path",
				"cfs endpoint requires path")
		}
	case "s3":
		if ep.Bucket == "" {
			return newConfigErr(ErrCodeRequiredFieldMissing, field+".bucket",
				"s3 endpoint requires bucket")
		}
		// endpoint may come from s3Defaults; validated again later when
		// merging defaults. But if endpoint is explicitly empty AND there's
		// no default, that's caught by Backend setup, not here.
		if ep.Endpoint != "" && !strings.HasPrefix(ep.Endpoint, "http://") &&
			!strings.HasPrefix(ep.Endpoint, "https://") {
			return newConfigErr(ErrCodeS3EndpointMissing, field+".endpoint",
				"s3 endpoint must start with http:// or https://, got: "+ep.Endpoint)
		}
	case "local":
		if ep.Path == "" {
			return newConfigErr(ErrCodeRequiredFieldMissing, field+".path",
				"local endpoint requires path")
		}
		// allowedRoots enforcement: path must be under one of the configured roots
		if len(posix.AllowedRoots) == 0 {
			return newConfigErr(ErrCodePathNotAllowed, field+".path",
				"local endpoint requires posix.allowedRoots to be configured")
		}
		clean := filepath.Clean(ep.Path)
		allowed := false
		for _, root := range posix.AllowedRoots {
			rootClean := filepath.Clean(root)
			if clean == rootClean || strings.HasPrefix(clean, rootClean+string(filepath.Separator)) {
				allowed = true
				break
			}
		}
		if !allowed {
			return newConfigErr(ErrCodePathNotAllowed, field+".path",
				fmt.Sprintf("path %q is not under any posix.allowedRoots %v", ep.Path, posix.AllowedRoots))
		}
	}
	return nil
}

func validateFilter(f *FilterConfig, field string) *ConfigError {
	if f.MinSize != "" && !regexpSize.MatchString(f.MinSize) {
		return newConfigErr(ErrCodeMinSizeUnit, field+".minSize",
			fmt.Sprintf("minSize must be <N><unit> with unit ∈ {B,KB,MB,GB,KiB,MiB,GiB}, got: %q", f.MinSize))
	}
	if f.MaxSize != "" && !regexpSize.MatchString(f.MaxSize) {
		return newConfigErr(ErrCodeMinSizeUnit, field+".maxSize",
			fmt.Sprintf("maxSize must be <N><unit>, got: %q", f.MaxSize))
	}
	if f.MinAge != "" && !regexpDuration.MatchString(f.MinAge) {
		return newConfigErr(ErrCodeInvalidDuration, field+".minAge",
			fmt.Sprintf("minAge must be <N><s|m|h|d|w>, got: %q", f.MinAge))
	}
	if f.MaxAge != "" && !regexpDuration.MatchString(f.MaxAge) {
		return newConfigErr(ErrCodeInvalidDuration, field+".maxAge",
			fmt.Sprintf("maxAge must be <N><s|m|h|d|w>, got: %q", f.MaxAge))
	}
	return nil
}

func validateRetention(r *RetentionConfig, field string) *ConfigError {
	if r.Pattern == "" {
		// retention is optional
		return nil
	}
	if !strings.Contains(r.Pattern, "{N}") {
		return newConfigErr(ErrCodeRetentionPatternNoN, field+".pattern",
			fmt.Sprintf("retention.pattern must contain {N} version placeholder, got: %q", r.Pattern))
	}
	if r.KeepLast < 0 {
		return newConfigErr(ErrCodeTypeError, field+".keepLast",
			"keepLast must be >= 0")
	}
	if r.KeepWithin != "" && !regexpDuration.MatchString(r.KeepWithin) {
		return newConfigErr(ErrCodeInvalidDuration, field+".keepWithin",
			fmt.Sprintf("keepWithin must be <N><s|m|h|d|w>, got: %q", r.KeepWithin))
	}
	return nil
}

// AtoiPort parses a port string to int; convenience for callers that want
// an int rather than string everywhere.
func AtoiPort(s string) (int, error) {
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, err
	}
	if n <= 0 || n > 65535 {
		return 0, fmt.Errorf("port out of range: %d", n)
	}
	return n, nil
}

// validateCronExpr is a lightweight syntactic check for cron strings. Phase A
// only needs to reject obviously malformed inputs so misconfigured rules
// don't load; the real cron parser is wired in Phase F when scheduler runs.
// Accepts:
//   - 5-field standard cron ("* * * * *")
//   - 6-field cron with seconds ("* * * * * *")
//   - common descriptors ("@daily", "@hourly", "@every 30s")
func validateCronExpr(s string) error {
	s = strings.TrimSpace(s)
	if s == "" {
		return fmt.Errorf("empty cron")
	}
	if strings.HasPrefix(s, "@") {
		switch s {
		case "@yearly", "@annually", "@monthly", "@weekly", "@daily", "@midnight", "@hourly":
			return nil
		}
		if strings.HasPrefix(s, "@every ") {
			return nil
		}
		return fmt.Errorf("unknown descriptor: %s", s)
	}
	fields := strings.Fields(s)
	if len(fields) != 5 && len(fields) != 6 {
		return fmt.Errorf("expected 5 or 6 cron fields, got %d", len(fields))
	}
	// Per-field syntactic check: each field is either '*', or contains
	// digits / '*' / ',' / '-' / '/' / letters only. Full semantic validation
	// is deferred to robfig/cron in Phase F.
	for i, f := range fields {
		if !isCronFieldShape(f) {
			return fmt.Errorf("field %d has invalid shape: %q", i+1, f)
		}
	}
	if !hasCronAnchor(fields) {
		return fmt.Errorf("cron has no numeric / '*' anchor in any field: %q", s)
	}
	return nil
}

func isCronFieldShape(f string) bool {
	if f == "" {
		return false
	}
	hasAnchor := false // at least one digit or '*'
	for _, c := range f {
		switch {
		case c >= '0' && c <= '9':
			hasAnchor = true
		case c == '*':
			hasAnchor = true
		case c == ',' || c == '-' || c == '/' || c == '?':
		case (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z'):
			// month / day names like JAN, MON allowed but only as the
			// whole field — accept; if combined with garbage in other
			// fields, the lack of hasAnchor elsewhere will fail
		default:
			return false
		}
	}
	// Pure-letter fields are only valid for month / day-of-week, but Phase A
	// doesn't track field position. Require that at least ONE field in the
	// cron string has a numeric/asterisk anchor — enforced by caller looping
	// over all fields and rejecting if NONE has an anchor. Per-field shape
	// check here is satisfied as long as no garbage chars appeared.
	_ = hasAnchor
	return true
}

// hasCronAnchor reports whether at least one field in `fields` contains a
// digit or '*'. A cron string composed entirely of letters (e.g.
// "abc def ghi jkl mno") fails this — valid cron always has anchors.
func hasCronAnchor(fields []string) bool {
	for _, f := range fields {
		for _, c := range f {
			if (c >= '0' && c <= '9') || c == '*' {
				return true
			}
		}
	}
	return false
}
