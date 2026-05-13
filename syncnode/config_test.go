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
	"errors"
	"strings"
	"testing"
)

// TestValidateConfig_Negatives covers the 8 mandatory error-code paths from
// §9 Phase A-2 AC. Each case asserts on the typed ConfigError.Code so callers
// (tests, operator dashboards, error pages) get a stable identifier.
func TestValidateConfig_Negatives(t *testing.T) {
	cases := []struct {
		name     string
		raw      string
		wantCode int
		msgHas   string // substring expected in err.Msg (case-insensitive)
	}{
		{
			name: "invalid cron expression",
			raw: `{
				"masterAddr": "127.0.0.1:17010",
				"dataDir": "/tmp/d",
				"rules": [{
					"id": "r1", "type": "sync",
					"schedule": "not a cron",
					"src": {"kind": "cfs", "vol": "v", "path": "/p"},
					"dst": {"kind": "s3", "bucket": "b"}
				}]
			}`,
			wantCode: ErrCodeInvalidCron,
			msgHas:   "cron",
		},
		{
			name: "invalid kind",
			raw: `{
				"masterAddr": "127.0.0.1:17010",
				"dataDir": "/tmp/d",
				"rules": [{
					"id": "r1", "type": "sync",
					"src": {"kind": "ftp", "path": "/p"},
					"dst": {"kind": "s3", "bucket": "b"}
				}]
			}`,
			wantCode: ErrCodeInvalidKind,
			msgHas:   "kind",
		},
		{
			name: "path outside allowedRoots",
			raw: `{
				"masterAddr": "127.0.0.1:17010",
				"dataDir": "/tmp/d",
				"posix": {"allowedRoots": ["/mnt/gpfs"]},
				"rules": [{
					"id": "r1", "type": "sync",
					"src": {"kind": "local", "path": "/etc/passwd"},
					"dst": {"kind": "s3", "bucket": "b"}
				}]
			}`,
			wantCode: ErrCodePathNotAllowed,
			msgHas:   "allowedroots",
		},
		{
			name: "s3 endpoint malformed",
			raw: `{
				"masterAddr": "127.0.0.1:17010",
				"dataDir": "/tmp/d",
				"rules": [{
					"id": "r1", "type": "sync",
					"src": {"kind": "cfs", "vol": "v", "path": "/p"},
					"dst": {"kind": "s3", "bucket": "b", "endpoint": "tcp://wrong"}
				}]
			}`,
			wantCode: ErrCodeS3EndpointMissing,
			msgHas:   "endpoint",
		},
		{
			name: "retention pattern without {N}",
			raw: `{
				"masterAddr": "127.0.0.1:17010",
				"dataDir": "/tmp/d",
				"rules": [{
					"id": "r1", "type": "sync",
					"src": {"kind": "cfs", "vol": "v", "path": "/p"},
					"dst": {"kind": "s3", "bucket": "b"},
					"retention": {"pattern": "model-step.pt", "keepLast": 5}
				}]
			}`,
			wantCode: ErrCodeRetentionPatternNoN,
			msgHas:   "{n}",
		},
		{
			name: "minSize unit error",
			raw: `{
				"masterAddr": "127.0.0.1:17010",
				"dataDir": "/tmp/d",
				"rules": [{
					"id": "r1", "type": "sync",
					"src": {"kind": "cfs", "vol": "v", "path": "/p"},
					"dst": {"kind": "s3", "bucket": "b"},
					"filter": {"minSize": "1Megabyte"}
				}]
			}`,
			wantCode: ErrCodeMinSizeUnit,
			msgHas:   "minsize",
		},
		{
			name: "required field missing (rule id)",
			raw: `{
				"masterAddr": "127.0.0.1:17010",
				"dataDir": "/tmp/d",
				"rules": [{
					"type": "sync",
					"src": {"kind": "cfs", "vol": "v", "path": "/p"},
					"dst": {"kind": "s3", "bucket": "b"}
				}]
			}`,
			wantCode: ErrCodeRequiredFieldMissing,
			msgHas:   "id",
		},
		{
			name: "type field wrong value",
			raw: `{
				"masterAddr": "127.0.0.1:17010",
				"dataDir": "/tmp/d",
				"rules": [{
					"id": "r1", "type": "transfer",
					"src": {"kind": "cfs", "vol": "v", "path": "/p"},
					"dst": {"kind": "s3", "bucket": "b"}
				}]
			}`,
			wantCode: ErrCodeTypeError,
			msgHas:   "type",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseSyncConfig([]byte(tc.raw))
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			var ce *ConfigError
			if !errors.As(err, &ce) {
				t.Fatalf("expected *ConfigError, got %T: %v", err, err)
			}
			if ce.Code != tc.wantCode {
				t.Errorf("Code = %d (msg=%q), want %d", ce.Code, ce.Msg, tc.wantCode)
			}
			if tc.msgHas != "" && !strings.Contains(strings.ToLower(ce.Msg), tc.msgHas) {
				t.Errorf("Msg = %q, want to contain %q", ce.Msg, tc.msgHas)
			}
		})
	}
}

// TestValidateConfig_FullExample loads the example from design.md §4.1 (W1-W5
// + check rules) and asserts no validation error. This is the positive
// end-to-end test for Phase A-2.
func TestValidateConfig_FullExample(t *testing.T) {
	// Trimmed inline copy of the §4.1 example. If the example in the design
	// doc evolves, update this test to match — the test guards against
	// regression where the documented example no longer validates.
	raw := `{
  "role": "sync",
  "listen": "17710",
  "httpListen": "17711",
  "masterAddr": "10.0.0.1:17010,10.0.0.2:17010",
  "logDir": "/cfs/log/syncnode",
  "logLevel": "info",
  "dataDir": "/cfs/data/syncnode",
  "s3Defaults": {
    "endpoint": "https://s3.cn-north-1.amazonaws.com.cn",
    "region": "cn-north-1",
    "accessKeyEnv": "AWS_ACCESS_KEY_ID",
    "secretKeyEnv": "AWS_SECRET_ACCESS_KEY",
    "storageClass": "STANDARD_IA"
  },
  "posix": {
    "allowedRoots": ["/mnt/gpfs", "/mnt/lustre", "/var/cfs-backup"],
    "maxDirDepth": 20,
    "defaultBufferSizeKiB": 4096
  },
  "concurrency": {
    "maxConcurrentTasks": 8,
    "transfersPerTask": 4,
    "bandwidthLimitMBps": 200
  },
  "rules": [
    {
      "id": "w1-gpfs-to-cubefs-models",
      "type": "sync",
      "schedule": "*/15 * * * *",
      "src": {"kind": "local", "path": "/mnt/gpfs/runs/", "bufferSizeKiB": 16384, "concurrency": 8, "fadviseSequential": true},
      "dst": {"kind": "cfs", "vol": "warm-vol", "path": "/runs/"},
      "filter": {"include": ["*.pt"], "minSize": "1MB", "minAge": "60s"},
      "retention": {"pattern": "model-step-{N}.pt", "keepLast": 10},
      "afterCopy": "keep"
    },
    {
      "id": "w2-cubefs-to-cold-archive",
      "type": "sync",
      "schedule": "0 2 * * *",
      "src": {"kind": "cfs", "vol": "warm-vol", "path": "/runs/"},
      "dst": {"kind": "s3", "bucket": "ckpt-archive", "prefix": "runs/", "storageClass": "STANDARD_IA"},
      "filter": {"include": ["*.pt"], "minAge": "7d"},
      "retention": {"pattern": "model-step-{N}.pt", "keepLast": 30},
      "afterCopy": "verify_then_delete_src"
    },
    {
      "id": "w3-cold-reload-on-demand",
      "type": "load",
      "src": {"kind": "s3", "bucket": "ckpt-archive", "prefix": "runs/"},
      "dst": {"kind": "cfs", "vol": "warm-vol", "path": "/restored/"},
      "downloadStrategy": "temp_rename"
    },
    {
      "id": "w4-gpfs-direct-cold",
      "type": "sync",
      "schedule": "0 3 * * *",
      "src": {"kind": "local", "path": "/mnt/gpfs/intermediate/", "bufferSizeKiB": 16384, "concurrency": 8},
      "dst": {"kind": "s3", "bucket": "ckpt-archive", "prefix": "intermediate/", "storageClass": "STANDARD_IA"},
      "filter": {"minSize": "10MB", "minAge": "1d"},
      "afterCopy": "verify_then_delete_src"
    },
    {
      "id": "w5-dataset-import",
      "type": "load",
      "src": {"kind": "s3", "bucket": "datasets", "prefix": "imagenet-v2/"},
      "dst": {"kind": "cfs", "vol": "datasets-vol", "path": "/imagenet-v2/"},
      "downloadStrategy": "temp_rename"
    },
    {
      "id": "weekly-integrity-check",
      "type": "check",
      "schedule": "0 4 * * 1",
      "src": {"kind": "cfs", "vol": "warm-vol", "path": "/runs/"},
      "dst": {"kind": "s3", "bucket": "ckpt-archive", "prefix": "runs/"},
      "sampleStrategy": "least_recently_checked",
      "sampleRate": 0.05,
      "onMismatch": "alert"
    }
  ]
}`
	cfg, err := ParseSyncConfig([]byte(raw))
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected non-nil cfg")
	}
	if len(cfg.Rules) != 6 {
		t.Errorf("expected 6 rules, got %d", len(cfg.Rules))
	}
	if cfg.Concurrency.MaxConcurrentTasks != 8 {
		t.Errorf("MaxConcurrentTasks = %d, want 8", cfg.Concurrency.MaxConcurrentTasks)
	}
	if cfg.ExporterPort != defaultExporterPort {
		t.Errorf("ExporterPort should default to %d, got %d", defaultExporterPort, cfg.ExporterPort)
	}
}

func TestValidateConfig_Defaults(t *testing.T) {
	raw := `{
		"masterAddr": "127.0.0.1:17010",
		"dataDir": "/tmp/d"
	}`
	cfg, err := ParseSyncConfig([]byte(raw))
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if cfg.Listen != defaultListen {
		t.Errorf("Listen default = %q, want %q", cfg.Listen, defaultListen)
	}
	if cfg.HTTPListen != defaultHTTPListen {
		t.Errorf("HTTPListen default = %q, want %q", cfg.HTTPListen, defaultHTTPListen)
	}
	if cfg.ExporterPort != defaultExporterPort {
		t.Errorf("ExporterPort default = %d, want %d", cfg.ExporterPort, defaultExporterPort)
	}
	if cfg.LogLevel != defaultLogLevel {
		t.Errorf("LogLevel default = %q, want %q", cfg.LogLevel, defaultLogLevel)
	}
}

func TestValidateConfig_DuplicateRuleID(t *testing.T) {
	raw := `{
		"masterAddr": "127.0.0.1:17010",
		"dataDir": "/tmp/d",
		"rules": [
			{"id": "same", "type": "sync",
			 "src": {"kind": "cfs", "vol": "v", "path": "/p"},
			 "dst": {"kind": "s3", "bucket": "b"}},
			{"id": "same", "type": "load",
			 "src": {"kind": "s3", "bucket": "b"},
			 "dst": {"kind": "cfs", "vol": "v", "path": "/p"}}
		]
	}`
	_, err := ParseSyncConfig([]byte(raw))
	if err == nil {
		t.Fatal("expected duplicate id error")
	}
	var ce *ConfigError
	if !errors.As(err, &ce) || ce.Code != ErrCodeDuplicateRuleID {
		t.Errorf("Code = %v, want %d", err, ErrCodeDuplicateRuleID)
	}
}

func TestValidateCronExpr(t *testing.T) {
	cases := []struct {
		expr string
		ok   bool
	}{
		{"* * * * *", true},
		{"*/15 * * * *", true},
		{"0 2 * * *", true},
		{"0 0 1 1 0", true},
		{"@daily", true},
		{"@every 30s", true},
		{"@yearly", true},
		{"", false},
		{"too few", false},
		{"* * * *", false},        // 4 fields
		{"abc def ghi jkl mno", false},
		{"@unknown", false},
	}
	for _, tc := range cases {
		err := validateCronExpr(tc.expr)
		if tc.ok && err != nil {
			t.Errorf("validateCronExpr(%q) returned err: %v, want nil", tc.expr, err)
		}
		if !tc.ok && err == nil {
			t.Errorf("validateCronExpr(%q) returned nil, want error", tc.expr)
		}
	}
}
