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

import "fmt"

// ConfigError represents a structured failure from validateConfig. Code is a
// stable integer (suitable for testing assertions and operator alerts) and
// Msg includes the offending field name plus context.
type ConfigError struct {
	Code  int
	Field string
	Msg   string
}

func (e *ConfigError) Error() string {
	if e.Field != "" {
		return fmt.Sprintf("syncnode config (code=%d field=%s): %s", e.Code, e.Field, e.Msg)
	}
	return fmt.Sprintf("syncnode config (code=%d): %s", e.Code, e.Msg)
}

// Error codes. Each is stable across releases; tests assert against the
// numeric value, not the message.
const (
	ErrCodeInvalidCron          = 1001
	ErrCodeInvalidKind          = 1002
	ErrCodePathNotAllowed       = 1003
	ErrCodeS3EndpointMissing    = 1004
	ErrCodeRetentionPatternNoN  = 1005
	ErrCodeMinSizeUnit          = 1006
	ErrCodeRequiredFieldMissing = 1007
	ErrCodeTypeError            = 1008
	ErrCodeDuplicateRuleID      = 1009
	ErrCodeUnknownAfterCopy     = 1010
	ErrCodeUnknownDownloadStrat = 1011
	ErrCodeUnknownOnMismatch    = 1012
	ErrCodeInvalidDuration      = 1013
	ErrCodeUnknownOnExisting    = 1014
	ErrCodeUnknownOnSymlink     = 1015
	ErrCodeDryRunConfirmConflict = 1016
)

func newConfigErr(code int, field, msg string) *ConfigError {
	return &ConfigError{Code: code, Field: field, Msg: msg}
}
