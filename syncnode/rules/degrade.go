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

package rules

import "strings"

// ErrorClass categorises terminal task errors so the rule store can react
// differently to each. The classifier never returns a "retryable" class:
// per-file retries are already handled inside the executor before the
// error surfaces here.
//
// See design.md §9 G-3.
type ErrorClass int

const (
	// ClassUnknown is the default — the classifier couldn't match any
	// known shape. Callers should leave the rule's state untouched.
	ClassUnknown ErrorClass = iota

	// ClassVolNotFound — the backend reports that the configured CubeFS
	// volume does not exist. Permanent until an operator recreates the
	// vol and manually resumes the rule.
	ClassVolNotFound

	// ClassPathNotAllowed — the local backend rejected the configured path
	// because it falls outside allowedRoots. Permanent until config is
	// fixed.
	ClassPathNotAllowed

	// ClassAuthFailure — S3 SignatureDoesNotMatch / 401 / 403 or equivalent
	// from another backend. Permanent until credentials are rotated.
	ClassAuthFailure

	// ClassTransientNet — connection reset, dial timeout, 5xx. The
	// executor already retried at the per-file level; the rule stays
	// active so the next scheduled run can try again.
	ClassTransientNet

	// ClassQuotaExceeded — upstream throttle (S3 SlowDown, 429). Same
	// handling as transient — alert only, do not auto-degrade.
	ClassQuotaExceeded
)

// String returns a stable lower-case identifier for the class. Useful as a
// label in metrics and in operator-facing logs.
func (ec ErrorClass) String() string {
	switch ec {
	case ClassVolNotFound:
		return "vol_not_found"
	case ClassPathNotAllowed:
		return "path_not_allowed"
	case ClassAuthFailure:
		return "auth_failure"
	case ClassTransientNet:
		return "transient_network"
	case ClassQuotaExceeded:
		return "quota_exceeded"
	default:
		return "unknown"
	}
}

// IsDegrading reports whether the class warrants flipping the rule to
// StateDegraded. Transient classes are alert-worthy but not degrading.
func (ec ErrorClass) IsDegrading() bool {
	switch ec {
	case ClassVolNotFound, ClassPathNotAllowed, ClassAuthFailure:
		return true
	default:
		return false
	}
}

// ClassifyError inspects the message of a terminal task error and returns
// the best-guess class. Matching is best-effort:
//
//   - Empty strings (and "unrecognised" shapes) return ClassUnknown.
//   - The classifier walks the message once, matching substring patterns
//     in priority order (vol > path > auth > quota > transient). First
//     match wins; ties resolve in declaration order.
//
// The patterns reflect REAL error strings produced today by the backends:
//
//   - cfs:   "cfs: get volume info: <wrapped proto.ErrVolNotExists>" where
//            proto.ErrVolNotExists.Error() == "vol not exists"
//   - local: "%w: path %q is outside allowedRoots %v" (see backend/local/local.go)
//   - s3:    wrapped aws-sdk errors carry "SignatureDoesNotMatch",
//            "InvalidAccessKey", "AccessDenied", "NoSuchBucket",
//            "SlowDown", and standard HTTP status codes.
//
// The classifier is intentionally tolerant of message-shape drift — it
// only requires the substring to appear somewhere in the formatted error.
func ClassifyError(errStr string) ErrorClass {
	if errStr == "" {
		return ClassUnknown
	}
	// Lowercase once so callers don't have to worry about producers using
	// title-case (e.g. AWS API error codes are typically PascalCase, while
	// proto / local backends produce all-lowercase messages).
	lower := strings.ToLower(errStr)

	// Vol not found — highest priority. Captures the proto.ErrVolNotExists
	// wording verbatim plus a couple of common variants we've seen in
	// SDK / objectnode logs.
	for _, pat := range volNotFoundPatterns {
		if strings.Contains(lower, pat) {
			return ClassVolNotFound
		}
	}

	// Local-backend path-not-allowed rejection.
	for _, pat := range pathNotAllowedPatterns {
		if strings.Contains(lower, pat) {
			return ClassPathNotAllowed
		}
	}

	// S3 auth failures.
	for _, pat := range authFailurePatterns {
		if strings.Contains(lower, pat) {
			return ClassAuthFailure
		}
	}

	// S3 throttles. Checked before generic transient so 429 stays in its
	// own class.
	for _, pat := range quotaExceededPatterns {
		if strings.Contains(lower, pat) {
			return ClassQuotaExceeded
		}
	}

	// Transient network errors.
	for _, pat := range transientNetPatterns {
		if strings.Contains(lower, pat) {
			return ClassTransientNet
		}
	}

	return ClassUnknown
}

// All match patterns are lower-cased — ClassifyError lower-cases the input
// once before walking these. Keep each list sorted from most-specific to
// most-generic so a false hit on a generic pattern cannot mask a more
// precise one.

var volNotFoundPatterns = []string{
	"vol not exists",      // proto.ErrVolNotExists.Error()
	"vol not exist",       // singular variant occasionally seen in SDK errors
	"volume not exists",   // some user-facing messages reorder the word
	"volume not exist",
	"volume not found",
	"vol not found",
	"no such volume",
}

var pathNotAllowedPatterns = []string{
	"outside allowedroots",          // backend/local/local.go line 181 / 193 / 231
	"is outside allowedroots",
	"resolves outside allowedroots", // line 317
	"resolves to",                   // narrow but unique to the local outside-roots path
}

var authFailurePatterns = []string{
	"signaturedoesnotmatch",
	"invalidaccesskeyid",
	"invalidaccesskey",
	"accessdenied",
	"access denied",
	"unauthorized",
	"forbidden",
	"403",
	"401",
}

var quotaExceededPatterns = []string{
	"slowdown",
	"throttlingexception",
	"requestlimitexceeded",
	"too many requests",
	"429",
}

var transientNetPatterns = []string{
	"connection reset",
	"connection refused",
	"i/o timeout",
	"dial tcp",
	"no such host",
	"network is unreachable",
	"eof",
	"broken pipe",
	"503",
	"502",
	"504",
	"500",
}
