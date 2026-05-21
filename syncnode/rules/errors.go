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

import "errors"

// Operator-facing sentinel errors used by the degrade classifier (see
// degrade.go). Backend packages don't currently wrap typed errors all the
// way out — most surfaces report a formatted string built around the
// underlying SDK error. These sentinels exist so:
//
//   - callers that DO have typed information can wrap one of these
//     (errors.Is matchable),
//   - the classifier can still fall back to substring search on legacy
//     string-only errors (see ClassifyError in degrade.go).
//
// Keep this list small and tied to the ErrorClass enum — one sentinel per
// distinct class. Do not add general-purpose error types here; those live
// in rule.go.
var (
	// ErrVolNotFound signals that a CubeFS volume referenced by a rule no
	// longer exists. The classifier flips matching rules to StateDegraded.
	// The matching substring "vol not exists" comes from proto.ErrVolNotExists.
	ErrVolNotFound = errors.New("vol not found")

	// ErrPathNotAllowed signals that the local backend refused a path because
	// it falls outside the configured allowedRoots. The classifier flips
	// matching rules to StateDegraded. Matches local backend's
	// "outside allowedRoots" / "outside allowedRoots" wording.
	ErrPathNotAllowed = errors.New("path not allowed")

	// ErrAuthFailure signals an authentication / authorization failure from
	// an upstream backend (S3 SignatureDoesNotMatch, 401, 403, etc).
	// The classifier flips matching rules to StateDegraded.
	ErrAuthFailure = errors.New("auth failure")

	// ErrTransientNet signals a transient network condition (connection
	// reset, dial timeout, 5xx). Best-effort retry has already happened
	// inside the executor; we keep the class so operators can alert on it
	// without auto-degrading the rule.
	ErrTransientNet = errors.New("transient network error")

	// ErrQuotaExceeded signals an upstream throttle (S3 SlowDown, 429).
	// Same handling as transient — alert, do not degrade.
	ErrQuotaExceeded = errors.New("quota exceeded")
)
