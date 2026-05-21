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

import (
	"context"
	"errors"
	"testing"
)

func TestClassifyError(t *testing.T) {
	tests := []struct {
		name string
		err  string
		want ErrorClass
	}{
		{"empty", "", ClassUnknown},
		{"unknown_garbage", "something completely unrelated", ClassUnknown},

		// Vol-not-found — the canonical proto.ErrVolNotExists wording.
		{"vol_not_exists_canonical", "cfs: get volume info: vol not exists", ClassVolNotFound},
		{"vol_not_exists_singular", "vol not exist", ClassVolNotFound},
		{"volume_not_exists", "volume not exists for v1", ClassVolNotFound},
		{"volume_not_found_uppercase", "GET /admin/getVol: Volume not found", ClassVolNotFound},
		{"no_such_volume", "master replied: no such volume", ClassVolNotFound},

		// Local-backend path-not-allowed rejection.
		{"path_outside_roots", `backend: invalid config: path "/etc/passwd" is outside allowedRoots [/data]`, ClassPathNotAllowed},
		{"resolves_outside_roots", `backend: invalid config: parent "/x" resolves outside allowedRoots`, ClassPathNotAllowed},

		// Auth failures.
		{"sig_does_not_match", "s3 Put bkt/obj: SignatureDoesNotMatch: bad sig", ClassAuthFailure},
		{"access_denied", "s3 Head: AccessDenied", ClassAuthFailure},
		{"http_403", "s3 PutObject bkt/obj: api error 403 forbidden", ClassAuthFailure},
		{"http_401", "s3 List bkt: api error 401 unauthorized", ClassAuthFailure},

		// Quota / throttle.
		{"slowdown", "s3 Put bkt/obj: SlowDown: please slow down", ClassQuotaExceeded},
		{"too_many_requests", "upstream returned: too many requests", ClassQuotaExceeded},
		{"http_429", "api error 429 rate limited", ClassQuotaExceeded},

		// Transient network.
		{"connection_reset", "read tcp: connection reset by peer", ClassTransientNet},
		{"i_o_timeout", "Get https://foo: dial tcp 10.0.0.1:443: i/o timeout", ClassTransientNet},
		{"http_503", "s3 PutObject bkt/obj: api error 503 service unavailable", ClassTransientNet},
		{"http_500", "s3 PutObject bkt/obj: api error 500", ClassTransientNet},
		{"eof", "cfs read source: EOF", ClassTransientNet},

		// Priority: vol-not-found ahead of auth even if both substrings appear.
		{"priority_vol_over_auth", "vol not exists and 403", ClassVolNotFound},
		// Priority: auth before quota when both substrings appear.
		{"priority_auth_over_quota", "AccessDenied; also 429", ClassAuthFailure},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ClassifyError(tc.err)
			if got != tc.want {
				t.Fatalf("ClassifyError(%q) = %v (%s), want %v (%s)",
					tc.err, got, got, tc.want, tc.want)
			}
		})
	}
}

func TestErrorClass_IsDegrading(t *testing.T) {
	tests := []struct {
		class ErrorClass
		want  bool
	}{
		{ClassUnknown, false},
		{ClassVolNotFound, true},
		{ClassPathNotAllowed, true},
		{ClassAuthFailure, true},
		{ClassTransientNet, false},
		{ClassQuotaExceeded, false},
	}
	for _, tc := range tests {
		t.Run(tc.class.String(), func(t *testing.T) {
			if got := tc.class.IsDegrading(); got != tc.want {
				t.Fatalf("%v.IsDegrading() = %v, want %v", tc.class, got, tc.want)
			}
		})
	}
}

func TestErrorClass_String(t *testing.T) {
	tests := []struct {
		class ErrorClass
		want  string
	}{
		{ClassUnknown, "unknown"},
		{ClassVolNotFound, "vol_not_found"},
		{ClassPathNotAllowed, "path_not_allowed"},
		{ClassAuthFailure, "auth_failure"},
		{ClassTransientNet, "transient_network"},
		{ClassQuotaExceeded, "quota_exceeded"},
		{ErrorClass(999), "unknown"},
	}
	for _, tc := range tests {
		t.Run(tc.want, func(t *testing.T) {
			if got := tc.class.String(); got != tc.want {
				t.Fatalf("class %d String() = %q, want %q", int(tc.class), got, tc.want)
			}
		})
	}
}

func TestDegrade_Success(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	t.Cleanup(func() { _ = s.Close() })

	r := newTestRule("rule-a")
	if err := s.Create(ctx, r); err != nil {
		t.Fatalf("Create: %v", err)
	}

	const reason = "cfs: get volume info: vol not exists"
	if err := Degrade(ctx, s, "rule-a", reason); err != nil {
		t.Fatalf("Degrade: %v", err)
	}

	got, err := s.Get(ctx, "rule-a")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.State != StateDegraded {
		t.Errorf("State = %q, want %q", got.State, StateDegraded)
	}
	if got.LastRunError != reason {
		t.Errorf("LastRunError = %q, want %q", got.LastRunError, reason)
	}
	if got.LastRunStatus != "failed" {
		t.Errorf("LastRunStatus = %q, want %q", got.LastRunStatus, "failed")
	}
	if got.LastRunAt.IsZero() {
		t.Errorf("LastRunAt is zero")
	}
}

func TestDegrade_EmptyReasonFallsBackToInterrupted(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	t.Cleanup(func() { _ = s.Close() })

	if err := s.Create(ctx, newTestRule("rule-empty")); err != nil {
		t.Fatalf("Create: %v", err)
	}

	if err := Degrade(ctx, s, "rule-empty", ""); err != nil {
		t.Fatalf("Degrade: %v", err)
	}

	got, _ := s.Get(ctx, "rule-empty")
	if got.LastRunError != ReasonRuleInterrupted {
		t.Errorf("LastRunError = %q, want %q", got.LastRunError, ReasonRuleInterrupted)
	}
}

func TestDegrade_Idempotent(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	t.Cleanup(func() { _ = s.Close() })

	if err := s.Create(ctx, newTestRule("rule-idem")); err != nil {
		t.Fatalf("Create: %v", err)
	}

	if err := Degrade(ctx, s, "rule-idem", "first"); err != nil {
		t.Fatalf("Degrade #1: %v", err)
	}
	if err := Degrade(ctx, s, "rule-idem", "second"); err != nil {
		t.Fatalf("Degrade #2: %v", err)
	}

	got, _ := s.Get(ctx, "rule-idem")
	if got.State != StateDegraded {
		t.Errorf("State = %q, want %q", got.State, StateDegraded)
	}
	if got.LastRunError != "second" {
		t.Errorf("LastRunError = %q, want %q (idempotent should overwrite)", got.LastRunError, "second")
	}
}

func TestDegrade_UnknownRule(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	t.Cleanup(func() { _ = s.Close() })

	err := Degrade(ctx, s, "ghost", "vol not exists")
	if !errors.Is(err, ErrRuleNotFound) {
		t.Fatalf("Degrade(ghost) = %v, want ErrRuleNotFound", err)
	}
}

func TestDegrade_NilStore(t *testing.T) {
	err := Degrade(context.Background(), nil, "anything", "reason")
	if err == nil {
		t.Fatal("Degrade(nil store) = nil, want error")
	}
}

func TestDegrade_EmptyRuleID(t *testing.T) {
	s := NewMemoryStore()
	t.Cleanup(func() { _ = s.Close() })
	err := Degrade(context.Background(), s, "", "reason")
	if !errors.Is(err, ErrRuleNotFound) {
		t.Fatalf("Degrade(\"\") = %v, want ErrRuleNotFound", err)
	}
}

// TestSentinelErrors ensures the package-level sentinels are stable string
// identifiers — they're documented for operators, so changing the wording
// is a breaking change.
func TestSentinelErrors(t *testing.T) {
	tests := []struct {
		err  error
		want string
	}{
		{ErrVolNotFound, "vol not found"},
		{ErrPathNotAllowed, "path not allowed"},
		{ErrAuthFailure, "auth failure"},
		{ErrTransientNet, "transient network error"},
		{ErrQuotaExceeded, "quota exceeded"},
	}
	for _, tc := range tests {
		t.Run(tc.want, func(t *testing.T) {
			if tc.err.Error() != tc.want {
				t.Errorf("Error() = %q, want %q", tc.err.Error(), tc.want)
			}
		})
	}
}
