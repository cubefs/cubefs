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

package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// recordedResponse decodes the wire envelope from a recorded response.
type recordedResponse struct {
	Status int
	Code   int
	Msg    string
	Data   interface{}
}

func decode(t *testing.T, rec *httptest.ResponseRecorder) recordedResponse {
	t.Helper()
	var resp Response
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode body: %v (body=%s)", err, rec.Body.String())
	}
	return recordedResponse{
		Status: rec.Code,
		Code:   resp.Code,
		Msg:    resp.Msg,
		Data:   resp.Data,
	}
}

func TestWriteOK_Envelope(t *testing.T) {
	rec := httptest.NewRecorder()
	WriteOK(rec, map[string]string{"hello": "world"})

	got := decode(t, rec)
	if got.Status != http.StatusOK {
		t.Errorf("status = %d, want 200", got.Status)
	}
	if got.Code != CodeOK {
		t.Errorf("code = %d, want %d", got.Code, CodeOK)
	}
	if got.Msg != "OK" {
		t.Errorf("msg = %q, want OK", got.Msg)
	}
	if rec.Header().Get("Content-Type") != "application/json; charset=utf-8" {
		t.Errorf("content-type = %q", rec.Header().Get("Content-Type"))
	}
}

func TestWriteOK_NilPayload(t *testing.T) {
	rec := httptest.NewRecorder()
	WriteOK(rec, nil)
	if !strings.Contains(rec.Body.String(), `"code":0`) {
		t.Errorf("missing code 0; body=%s", rec.Body.String())
	}
}

func TestWriteError_APIError(t *testing.T) {
	rec := httptest.NewRecorder()
	WriteError(rec, ErrNotFound("rule", "abc"))

	got := decode(t, rec)
	if got.Status != http.StatusNotFound {
		t.Errorf("status = %d, want 404", got.Status)
	}
	if got.Code != CodeNotFound {
		t.Errorf("code = %d, want %d", got.Code, CodeNotFound)
	}
	if !strings.Contains(got.Msg, "abc") {
		t.Errorf("msg = %q, missing resource id", got.Msg)
	}
}

func TestWriteError_GenericError(t *testing.T) {
	rec := httptest.NewRecorder()
	WriteError(rec, errors.New("boom"))

	got := decode(t, rec)
	if got.Status != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", got.Status)
	}
	if got.Code != CodeInternal {
		t.Errorf("code = %d, want %d", got.Code, CodeInternal)
	}
	if got.Msg != "boom" {
		t.Errorf("msg = %q, want boom", got.Msg)
	}
}

func TestWriteError_WrappedAPIError(t *testing.T) {
	// errors.As must unwrap the *APIError even when it's wrapped.
	inner := ErrConflict("dup rule %q", "r-1")
	wrapped := errWrap{inner: inner, prefix: "store: "}
	rec := httptest.NewRecorder()
	WriteError(rec, wrapped)

	got := decode(t, rec)
	if got.Status != http.StatusConflict {
		t.Errorf("status = %d, want 409", got.Status)
	}
	if got.Code != CodeConflict {
		t.Errorf("code = %d, want %d", got.Code, CodeConflict)
	}
}

func TestWriteError_Nil(t *testing.T) {
	rec := httptest.NewRecorder()
	WriteError(rec, nil)
	if rec.Code != http.StatusOK {
		t.Errorf("nil err should fall back to 200; got %d", rec.Code)
	}
}

func TestAPIErrorString(t *testing.T) {
	if (*APIError)(nil).Error() != "" {
		t.Errorf("nil receiver should yield empty string")
	}
	s := ErrInvalidField("foo", "must be positive").Error()
	for _, want := range []string{"code=2003", "status=400", "foo", "must be positive"} {
		if !strings.Contains(s, want) {
			t.Errorf("Error() = %q missing %q", s, want)
		}
	}
}

func TestToHTTPHandler_Success(t *testing.T) {
	h := ToHTTPHandler(func(r *http.Request) (interface{}, error) {
		return map[string]int{"n": 42}, nil
	})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/anything", nil)
	h(rec, req)

	got := decode(t, rec)
	if got.Status != http.StatusOK {
		t.Errorf("status = %d, want 200", got.Status)
	}
}

func TestToHTTPHandler_Error(t *testing.T) {
	h := ToHTTPHandler(func(r *http.Request) (interface{}, error) {
		return nil, ErrMissingField("ruleID")
	})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/x", nil)
	h(rec, req)

	got := decode(t, rec)
	if got.Status != http.StatusBadRequest || got.Code != CodeMissingField {
		t.Errorf("status=%d code=%d; want 400/%d", got.Status, got.Code, CodeMissingField)
	}
}

func TestAuthMiddleware_Passthrough(t *testing.T) {
	// P0 = no-op auth. Verify the chained handler runs end-to-end and the
	// final payload reaches the wire envelope unchanged.
	want := map[string]string{"ok": "yes"}
	chained := Chain(
		func(r *http.Request) (interface{}, error) { return want, nil },
		AuthMiddleware,
	)
	got, err := chained(httptest.NewRequest(http.MethodGet, "/", nil))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if got.(map[string]string)["ok"] != "yes" {
		t.Errorf("got %v", got)
	}
}

// withAdminToken sets the package-level admin token for the duration of
// the test and restores the previous value via t.Cleanup. Tests that
// install a token must do so via this helper so a panic / fail in one
// test doesn't leak into the next.
func withAdminToken(t *testing.T, token string) {
	t.Helper()
	prev := getAdminToken()
	SetAdminToken(token)
	t.Cleanup(func() { SetAdminToken(prev) })
}

// TestAuthMiddleware_DisabledWhenTokenEmpty pins down the "auth-off"
// default: when no SetAdminToken has happened (or it's been cleared),
// every request passes through. This preserves the pre-fix behaviour
// for tests + dev that build a SyncConfig without an adminToken.
func TestAuthMiddleware_DisabledWhenTokenEmpty(t *testing.T) {
	withAdminToken(t, "")

	want := map[string]string{"ok": "yes"}
	chained := AuthMiddleware(func(r *http.Request) (interface{}, error) { return want, nil })
	req := httptest.NewRequest(http.MethodGet, "/admin/syncnode/version", nil)
	// No Authorization header at all — should still pass.
	got, err := chained(req)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if got.(map[string]string)["ok"] != "yes" {
		t.Errorf("got %v", got)
	}
}

// TestAuthMiddleware_RejectsMissingToken: when a token is set on the
// server, a request with no credential is 401 / CodeUnauthorized.
func TestAuthMiddleware_RejectsMissingToken(t *testing.T) {
	withAdminToken(t, "s3cret-token")

	called := false
	chained := AuthMiddleware(func(r *http.Request) (interface{}, error) {
		called = true
		return "ok", nil
	})
	req := httptest.NewRequest(http.MethodGet, "/admin/syncnode/version", nil)
	_, err := chained(req)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if called {
		t.Fatal("inner handler must not be called when auth fails")
	}
	apiErr, ok := err.(*APIError)
	if !ok || apiErr == nil {
		t.Fatalf("err = %v (%T), want *APIError", err, err)
	}
	if apiErr.Status != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", apiErr.Status)
	}
	if apiErr.Code != CodeUnauthorized {
		t.Errorf("code = %d, want %d", apiErr.Code, CodeUnauthorized)
	}
}

// TestAuthMiddleware_AcceptsBearer: Authorization: Bearer <token> form
// is accepted, including case-insensitive scheme matching.
func TestAuthMiddleware_AcceptsBearer(t *testing.T) {
	withAdminToken(t, "s3cret-token")

	cases := []string{
		"Bearer s3cret-token",
		"bearer s3cret-token",
		"BEARER s3cret-token",
	}
	for _, header := range cases {
		header := header
		t.Run(header, func(t *testing.T) {
			t.Parallel()
			chained := AuthMiddleware(func(r *http.Request) (interface{}, error) {
				return "ok", nil
			})
			req := httptest.NewRequest(http.MethodGet, "/admin/syncnode/version", nil)
			req.Header.Set("Authorization", header)
			got, err := chained(req)
			if err != nil {
				t.Fatalf("err: %v", err)
			}
			if got != "ok" {
				t.Errorf("got %v", got)
			}
		})
	}
}

// TestAuthMiddleware_AcceptsXSyncToken: X-Sync-Token header is the
// secondary form, used by curl-friendly tooling that doesn't want to
// wrestle with Authorization parsing.
func TestAuthMiddleware_AcceptsXSyncToken(t *testing.T) {
	withAdminToken(t, "s3cret-token")

	chained := AuthMiddleware(func(r *http.Request) (interface{}, error) {
		return "ok", nil
	})
	req := httptest.NewRequest(http.MethodGet, "/admin/syncnode/version", nil)
	req.Header.Set("X-Sync-Token", "s3cret-token")
	got, err := chained(req)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if got != "ok" {
		t.Errorf("got %v", got)
	}
}

// TestAuthMiddleware_RejectsWrongToken: a credential is provided but it
// does not match. Must return 401 + CodeUnauthorized and must NOT call
// the inner handler.
func TestAuthMiddleware_RejectsWrongToken(t *testing.T) {
	withAdminToken(t, "s3cret-token")

	cases := []struct {
		name   string
		header string
		value  string
	}{
		{"bearer_mismatch", "Authorization", "Bearer wrong"},
		{"xsync_mismatch", "X-Sync-Token", "wrong"},
		{"bearer_empty", "Authorization", "Bearer "},
		{"bearer_no_prefix", "Authorization", "s3cret-token"},     // missing "Bearer "
		{"xsync_whitespace", "X-Sync-Token", "    "},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			called := false
			chained := AuthMiddleware(func(r *http.Request) (interface{}, error) {
				called = true
				return "ok", nil
			})
			req := httptest.NewRequest(http.MethodGet, "/admin/syncnode/version", nil)
			req.Header.Set(tc.header, tc.value)
			_, err := chained(req)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if called {
				t.Fatal("inner handler must not be called when auth fails")
			}
			apiErr, ok := err.(*APIError)
			if !ok || apiErr.Status != http.StatusUnauthorized || apiErr.Code != CodeUnauthorized {
				t.Fatalf("err = %v (%T), want 401/%d", err, err, CodeUnauthorized)
			}
		})
	}
}

// TestAuthMiddleware_EndToEnd wires AuthMiddleware through ToHTTPHandler
// to confirm the 401 reaches the wire envelope exactly the way operator
// tooling sees it.
func TestAuthMiddleware_EndToEnd(t *testing.T) {
	withAdminToken(t, "abc123")

	h := ToHTTPHandler(func(r *http.Request) (interface{}, error) {
		return map[string]string{"role": "sync"}, nil
	}, AuthMiddleware)

	// No header — 401.
	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodGet, "/admin/syncnode/version", nil))
	got := decode(t, rec)
	if got.Status != http.StatusUnauthorized || got.Code != CodeUnauthorized {
		t.Fatalf("missing-token: status=%d code=%d, want 401/%d", got.Status, got.Code, CodeUnauthorized)
	}

	// Valid header — 200.
	rec = httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/admin/syncnode/version", nil)
	req.Header.Set("Authorization", "Bearer abc123")
	h(rec, req)
	got = decode(t, rec)
	if got.Status != http.StatusOK || got.Code != CodeOK {
		t.Fatalf("valid-token: status=%d code=%d, want 200/0", got.Status, got.Code)
	}
}

// TestConstantTimeEq exercises the helper directly so a future refactor
// can't drop the length-mismatch handling without a test screaming.
func TestConstantTimeEq(t *testing.T) {
	cases := []struct {
		name string
		a, b string
		want bool
	}{
		{"equal", "abcdef", "abcdef", true},
		{"diff_value_same_len", "abcdef", "abcdez", false},
		{"diff_len_short", "abc", "abcdef", false},
		{"diff_len_long", "abcdef", "abc", false},
		{"both_empty", "", "", true},
		{"empty_vs_nonempty", "", "x", false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if got := constantTimeEq(tc.a, tc.b); got != tc.want {
				t.Errorf("constantTimeEq(%q,%q) = %v, want %v", tc.a, tc.b, got, tc.want)
			}
		})
	}
}

// TestSetAdminToken_Swap confirms a later SetAdminToken call REPLACES
// the previous value rather than appending. The reload path in
// server.go relies on this to roll the token without restarting the
// listener (even though current design only sets on startup, the swap
// behaviour is a public guarantee).
func TestSetAdminToken_Swap(t *testing.T) {
	prev := getAdminToken()
	t.Cleanup(func() { SetAdminToken(prev) })

	SetAdminToken("first")
	if got := getAdminToken(); got != "first" {
		t.Fatalf("getAdminToken = %q, want first", got)
	}
	SetAdminToken("second")
	if got := getAdminToken(); got != "second" {
		t.Fatalf("getAdminToken = %q, want second", got)
	}
	SetAdminToken("")
	if got := getAdminToken(); got != "" {
		t.Fatalf("getAdminToken = %q, want empty", got)
	}
}

func TestChain_Ordering(t *testing.T) {
	// Outer middleware should run BEFORE inner. We tag the request via a
	// header so the order of operations is observable end-to-end.
	order := []string{}
	mw := func(label string) Middleware {
		return func(next Handler) Handler {
			return func(r *http.Request) (interface{}, error) {
				order = append(order, label)
				return next(r)
			}
		}
	}
	chained := Chain(
		func(r *http.Request) (interface{}, error) {
			order = append(order, "inner")
			return nil, nil
		},
		mw("first"),
		mw("second"),
	)
	_, _ = chained(httptest.NewRequest(http.MethodGet, "/", nil))
	want := []string{"first", "second", "inner"}
	if len(order) != len(want) {
		t.Fatalf("order = %v, want %v", order, want)
	}
	for i := range order {
		if order[i] != want[i] {
			t.Errorf("order[%d] = %s, want %s", i, order[i], want[i])
		}
	}
}

// errWrap is a minimal error wrapper used only for testing Unwrap-style
// error chains pass through to APIError.
type errWrap struct {
	inner  error
	prefix string
}

func (e errWrap) Error() string { return e.prefix + e.inner.Error() }
func (e errWrap) Unwrap() error { return e.inner }
