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
