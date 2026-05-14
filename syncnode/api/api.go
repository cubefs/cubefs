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

// Package api carries the HTTP admin surface shared by every syncnode
// handler — envelope, error codes, middleware. See design.md §5.1 + §9
// Phase E-1.
//
// All endpoints return the same JSON envelope:
//
//	{"code": <int>, "msg": "<string>", "data": <any>}
//
// HTTP status mirrors the error class (4xx for client mistakes, 5xx for
// server faults). Operators / clients should treat the `code` field as the
// stable identifier; `msg` is human-readable and can change between releases.
package api

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
)

// Response is the wire shape of every admin response. Empty payloads
// (Data == nil) marshal as `"data": null` which is fine for both success
// and error responses.
type Response struct {
	Code int         `json:"code"`
	Msg  string      `json:"msg"`
	Data interface{} `json:"data,omitempty"`
}

// CodeOK is the success sentinel. Distinct from zero so a future "no code
// set" bug stays detectable (operators page on Code != 0).
const CodeOK = 0

// Error codes (stable across releases — tests assert on the integer).
//
// 1000 series → config errors (defined in syncnode/errors.go).
// 2000 series → HTTP / admin API errors. Allocated here so handlers across
// rules + tasks + scheduler all share one namespace.
const (
	CodeBadRequest      = 2001 // malformed JSON / wrong content type
	CodeMissingField    = 2002 // required field absent
	CodeInvalidField    = 2003 // field present but value invalid
	CodeNotFound        = 2004 // resource id doesn't exist
	CodeConflict        = 2005 // resource already exists / state conflict
	CodeUnauthorized    = 2006 // auth middleware rejection
	CodeTooManyRequests = 2007 // backpressure (429)
	CodeMethodNotAllow  = 2008 // HTTP verb not supported on route
	CodeInternal        = 2010 // unexpected server-side fault
)

// APIError carries the HTTP status, the stable code, and a human message.
// Handlers return *APIError (or wrap one) and the WriteError helper renders
// it into the wire envelope with the correct status. Errors that aren't
// *APIError are mapped to CodeInternal / 500.
type APIError struct {
	Status int    // HTTP status code
	Code   int    // stable Code* constant
	Msg    string // human-readable; may include field context
}

func (e *APIError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("api error code=%d status=%d: %s", e.Code, e.Status, e.Msg)
}

// Newf constructs an *APIError with a formatted message.
func Newf(status, code int, format string, args ...interface{}) *APIError {
	return &APIError{Status: status, Code: code, Msg: fmt.Sprintf(format, args...)}
}

// Common-case helpers. They live as functions rather than vars so callers
// can attach context-specific messages without mutating shared state.

func ErrBadRequest(format string, args ...interface{}) *APIError {
	return Newf(http.StatusBadRequest, CodeBadRequest, format, args...)
}

func ErrMissingField(field string) *APIError {
	return Newf(http.StatusBadRequest, CodeMissingField, "missing required field: %s", field)
}

func ErrInvalidField(field, reason string) *APIError {
	return Newf(http.StatusBadRequest, CodeInvalidField, "invalid %s: %s", field, reason)
}

func ErrNotFound(resource, id string) *APIError {
	return Newf(http.StatusNotFound, CodeNotFound, "%s not found: %s", resource, id)
}

func ErrConflict(format string, args ...interface{}) *APIError {
	return Newf(http.StatusConflict, CodeConflict, format, args...)
}

func ErrInternal(format string, args ...interface{}) *APIError {
	return Newf(http.StatusInternalServerError, CodeInternal, format, args...)
}

// WriteOK serialises payload into the envelope and returns 200 OK. payload
// may be nil — the wire response will have `"data": null`.
func WriteOK(w http.ResponseWriter, payload interface{}) {
	writeResponse(w, http.StatusOK, Response{Code: CodeOK, Msg: "OK", Data: payload})
}

// WriteError renders an error into the envelope. If err is an *APIError its
// status + code are honoured; any other error becomes 500 / CodeInternal.
func WriteError(w http.ResponseWriter, err error) {
	if err == nil {
		WriteOK(w, nil)
		return
	}
	var apiErr *APIError
	if errors.As(err, &apiErr) && apiErr != nil {
		writeResponse(w, apiErr.Status, Response{Code: apiErr.Code, Msg: apiErr.Msg})
		return
	}
	writeResponse(w, http.StatusInternalServerError, Response{
		Code: CodeInternal,
		Msg:  err.Error(),
	})
}

func writeResponse(w http.ResponseWriter, status int, resp Response) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(resp)
}

// Handler is the HTTP handler shape every admin endpoint conforms to. It
// returns either a payload (rendered via WriteOK) or an error (rendered via
// WriteError). Centralising this means handlers don't repeat envelope
// boilerplate, and the auth/middleware layer can intercept errors uniformly.
type Handler func(r *http.Request) (interface{}, error)

// ToHTTPHandler adapts a Handler into stdlib's http.Handler. The returned
// function runs the auth middleware first, then dispatches.
func ToHTTPHandler(h Handler, mw ...Middleware) http.HandlerFunc {
	chained := Chain(h, mw...)
	return func(w http.ResponseWriter, r *http.Request) {
		payload, err := chained(r)
		if err != nil {
			WriteError(w, err)
			return
		}
		WriteOK(w, payload)
	}
}

// Middleware wraps a Handler. The Phase E-1 AuthMiddleware is a no-op hook;
// real implementations land in P2-H (JWT / TLS cert / shared token).
type Middleware func(Handler) Handler

// Chain composes a list of middlewares onto a base Handler. The first
// middleware in the slice is the outermost layer.
func Chain(h Handler, mw ...Middleware) Handler {
	for i := len(mw) - 1; i >= 0; i-- {
		h = mw[i](h)
	}
	return h
}

// SEC4: shared-token gate for the admin surface.
//
// The token is installed at server startup via SetAdminToken (or by tests
// directly). When the token is EMPTY the middleware is a passthrough —
// this preserves the P0/dev behaviour where every test that builds a
// SyncConfig without an adminToken still works.
//
// Wire format: `Authorization: Bearer <token>` OR `X-Sync-Token: <token>`.
// Both are accepted so operators can pick whichever fits their proxy /
// curl invocation. The compare is constant-time so a network attacker
// cannot derive the token from response-time differences.
//
// Threadsafe: the token slot is guarded by an RWMutex so a future
// SIGHUP-driven rotation can swap without restarting the listener.
var (
	adminTokenMu sync.RWMutex
	adminToken   string
)

// SetAdminToken installs the admin token. Passing an empty string
// disables auth. Safe to call concurrently.
func SetAdminToken(t string) {
	adminTokenMu.Lock()
	adminToken = t
	adminTokenMu.Unlock()
}

// getAdminToken returns the live admin token under the read lock.
func getAdminToken() string {
	adminTokenMu.RLock()
	defer adminTokenMu.RUnlock()
	return adminToken
}

// constantTimeEq compares two strings in O(len) time without leaking the
// match position via early-exit. Length mismatch is handled with a
// throwaway constant-time compare so two strings of different lengths
// still take the same amount of work.
func constantTimeEq(a, b string) bool {
	if len(a) != len(b) {
		// Do a dummy compare so the timing of "wrong length" matches the
		// timing of "right length, wrong value" — defends against the
		// length-leak case where an attacker can probe one byte at a time.
		_ = subtle.ConstantTimeCompare([]byte(a), []byte(a))
		return false
	}
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

// AuthMiddleware enforces the shared-token gate on admin requests. When
// the token slot is empty the middleware is a passthrough. When set, the
// request must carry the same token in either `Authorization: Bearer ...`
// or `X-Sync-Token: ...`.
func AuthMiddleware(next Handler) Handler {
	return func(r *http.Request) (interface{}, error) {
		tok := getAdminToken()
		if tok == "" {
			// Auth disabled — preserves dev/P0 behaviour for callers that
			// build a SyncConfig without an adminToken.
			return next(r)
		}
		provided := extractToken(r)
		if provided == "" || !constantTimeEq(provided, tok) {
			return nil, &APIError{
				Status: http.StatusUnauthorized,
				Code:   CodeUnauthorized,
				Msg:    "missing or invalid admin token",
			}
		}
		return next(r)
	}
}

// extractToken pulls the bearer / X-Sync-Token credential out of r.
// Returns "" when neither header carries a non-empty token.
func extractToken(r *http.Request) string {
	if v := r.Header.Get("Authorization"); v != "" {
		// Case-insensitive prefix match — RFC 7235 says the scheme is
		// case-insensitive, so accept "Bearer", "bearer", "BEARER".
		const bearer = "bearer "
		if len(v) > len(bearer) && strings.EqualFold(v[:len(bearer)], bearer) {
			return strings.TrimSpace(v[len(bearer):])
		}
	}
	return strings.TrimSpace(r.Header.Get("X-Sync-Token"))
}
