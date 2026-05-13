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
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/cubefs/cubefs/syncnode/spec"
	"github.com/cubefs/cubefs/syncnode/api"
	"github.com/gorilla/mux"
)

// envelope mirrors api.Response but with a concrete Data field for tests.
type envelope struct {
	Code int             `json:"code"`
	Msg  string          `json:"msg"`
	Data json.RawMessage `json:"data,omitempty"`
}

func decodeEnvelope(t *testing.T, body []byte) envelope {
	t.Helper()
	var env envelope
	if err := json.Unmarshal(body, &env); err != nil {
		t.Fatalf("decode: %v body=%s", err, string(body))
	}
	return env
}

// newTestHandlers wires up a Handlers + mux.Router for table-driven tests.
// Using the real router means every assertion exercises auth middleware +
// method matching, not just the inner handler function.
func newTestHandlers(t *testing.T) (*Handlers, *mux.Router) {
	t.Helper()
	store := NewMemoryStore()
	t.Cleanup(func() { _ = store.Close() })
	h := NewHandlers(store)
	router := mux.NewRouter()
	h.Register(router)
	return h, router
}

// doJSON issues an in-memory request and returns status + decoded envelope.
func doJSON(t *testing.T, router *mux.Router, method, target string, body interface{}) (int, envelope) {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatalf("encode body: %v", err)
		}
	}
	req := httptest.NewRequest(method, target, &buf)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec.Code, decodeEnvelope(t, rec.Body.Bytes())
}

func sampleRuleConfig(id string) spec.RuleConfig {
	return spec.RuleConfig{
		ID:   id,
		Type: "sync",
		Src:  spec.EndpointConfig{Kind: "cfs", Vol: "v1", Path: "/x/" + id},
		Dst:  spec.EndpointConfig{Kind: "s3", Bucket: "b1", Prefix: "p/" + id},
	}
}

func TestHandlers_List_Empty(t *testing.T) {
	_, router := newTestHandlers(t)
	status, env := doJSON(t, router, http.MethodGet, "/admin/sync/rule/list", nil)
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if env.Code != api.CodeOK {
		t.Errorf("code = %d, want %d", env.Code, api.CodeOK)
	}
	var rules []*Rule
	if err := json.Unmarshal(env.Data, &rules); err != nil {
		t.Fatalf("decode data: %v (raw=%s)", err, string(env.Data))
	}
	if len(rules) != 0 {
		t.Errorf("len = %d, want 0", len(rules))
	}
}

func TestHandlers_CreateGetCycle(t *testing.T) {
	_, router := newTestHandlers(t)

	status, env := doJSON(t, router, http.MethodPost, "/admin/sync/rule/create", sampleRuleConfig("r1"))
	if status != http.StatusOK || env.Code != api.CodeOK {
		t.Fatalf("create: status=%d code=%d msg=%s", status, env.Code, env.Msg)
	}
	var created Rule
	if err := json.Unmarshal(env.Data, &created); err != nil {
		t.Fatalf("decode create data: %v", err)
	}
	if created.Config.ID != "r1" {
		t.Errorf("ID = %q, want r1", created.Config.ID)
	}
	if created.State != StateActive {
		t.Errorf("State = %q, want active", created.State)
	}

	status, env = doJSON(t, router, http.MethodGet, "/admin/sync/rule/get?id=r1", nil)
	if status != http.StatusOK || env.Code != api.CodeOK {
		t.Fatalf("get: status=%d code=%d msg=%s", status, env.Code, env.Msg)
	}
	var got Rule
	_ = json.Unmarshal(env.Data, &got)
	if got.Config.ID != "r1" || got.Config.Type != "sync" {
		t.Errorf("got %+v", got.Config)
	}
}

func TestHandlers_CreateDuplicate(t *testing.T) {
	_, router := newTestHandlers(t)
	if status, env := doJSON(t, router, http.MethodPost, "/admin/sync/rule/create", sampleRuleConfig("dup")); status != 200 || env.Code != api.CodeOK {
		t.Fatalf("seed: %d %d", status, env.Code)
	}
	status, env := doJSON(t, router, http.MethodPost, "/admin/sync/rule/create", sampleRuleConfig("dup"))
	if status != http.StatusConflict {
		t.Errorf("status = %d, want 409", status)
	}
	if env.Code != api.CodeConflict {
		t.Errorf("code = %d, want %d", env.Code, api.CodeConflict)
	}
	if !strings.Contains(env.Msg, "dup") {
		t.Errorf("msg = %q, missing id", env.Msg)
	}
}

func TestHandlers_GetUnknown(t *testing.T) {
	_, router := newTestHandlers(t)
	status, env := doJSON(t, router, http.MethodGet, "/admin/sync/rule/get?id=ghost", nil)
	if status != http.StatusNotFound {
		t.Errorf("status = %d, want 404", status)
	}
	if env.Code != api.CodeNotFound {
		t.Errorf("code = %d, want %d", env.Code, api.CodeNotFound)
	}
}

func TestHandlers_GetMissingID(t *testing.T) {
	_, router := newTestHandlers(t)
	status, env := doJSON(t, router, http.MethodGet, "/admin/sync/rule/get", nil)
	if status != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", status)
	}
	if env.Code != api.CodeMissingField {
		t.Errorf("code = %d, want %d", env.Code, api.CodeMissingField)
	}
}

func TestHandlers_UpdateExisting(t *testing.T) {
	_, router := newTestHandlers(t)
	_, _ = doJSON(t, router, http.MethodPost, "/admin/sync/rule/create", sampleRuleConfig("r"))

	updated := sampleRuleConfig("r")
	updated.Type = "load"
	status, env := doJSON(t, router, http.MethodPost, "/admin/sync/rule/update", updated)
	if status != http.StatusOK || env.Code != api.CodeOK {
		t.Fatalf("update: status=%d code=%d msg=%s", status, env.Code, env.Msg)
	}
	var got Rule
	_ = json.Unmarshal(env.Data, &got)
	if got.Config.Type != "load" {
		t.Errorf("Type = %q, want load", got.Config.Type)
	}
}

func TestHandlers_UpdateUnknown(t *testing.T) {
	_, router := newTestHandlers(t)
	status, env := doJSON(t, router, http.MethodPost, "/admin/sync/rule/update", sampleRuleConfig("ghost"))
	if status != http.StatusNotFound {
		t.Errorf("status = %d, want 404", status)
	}
	if env.Code != api.CodeNotFound {
		t.Errorf("code = %d, want %d", env.Code, api.CodeNotFound)
	}
}

func TestHandlers_DeleteCycle(t *testing.T) {
	_, router := newTestHandlers(t)
	_, _ = doJSON(t, router, http.MethodPost, "/admin/sync/rule/create", sampleRuleConfig("kill"))

	status, env := doJSON(t, router, http.MethodPost, "/admin/sync/rule/delete?id=kill", nil)
	if status != http.StatusOK || env.Code != api.CodeOK {
		t.Fatalf("delete: status=%d code=%d", status, env.Code)
	}

	status, env = doJSON(t, router, http.MethodGet, "/admin/sync/rule/get?id=kill", nil)
	if status != http.StatusNotFound {
		t.Errorf("post-delete get status = %d, want 404", status)
	}
	if env.Code != api.CodeNotFound {
		t.Errorf("post-delete code = %d", env.Code)
	}
}

func TestHandlers_DeleteUnknown(t *testing.T) {
	_, router := newTestHandlers(t)
	status, env := doJSON(t, router, http.MethodPost, "/admin/sync/rule/delete?id=ghost", nil)
	if status != http.StatusNotFound || env.Code != api.CodeNotFound {
		t.Errorf("status=%d code=%d", status, env.Code)
	}
}

func TestHandlers_PauseResume(t *testing.T) {
	_, router := newTestHandlers(t)
	_, _ = doJSON(t, router, http.MethodPost, "/admin/sync/rule/create", sampleRuleConfig("r"))

	status, env := doJSON(t, router, http.MethodPost, "/admin/sync/rule/pause?id=r", nil)
	if status != http.StatusOK || env.Code != api.CodeOK {
		t.Fatalf("pause: status=%d code=%d", status, env.Code)
	}
	var paused Rule
	_ = json.Unmarshal(env.Data, &paused)
	if paused.State != StatePaused {
		t.Errorf("State = %q, want paused", paused.State)
	}

	status, env = doJSON(t, router, http.MethodPost, "/admin/sync/rule/resume?id=r", nil)
	if status != http.StatusOK || env.Code != api.CodeOK {
		t.Fatalf("resume: status=%d code=%d", status, env.Code)
	}
	var resumed Rule
	_ = json.Unmarshal(env.Data, &resumed)
	if resumed.State != StateActive {
		t.Errorf("State = %q, want active", resumed.State)
	}
}

func TestHandlers_PauseUnknown(t *testing.T) {
	_, router := newTestHandlers(t)
	status, env := doJSON(t, router, http.MethodPost, "/admin/sync/rule/pause?id=ghost", nil)
	if status != http.StatusNotFound || env.Code != api.CodeNotFound {
		t.Errorf("status=%d code=%d", status, env.Code)
	}
}

func TestHandlers_CreateEmptyID(t *testing.T) {
	_, router := newTestHandlers(t)
	cfg := sampleRuleConfig("")
	status, env := doJSON(t, router, http.MethodPost, "/admin/sync/rule/create", cfg)
	if status != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", status)
	}
	if env.Code != api.CodeMissingField {
		t.Errorf("code = %d, want %d", env.Code, api.CodeMissingField)
	}
}

func TestHandlers_CreateEmptyType(t *testing.T) {
	_, router := newTestHandlers(t)
	cfg := sampleRuleConfig("r")
	cfg.Type = ""
	status, env := doJSON(t, router, http.MethodPost, "/admin/sync/rule/create", cfg)
	if status != http.StatusBadRequest || env.Code != api.CodeMissingField {
		t.Errorf("status=%d code=%d", status, env.Code)
	}
}

func TestHandlers_CreateMalformedJSON(t *testing.T) {
	_, router := newTestHandlers(t)
	req := httptest.NewRequest(http.MethodPost, "/admin/sync/rule/create",
		strings.NewReader("{not json"))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
	env := decodeEnvelope(t, rec.Body.Bytes())
	if env.Code != api.CodeBadRequest {
		t.Errorf("code = %d, want %d", env.Code, api.CodeBadRequest)
	}
}

func TestHandlers_MethodNotAllowed(t *testing.T) {
	// Wrong verb on a POST endpoint must be rejected by mux's method matcher.
	_, router := newTestHandlers(t)
	req := httptest.NewRequest(http.MethodGet, "/admin/sync/rule/create", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", rec.Code)
	}
}

func TestHandlers_ListReturnsAllAfterCreates(t *testing.T) {
	_, router := newTestHandlers(t)
	for _, id := range []string{"a", "b", "c"} {
		if status, env := doJSON(t, router, http.MethodPost, "/admin/sync/rule/create", sampleRuleConfig(id)); status != 200 || env.Code != api.CodeOK {
			t.Fatalf("create %s: status=%d code=%d", id, status, env.Code)
		}
	}
	_, env := doJSON(t, router, http.MethodGet, "/admin/sync/rule/list", nil)
	var rules []*Rule
	if err := json.Unmarshal(env.Data, &rules); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(rules) != 3 {
		t.Errorf("len = %d, want 3", len(rules))
	}
	// Sorted by ID.
	want := []string{"a", "b", "c"}
	for i, r := range rules {
		if r.Config.ID != want[i] {
			t.Errorf("[%d] = %q, want %q", i, r.Config.ID, want[i])
		}
	}
}

// failingStore wraps the in-memory store but errors on List/Get to exercise
// the generic-error → ErrInternal mapping in handlers.
type failingStore struct {
	Store
	failList bool
	failGet  bool
}

func (f *failingStore) List(ctx context.Context) ([]*Rule, error) {
	if f.failList {
		return nil, errBoom
	}
	return f.Store.List(ctx)
}

func (f *failingStore) Get(ctx context.Context, id string) (*Rule, error) {
	if f.failGet {
		return nil, errBoom
	}
	return f.Store.Get(ctx, id)
}

var errBoom = boomErr{}

type boomErr struct{}

func (boomErr) Error() string { return "boom" }

func TestHandlers_InternalErrorMapping(t *testing.T) {
	mem := NewMemoryStore()
	_ = mem.Create(context.Background(), newTestRule("r"))
	store := &failingStore{Store: mem, failList: true}
	h := NewHandlers(store)
	router := mux.NewRouter()
	h.Register(router)

	status, env := doJSON(t, router, http.MethodGet, "/admin/sync/rule/list", nil)
	if status != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", status)
	}
	if env.Code != api.CodeInternal {
		t.Errorf("code = %d, want %d", env.Code, api.CodeInternal)
	}
}


// -----------------------------------------------------------------------
// E-4 conflict-detection wiring (handler-level)
// -----------------------------------------------------------------------

// TestHandlers_Create_RejectsStructuralDuplicate exercises the E-4 path:
// two rules with distinct IDs but identical src+dst trip the
// duplicate-pair detector, returning CodeConflict with the 1014 code.
func TestHandlers_Create_RejectsStructuralDuplicate(t *testing.T) {
	_, router := newTestHandlers(t)
	original := spec.RuleConfig{
		ID:   "r1",
		Type: "sync",
		Src:  spec.EndpointConfig{Kind: "cfs", Vol: "v1", Path: "/x"},
		Dst:  spec.EndpointConfig{Kind: "s3", Bucket: "b1", Prefix: "p"},
	}
	twin := original
	twin.ID = "r2"

	if status, env := doJSON(t, router, http.MethodPost, "/admin/sync/rule/create", original); status != 200 || env.Code != api.CodeOK {
		t.Fatalf("seed r1: status=%d code=%d msg=%s", status, env.Code, env.Msg)
	}
	status, env := doJSON(t, router, http.MethodPost, "/admin/sync/rule/create", twin)
	if status != http.StatusConflict {
		t.Errorf("status = %d, want 409", status)
	}
	if env.Code != ErrCodeDuplicateRulePair {
		t.Errorf("code = %d, want %d", env.Code, ErrCodeDuplicateRulePair)
	}
}

// TestHandlers_Create_RejectsPrefixOverlap: same backend pair, src.path on
// the new rule is a prefix of an existing one.
func TestHandlers_Create_RejectsPrefixOverlap(t *testing.T) {
	_, router := newTestHandlers(t)
	parent := spec.RuleConfig{
		ID:   "parent",
		Type: "sync",
		Src:  spec.EndpointConfig{Kind: "cfs", Vol: "v1", Path: "/data"},
		Dst:  spec.EndpointConfig{Kind: "s3", Bucket: "b1", Prefix: "p"},
	}
	child := parent
	child.ID = "child"
	child.Src.Path = "/data/sub" // strict prefix of /data
	child.Dst.Prefix = "p/sub"   // strict prefix of p

	if status, _ := doJSON(t, router, http.MethodPost, "/admin/sync/rule/create", parent); status != 200 {
		t.Fatalf("seed parent failed status=%d", status)
	}
	status, env := doJSON(t, router, http.MethodPost, "/admin/sync/rule/create", child)
	if status != http.StatusConflict {
		t.Errorf("status = %d, want 409", status)
	}
	if env.Code != ErrCodePrefixOverlap {
		t.Errorf("code = %d, want %d", env.Code, ErrCodePrefixOverlap)
	}
}

// TestHandlers_Create_RejectsCycleSync: A: cfs→s3 + B: s3→cfs inverse pair.
func TestHandlers_Create_RejectsCycleSync(t *testing.T) {
	_, router := newTestHandlers(t)
	a := spec.RuleConfig{
		ID:   "a-to-b",
		Type: "sync",
		Src:  spec.EndpointConfig{Kind: "cfs", Vol: "v1", Path: "/x"},
		Dst:  spec.EndpointConfig{Kind: "s3", Bucket: "b1", Prefix: "p"},
	}
	b := spec.RuleConfig{
		ID:   "b-to-a",
		Type: "sync",
		Src:  spec.EndpointConfig{Kind: "s3", Bucket: "b1", Prefix: "p"},
		Dst:  spec.EndpointConfig{Kind: "cfs", Vol: "v1", Path: "/x"},
	}
	if status, _ := doJSON(t, router, http.MethodPost, "/admin/sync/rule/create", a); status != 200 {
		t.Fatalf("seed a failed status=%d", status)
	}
	status, env := doJSON(t, router, http.MethodPost, "/admin/sync/rule/create", b)
	if status != http.StatusConflict {
		t.Errorf("status = %d, want 409", status)
	}
	if env.Code != ErrCodeCycleSync {
		t.Errorf("code = %d, want %d", env.Code, ErrCodeCycleSync)
	}
}

// TestHandlers_Update_SelfReplaceAllowed: updating a rule to the same shape
// (including same src+dst) MUST NOT trip the duplicate-pair detector — the
// rule is replacing itself.
func TestHandlers_Update_SelfReplaceAllowed(t *testing.T) {
	_, router := newTestHandlers(t)
	cfg := sampleRuleConfig("self")
	if status, _ := doJSON(t, router, http.MethodPost, "/admin/sync/rule/create", cfg); status != 200 {
		t.Fatalf("seed failed status=%d", status)
	}
	status, env := doJSON(t, router, http.MethodPost, "/admin/sync/rule/update", cfg)
	if status != 200 || env.Code != api.CodeOK {
		t.Errorf("self-replace update should pass: status=%d code=%d msg=%s", status, env.Code, env.Msg)
	}
}
