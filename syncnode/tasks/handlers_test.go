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

package tasks

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/cubefs/cubefs/syncnode/spec"
	"github.com/cubefs/cubefs/syncnode/api"
	"github.com/cubefs/cubefs/syncnode/backend"
	"github.com/cubefs/cubefs/syncnode/executor"
	"github.com/gorilla/mux"
)

// envelope mirrors api.Response with a concrete Data field for tests.
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

func doReq(t *testing.T, router *mux.Router, method, target string) (int, envelope) {
	t.Helper()
	req := httptest.NewRequest(method, target, nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec.Code, decodeEnvelope(t, rec.Body.Bytes())
}

// newHandlerHarness wires Runner + Handlers + Router with a configurable
// backend factory.
func newHandlerHarness(t *testing.T, build func(ep *spec.EndpointConfig) (backend.Backend, error)) (*Runner, *stubRuleLookup, *memoryStore, *mux.Router) {
	t.Helper()
	exec := executor.New(executor.WithProgressInterval(20 * time.Millisecond))
	t.Cleanup(func() { _ = exec.Close() })
	store := NewMemoryStore()
	t.Cleanup(func() { _ = store.Close() })
	lookup := newStubRuleLookup()
	builder := &stubBackendBuilder{factory: build}
	runner := NewRunner(exec, store, lookup, builder)
	handlers := NewHandlers(runner, store)
	router := mux.NewRouter()
	handlers.Register(router)
	return runner, lookup, store, router
}

func TestHandlers_TriggerUnknownRule(t *testing.T) {
	_, _, _, router := newHandlerHarness(t, func(*spec.EndpointConfig) (backend.Backend, error) {
		return &emptyBackend{}, nil
	})
	status, env := doReq(t, router, http.MethodPost, "/admin/sync/task/trigger?ruleID=ghost")
	if status != http.StatusNotFound {
		t.Errorf("status = %d, want 404", status)
	}
	if env.Code != api.CodeNotFound {
		t.Errorf("code = %d, want %d", env.Code, api.CodeNotFound)
	}
}

func TestHandlers_TriggerMissingRuleID(t *testing.T) {
	_, _, _, router := newHandlerHarness(t, func(*spec.EndpointConfig) (backend.Backend, error) {
		return &emptyBackend{}, nil
	})
	status, env := doReq(t, router, http.MethodPost, "/admin/sync/task/trigger")
	if status != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", status)
	}
	if env.Code != api.CodeMissingField {
		t.Errorf("code = %d, want %d", env.Code, api.CodeMissingField)
	}
}

func TestHandlers_TriggerNoWait(t *testing.T) {
	_, lookup, store, router := newHandlerHarness(t, func(*spec.EndpointConfig) (backend.Backend, error) {
		return &blockingBackend{}, nil
	})
	lookup.put(newSyncRule("r1"))

	status, env := doReq(t, router, http.MethodPost, "/admin/sync/task/trigger?ruleID=r1")
	if status != http.StatusOK || env.Code != api.CodeOK {
		t.Fatalf("status=%d code=%d msg=%s", status, env.Code, env.Msg)
	}
	var rec Record
	if err := json.Unmarshal(env.Data, &rec); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if rec.Status != executor.StatusRunning {
		t.Errorf("Status = %q, want running", rec.Status)
	}
	if rec.RuleID != "r1" {
		t.Errorf("RuleID = %q", rec.RuleID)
	}

	// Cleanup the dangling task.
	_, _ = doReq(t, router, http.MethodPost, "/admin/sync/task/cancel?id="+rec.TaskID)
	waitForStatus(t, store, rec.TaskID, executor.StatusCancelled, 3*time.Second)
}

func TestHandlers_TriggerWaitReturnsTerminal(t *testing.T) {
	_, lookup, _, router := newHandlerHarness(t, func(*spec.EndpointConfig) (backend.Backend, error) {
		return &emptyBackend{}, nil
	})
	lookup.put(newSyncRule("r1"))

	status, env := doReq(t, router, http.MethodPost, "/admin/sync/task/trigger?ruleID=r1&wait=true")
	if status != http.StatusOK || env.Code != api.CodeOK {
		t.Fatalf("status=%d code=%d", status, env.Code)
	}
	var rec Record
	_ = json.Unmarshal(env.Data, &rec)
	if rec.Status != executor.StatusDone {
		t.Errorf("Status = %q, want done", rec.Status)
	}
}

func TestHandlers_SaveTypeMismatch(t *testing.T) {
	_, lookup, _, router := newHandlerHarness(t, func(*spec.EndpointConfig) (backend.Backend, error) {
		return &emptyBackend{}, nil
	})
	lookup.put(newLoadRule("load1"))

	status, env := doReq(t, router, http.MethodPost, "/admin/sync/task/save?ruleID=load1")
	if status != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", status)
	}
	if env.Code != api.CodeInvalidField {
		t.Errorf("code = %d, want %d", env.Code, api.CodeInvalidField)
	}
}

func TestHandlers_SaveSuccess(t *testing.T) {
	_, lookup, _, router := newHandlerHarness(t, func(*spec.EndpointConfig) (backend.Backend, error) {
		return &emptyBackend{}, nil
	})
	lookup.put(newSyncRule("sync1"))

	status, env := doReq(t, router, http.MethodPost, "/admin/sync/task/save?ruleID=sync1&wait=true")
	if status != http.StatusOK || env.Code != api.CodeOK {
		t.Fatalf("status=%d code=%d", status, env.Code)
	}
	var rec Record
	_ = json.Unmarshal(env.Data, &rec)
	if rec.Type != executor.TaskTypeSync {
		t.Errorf("Type = %q", rec.Type)
	}
}

func TestHandlers_LoadSuccess(t *testing.T) {
	_, lookup, _, router := newHandlerHarness(t, func(*spec.EndpointConfig) (backend.Backend, error) {
		return &emptyBackend{}, nil
	})
	lookup.put(newLoadRule("load1"))

	status, env := doReq(t, router, http.MethodPost, "/admin/sync/task/load?ruleID=load1&wait=true")
	if status != http.StatusOK || env.Code != api.CodeOK {
		t.Fatalf("status=%d code=%d", status, env.Code)
	}
	var rec Record
	_ = json.Unmarshal(env.Data, &rec)
	if rec.Type != executor.TaskTypeLoad {
		t.Errorf("Type = %q", rec.Type)
	}
}

func TestHandlers_LoadTypeMismatch(t *testing.T) {
	_, lookup, _, router := newHandlerHarness(t, func(*spec.EndpointConfig) (backend.Backend, error) {
		return &emptyBackend{}, nil
	})
	lookup.put(newSyncRule("sync1"))

	status, env := doReq(t, router, http.MethodPost, "/admin/sync/task/load?ruleID=sync1")
	if status != http.StatusBadRequest || env.Code != api.CodeInvalidField {
		t.Errorf("status=%d code=%d", status, env.Code)
	}
}

func TestHandlers_ListEmpty(t *testing.T) {
	_, _, _, router := newHandlerHarness(t, func(*spec.EndpointConfig) (backend.Backend, error) {
		return &emptyBackend{}, nil
	})
	status, env := doReq(t, router, http.MethodGet, "/admin/sync/task/list")
	if status != http.StatusOK || env.Code != api.CodeOK {
		t.Fatalf("status=%d code=%d", status, env.Code)
	}
	var recs []*Record
	if err := json.Unmarshal(env.Data, &recs); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(recs) != 0 {
		t.Errorf("len = %d, want 0", len(recs))
	}
}

func TestHandlers_ListWithStatusFilter(t *testing.T) {
	_, lookup, _, router := newHandlerHarness(t, func(*spec.EndpointConfig) (backend.Backend, error) {
		return &emptyBackend{}, nil
	})
	lookup.put(newSyncRule("r1"))
	// Three successful runs.
	for i := 0; i < 3; i++ {
		status, _ := doReq(t, router, http.MethodPost, "/admin/sync/task/trigger?ruleID=r1&wait=true")
		if status != http.StatusOK {
			t.Fatalf("seed %d: %d", i, status)
		}
	}

	_, env := doReq(t, router, http.MethodGet, "/admin/sync/task/list?status=done")
	var recs []*Record
	_ = json.Unmarshal(env.Data, &recs)
	if len(recs) != 3 {
		t.Errorf("done filter len = %d, want 3", len(recs))
	}
	for _, r := range recs {
		if r.Status != executor.StatusDone {
			t.Errorf("rec.Status = %q", r.Status)
		}
	}

	_, env = doReq(t, router, http.MethodGet, "/admin/sync/task/list?status=running")
	_ = json.Unmarshal(env.Data, &recs)
	if len(recs) != 0 {
		t.Errorf("running filter len = %d, want 0", len(recs))
	}
}

func TestHandlers_GetUnknown(t *testing.T) {
	_, _, _, router := newHandlerHarness(t, func(*spec.EndpointConfig) (backend.Backend, error) {
		return &emptyBackend{}, nil
	})
	status, env := doReq(t, router, http.MethodGet, "/admin/sync/task/get?id=ghost")
	if status != http.StatusNotFound || env.Code != api.CodeNotFound {
		t.Errorf("status=%d code=%d", status, env.Code)
	}
}

func TestHandlers_GetMissingID(t *testing.T) {
	_, _, _, router := newHandlerHarness(t, func(*spec.EndpointConfig) (backend.Backend, error) {
		return &emptyBackend{}, nil
	})
	status, env := doReq(t, router, http.MethodGet, "/admin/sync/task/get")
	if status != http.StatusBadRequest || env.Code != api.CodeMissingField {
		t.Errorf("status=%d code=%d", status, env.Code)
	}
}

func TestHandlers_GetSuccess(t *testing.T) {
	_, lookup, _, router := newHandlerHarness(t, func(*spec.EndpointConfig) (backend.Backend, error) {
		return &emptyBackend{}, nil
	})
	lookup.put(newSyncRule("r1"))
	_, env := doReq(t, router, http.MethodPost, "/admin/sync/task/trigger?ruleID=r1&wait=true")
	var seeded Record
	_ = json.Unmarshal(env.Data, &seeded)

	status, env := doReq(t, router, http.MethodGet, "/admin/sync/task/get?id="+seeded.TaskID)
	if status != http.StatusOK {
		t.Fatalf("status = %d", status)
	}
	var got Record
	_ = json.Unmarshal(env.Data, &got)
	if got.TaskID != seeded.TaskID || got.Status != executor.StatusDone {
		t.Errorf("got %+v", got)
	}
}

func TestHandlers_CancelKnown(t *testing.T) {
	_, lookup, store, router := newHandlerHarness(t, func(*spec.EndpointConfig) (backend.Backend, error) {
		return &blockingBackend{}, nil
	})
	lookup.put(newSyncRule("r1"))

	_, env := doReq(t, router, http.MethodPost, "/admin/sync/task/trigger?ruleID=r1")
	var seeded Record
	_ = json.Unmarshal(env.Data, &seeded)

	status, env := doReq(t, router, http.MethodPost, "/admin/sync/task/cancel?id="+seeded.TaskID)
	if status != http.StatusOK || env.Code != api.CodeOK {
		t.Fatalf("status=%d code=%d", status, env.Code)
	}
	waitForStatus(t, store, seeded.TaskID, executor.StatusCancelled, 2*time.Second)
}

func TestHandlers_CancelUnknown(t *testing.T) {
	_, _, _, router := newHandlerHarness(t, func(*spec.EndpointConfig) (backend.Backend, error) {
		return &emptyBackend{}, nil
	})
	status, env := doReq(t, router, http.MethodPost, "/admin/sync/task/cancel?id=ghost")
	if status != http.StatusNotFound || env.Code != api.CodeNotFound {
		t.Errorf("status=%d code=%d", status, env.Code)
	}
}

func TestHandlers_CancelMissingID(t *testing.T) {
	_, _, _, router := newHandlerHarness(t, func(*spec.EndpointConfig) (backend.Backend, error) {
		return &emptyBackend{}, nil
	})
	status, env := doReq(t, router, http.MethodPost, "/admin/sync/task/cancel")
	if status != http.StatusBadRequest || env.Code != api.CodeMissingField {
		t.Errorf("status=%d code=%d", status, env.Code)
	}
}

func TestHandlers_RetryUnknown(t *testing.T) {
	_, _, _, router := newHandlerHarness(t, func(*spec.EndpointConfig) (backend.Backend, error) {
		return &emptyBackend{}, nil
	})
	status, env := doReq(t, router, http.MethodPost, "/admin/sync/task/retry?id=ghost")
	if status != http.StatusNotFound || env.Code != api.CodeNotFound {
		t.Errorf("status=%d code=%d", status, env.Code)
	}
}

func TestHandlers_RetrySuccess(t *testing.T) {
	_, lookup, store, router := newHandlerHarness(t, func(*spec.EndpointConfig) (backend.Backend, error) {
		return &emptyBackend{}, nil
	})
	lookup.put(newSyncRule("r1"))
	_, env := doReq(t, router, http.MethodPost, "/admin/sync/task/trigger?ruleID=r1&wait=true")
	var first Record
	_ = json.Unmarshal(env.Data, &first)

	status, env := doReq(t, router, http.MethodPost, "/admin/sync/task/retry?id="+first.TaskID)
	if status != http.StatusOK || env.Code != api.CodeOK {
		t.Fatalf("status=%d code=%d msg=%s", status, env.Code, env.Msg)
	}
	var retry Record
	_ = json.Unmarshal(env.Data, &retry)
	if retry.TaskID == first.TaskID {
		t.Errorf("retry id = original id %q", retry.TaskID)
	}
	waitForStatus(t, store, retry.TaskID, executor.StatusDone, 2*time.Second)
}

func TestHandlers_MethodNotAllowed(t *testing.T) {
	_, _, _, router := newHandlerHarness(t, func(*spec.EndpointConfig) (backend.Backend, error) {
		return &emptyBackend{}, nil
	})
	req := httptest.NewRequest(http.MethodGet, "/admin/sync/task/trigger", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", rec.Code)
	}
}

// failingListStore exercises the generic error path in handleList.
type failingListStore struct {
	Store
}

func (f *failingListStore) List(context.Context, executor.Status) ([]*Record, error) {
	return nil, errBoom
}

var errBoom = boomErr{}

type boomErr struct{}

func (boomErr) Error() string { return "boom" }

func TestHandlers_ListInternalError(t *testing.T) {
	exec := executor.New()
	t.Cleanup(func() { _ = exec.Close() })
	store := NewMemoryStore()
	t.Cleanup(func() { _ = store.Close() })
	lookup := newStubRuleLookup()
	builder := &stubBackendBuilder{factory: func(*spec.EndpointConfig) (backend.Backend, error) {
		return &emptyBackend{}, nil
	}}
	runner := NewRunner(exec, store, lookup, builder)
	handlers := NewHandlers(runner, &failingListStore{Store: store})
	router := mux.NewRouter()
	handlers.Register(router)

	status, env := doReq(t, router, http.MethodGet, "/admin/sync/task/list")
	if status != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", status)
	}
	if env.Code != api.CodeInternal {
		t.Errorf("code = %d", env.Code)
	}
}
