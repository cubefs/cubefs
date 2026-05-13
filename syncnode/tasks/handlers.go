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
	"errors"
	"net/http"

	"github.com/cubefs/cubefs/syncnode/api"
	"github.com/cubefs/cubefs/syncnode/executor"
	"github.com/cubefs/cubefs/syncnode/rules"
	"github.com/gorilla/mux"
)

// Handlers bundles the task HTTP endpoints around a Runner + Store.
// Construct one per process and call Register on the router during server
// startup. The Runner already owns store + executor; the store reference
// here is for read-only handlers (list/get) that don't need the runner.
type Handlers struct {
	runner *Runner
	store  Store
}

// NewHandlers returns Handlers bound to runner + store. Both must be
// non-nil. The lifecycle of both objects belongs to the caller.
func NewHandlers(runner *Runner, store Store) *Handlers {
	return &Handlers{runner: runner, store: store}
}

// Register wires every task endpoint onto router under
// "/admin/sync/task/...". Each handler is wrapped with the package-level
// AuthMiddleware so request-shape changes propagate uniformly.
func (h *Handlers) Register(router *mux.Router) {
	router.HandleFunc("/admin/sync/task/trigger", api.ToHTTPHandler(h.handleTrigger, api.AuthMiddleware)).Methods(http.MethodPost)
	router.HandleFunc("/admin/sync/task/save", api.ToHTTPHandler(h.handleSave, api.AuthMiddleware)).Methods(http.MethodPost)
	router.HandleFunc("/admin/sync/task/load", api.ToHTTPHandler(h.handleLoad, api.AuthMiddleware)).Methods(http.MethodPost)
	router.HandleFunc("/admin/sync/task/list", api.ToHTTPHandler(h.handleList, api.AuthMiddleware)).Methods(http.MethodGet)
	router.HandleFunc("/admin/sync/task/get", api.ToHTTPHandler(h.handleGet, api.AuthMiddleware)).Methods(http.MethodGet)
	router.HandleFunc("/admin/sync/task/cancel", api.ToHTTPHandler(h.handleCancel, api.AuthMiddleware)).Methods(http.MethodPost)
	router.HandleFunc("/admin/sync/task/retry", api.ToHTTPHandler(h.handleRetry, api.AuthMiddleware)).Methods(http.MethodPost)
}

// handleTrigger runs the rule's task type once. ?ruleID= is required;
// ?wait=true blocks until terminal status (or HTTP ctx cancellation).
func (h *Handlers) handleTrigger(r *http.Request) (interface{}, error) {
	ruleID, err := requireQueryParam(r, "ruleID")
	if err != nil {
		return nil, err
	}
	wait := r.URL.Query().Get("wait") == "true"
	rec, err := h.runner.Trigger(r.Context(), ruleID, wait)
	if err != nil {
		return nil, mapRunnerErr(err, ruleID)
	}
	return rec, nil
}

// handleSave is an alias for trigger when rule.type == sync.
func (h *Handlers) handleSave(r *http.Request) (interface{}, error) {
	return h.triggerAs(r, executor.TaskTypeSync)
}

// handleLoad is an alias for trigger when rule.type == load.
func (h *Handlers) handleLoad(r *http.Request) (interface{}, error) {
	return h.triggerAs(r, executor.TaskTypeLoad)
}

func (h *Handlers) triggerAs(r *http.Request, want executor.TaskType) (interface{}, error) {
	ruleID, err := requireQueryParam(r, "ruleID")
	if err != nil {
		return nil, err
	}
	wait := r.URL.Query().Get("wait") == "true"
	rec, err := h.runner.TriggerAs(r.Context(), ruleID, want, wait)
	if err != nil {
		return nil, mapRunnerErr(err, ruleID)
	}
	return rec, nil
}

// handleList returns every record matching the optional ?status= filter.
// Empty store yields []*Record{} (not null) so clients can iterate.
func (h *Handlers) handleList(r *http.Request) (interface{}, error) {
	filter := executor.Status(r.URL.Query().Get("status"))
	recs, err := h.store.List(r.Context(), filter)
	if err != nil {
		return nil, api.ErrInternal("list tasks: %v", err)
	}
	if recs == nil {
		recs = []*Record{}
	}
	return recs, nil
}

// handleGet fetches a single record by ?id=.
func (h *Handlers) handleGet(r *http.Request) (interface{}, error) {
	id, err := requireQueryParam(r, "id")
	if err != nil {
		return nil, err
	}
	rec, err := h.store.Get(r.Context(), id)
	if err != nil {
		return nil, mapStoreErr(err, id)
	}
	return rec, nil
}

// handleCancel signals executor.Cancel on the task and returns 200 OK
// immediately; the actual status flip happens asynchronously when the
// executor's goroutine notices ctx.Done.
func (h *Handlers) handleCancel(r *http.Request) (interface{}, error) {
	id, err := requireQueryParam(r, "id")
	if err != nil {
		return nil, err
	}
	if err := h.runner.Cancel(r.Context(), id); err != nil {
		return nil, mapStoreErr(err, id)
	}
	return map[string]string{"taskID": id, "status": "cancelling"}, nil
}

// handleRetry re-runs a failed / cancelled task with a fresh taskID. The
// original record is preserved.
func (h *Handlers) handleRetry(r *http.Request) (interface{}, error) {
	id, err := requireQueryParam(r, "id")
	if err != nil {
		return nil, err
	}
	rec, err := h.runner.Retry(r.Context(), id)
	if err != nil {
		return nil, mapStoreErr(err, id)
	}
	return rec, nil
}

// requireQueryParam returns the named query parameter or a 400-class
// APIError if it's missing / empty.
func requireQueryParam(r *http.Request, name string) (string, error) {
	v := r.URL.Query().Get(name)
	if v == "" {
		return "", api.ErrMissingField(name)
	}
	return v, nil
}

// mapStoreErr translates store sentinel errors to APIErrors so the envelope
// code matches the failure class.
func mapStoreErr(err error, id string) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, ErrTaskNotFound):
		return api.ErrNotFound("task", id)
	case errors.Is(err, rules.ErrRuleNotFound):
		return api.ErrNotFound("rule", id)
	default:
		return api.ErrInternal("%s", err.Error())
	}
}

// mapRunnerErr translates Runner / rules-package errors to APIErrors. Same
// table as mapStoreErr plus the rule-type-mismatch case for /save and /load.
func mapRunnerErr(err error, id string) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, rules.ErrRuleNotFound):
		return api.ErrNotFound("rule", id)
	case errors.Is(err, ErrTaskNotFound):
		return api.ErrNotFound("task", id)
	case errors.Is(err, ErrRuleTypeMismatch):
		return api.ErrInvalidField("ruleID", err.Error())
	default:
		return api.ErrInternal("%s", err.Error())
	}
}
