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
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/cubefs/cubefs/syncnode/api"
	"github.com/cubefs/cubefs/syncnode/spec"
	"github.com/gorilla/mux"
)

// Handlers bundles the rule CRUD HTTP endpoints around a Store.
// Construct one per process (the Store should also be process-singleton)
// and call Register on the router during server startup.
type Handlers struct {
	store Store
}

// NewHandlers returns Handlers bound to the given Store. The Store is
// retained, not copied; the caller owns its lifecycle (including Close).
func NewHandlers(store Store) *Handlers {
	return &Handlers{store: store}
}

// Register wires every rule CRUD endpoint onto router under
// "/admin/sync/rule/...". Each handler is wrapped with the package-level
// AuthMiddleware so request-shape changes propagate uniformly.
func (h *Handlers) Register(router *mux.Router) {
	router.HandleFunc("/admin/sync/rule/list", api.ToHTTPHandler(h.handleList, api.AuthMiddleware)).Methods(http.MethodGet)
	router.HandleFunc("/admin/sync/rule/get", api.ToHTTPHandler(h.handleGet, api.AuthMiddleware)).Methods(http.MethodGet)
	router.HandleFunc("/admin/sync/rule/create", api.ToHTTPHandler(h.handleCreate, api.AuthMiddleware)).Methods(http.MethodPost)
	router.HandleFunc("/admin/sync/rule/update", api.ToHTTPHandler(h.handleUpdate, api.AuthMiddleware)).Methods(http.MethodPost)
	router.HandleFunc("/admin/sync/rule/delete", api.ToHTTPHandler(h.handleDelete, api.AuthMiddleware)).Methods(http.MethodPost)
	router.HandleFunc("/admin/sync/rule/pause", api.ToHTTPHandler(h.handlePause, api.AuthMiddleware)).Methods(http.MethodPost)
	router.HandleFunc("/admin/sync/rule/resume", api.ToHTTPHandler(h.handleResume, api.AuthMiddleware)).Methods(http.MethodPost)
}

// handleList returns every rule in the store. Empty store yields an empty
// slice (NOT null) so clients can iterate without nil-checks.
func (h *Handlers) handleList(r *http.Request) (interface{}, error) {
	rules, err := h.store.List(r.Context())
	if err != nil {
		return nil, api.ErrInternal("list rules: %v", err)
	}
	if rules == nil {
		rules = []*Rule{}
	}
	return rules, nil
}

// handleGet fetches a single rule by ?id=.
func (h *Handlers) handleGet(r *http.Request) (interface{}, error) {
	id, err := requireQueryID(r)
	if err != nil {
		return nil, err
	}
	rule, err := h.store.Get(r.Context(), id)
	if err != nil {
		return nil, mapStoreErr(err, id)
	}
	return rule, nil
}

// handleCreate inserts a new rule from the POST body. The body is the
// RuleConfig JSON; the runtime fields (state, timestamps) are populated by
// the store. Duplicate IDs yield 409. The candidate rule is also run through
// the conflict validator (E-4) against the current store contents — prefix
// overlaps and cycle-sync configurations fail with CodePrefixOverlap or
// CodeCycleSync.
func (h *Handlers) handleCreate(r *http.Request) (interface{}, error) {
	cfg, err := decodeRuleConfig(r)
	if err != nil {
		return nil, err
	}
	if vErr := validateCreatePayload(cfg); vErr != nil {
		return nil, vErr
	}
	rule := NewRule(*cfg)
	if err := h.checkConflicts(r.Context(), rule, false); err != nil {
		return nil, err
	}
	if err := h.store.Create(r.Context(), rule); err != nil {
		return nil, mapStoreErr(err, cfg.ID)
	}
	// Re-read so the response carries normalised CreatedAt / UpdatedAt /
	// State from the store, not the caller-supplied copy.
	stored, err := h.store.Get(r.Context(), cfg.ID)
	if err != nil {
		return nil, mapStoreErr(err, cfg.ID)
	}
	return stored, nil
}

// handleUpdate replaces an existing rule. 404 if id is unknown. The candidate
// rule is run through the conflict validator (E-4); a rule cannot be updated
// into a shape that conflicts with another existing rule.
func (h *Handlers) handleUpdate(r *http.Request) (interface{}, error) {
	cfg, err := decodeRuleConfig(r)
	if err != nil {
		return nil, err
	}
	if vErr := validateCreatePayload(cfg); vErr != nil {
		return nil, vErr
	}
	updated := &Rule{Config: *cfg}
	if err := h.checkConflicts(r.Context(), updated, true); err != nil {
		return nil, err
	}
	if err := h.store.Update(r.Context(), updated); err != nil {
		return nil, mapStoreErr(err, cfg.ID)
	}
	stored, err := h.store.Get(r.Context(), cfg.ID)
	if err != nil {
		return nil, mapStoreErr(err, cfg.ID)
	}
	return stored, nil
}

// handleDelete removes a rule by ?id=.
func (h *Handlers) handleDelete(r *http.Request) (interface{}, error) {
	id, err := requireQueryID(r)
	if err != nil {
		return nil, err
	}
	if err := h.store.Delete(r.Context(), id); err != nil {
		return nil, mapStoreErr(err, id)
	}
	return map[string]string{"id": id}, nil
}

// handlePause sets state=paused on the rule. 404 if id is unknown.
func (h *Handlers) handlePause(r *http.Request) (interface{}, error) {
	return h.setStateHandler(r, StatePaused)
}

// handleResume sets state=active on the rule. 404 if id is unknown.
func (h *Handlers) handleResume(r *http.Request) (interface{}, error) {
	return h.setStateHandler(r, StateActive)
}

func (h *Handlers) setStateHandler(r *http.Request, st State) (interface{}, error) {
	id, err := requireQueryID(r)
	if err != nil {
		return nil, err
	}
	if err := h.store.SetState(r.Context(), id, st); err != nil {
		return nil, mapStoreErr(err, id)
	}
	stored, err := h.store.Get(r.Context(), id)
	if err != nil {
		return nil, mapStoreErr(err, id)
	}
	return stored, nil
}

// requireQueryID extracts the ?id= query parameter and returns a 400-class
// APIError if it's missing.
func requireQueryID(r *http.Request) (string, error) {
	id := r.URL.Query().Get("id")
	if id == "" {
		return "", api.ErrMissingField("id")
	}
	return id, nil
}

// decodeRuleConfig parses the POST body into a RuleConfig. Validates the
// content type loosely (JSON only) and returns a 400 on parse failure.
func decodeRuleConfig(r *http.Request) (*spec.RuleConfig, error) {
	if r.Body == nil {
		return nil, api.ErrBadRequest("empty request body")
	}
	defer r.Body.Close()
	var cfg spec.RuleConfig
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return nil, api.ErrBadRequest("decode body: %v", err)
	}
	return &cfg, nil
}

// validateCreatePayload runs the cheap E-2-scope validation: id + type
// must be non-empty. Full validation (allowed roots, cron, etc.) belongs
// to the config-parser path that fires in Phase E-4.
func validateCreatePayload(cfg *spec.RuleConfig) error {
	if cfg.ID == "" {
		return api.ErrMissingField("id")
	}
	if cfg.Type == "" {
		return api.ErrMissingField("type")
	}
	return nil
}

// checkConflicts runs the E-4 conflict validator against the current store
// contents augmented with the candidate rule. Rules with the same ID as the
// candidate are EXCLUDED from the comparison set so a same-id Create (or
// self-replace Update) doesn't double-fire as a duplicate-pair conflict —
// the store handles ID uniqueness with its own ErrRuleExists / CodeConflict.
//
// Conflicts surface as *api.APIError so the wire envelope carries the stable
// 1014-1016 codes plus the offending rule IDs.
func (h *Handlers) checkConflicts(ctx context.Context, candidate *Rule, isUpdate bool) error {
	_ = isUpdate // kept for API clarity; logic below covers both paths uniformly
	existing, err := h.store.List(ctx)
	if err != nil {
		return api.ErrInternal("list rules for conflict check: %v", err)
	}
	set := make([]*Rule, 0, len(existing)+1)
	for _, r := range existing {
		if r.ID() == candidate.ID() {
			continue
		}
		set = append(set, r)
	}
	set = append(set, candidate)

	if vErr := Validate(set); vErr != nil {
		var ce *ConflictError
		if errors.As(vErr, &ce) {
			return &api.APIError{
				Status: http.StatusConflict,
				Code:   ce.Code,
				Msg:    fmt.Sprintf("%s (rules: %v)", ce.Msg, ce.RuleIDs),
			}
		}
		return api.ErrConflict("%v", vErr)
	}
	return nil
}

// mapStoreErr translates the sentinel store errors into APIErrors so the
// envelope code matches the failure class.
func mapStoreErr(err error, id string) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, ErrRuleNotFound):
		return api.ErrNotFound("rule", id)
	case errors.Is(err, ErrRuleExists):
		return api.ErrConflict("rule already exists: %s", id)
	case errors.Is(err, ErrInvalidState):
		return api.ErrInvalidField("state", err.Error())
	default:
		return api.ErrInternal("%s", err.Error())
	}
}

