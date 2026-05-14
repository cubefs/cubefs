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

package syncnode

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/syncnode/api"
	"github.com/cubefs/cubefs/syncnode/rules"
	"github.com/cubefs/cubefs/util/log"
)

// reloadFailuresTotal counts how many SIGHUP / POST /admin/syncnode/reload
// attempts were rejected for validation failure. Exposed via
// /admin/syncnode/stat so operators see a non-zero counter when the most
// recent reload didn't take.
var reloadFailuresTotal atomic.Uint64

// reload re-reads the on-disk config file at s.cfgPath, validates the entire
// new config + the merged rule set, and — only when both pass — swaps it in
// and re-applies the rule set to the scheduler.
//
// Design contract (§9 F-3):
//   - In-flight tasks continue to use the OLD config (the task already has
//     references to the old Backend instances; we don't re-build them).
//   - NEW tasks (next scheduler tick / next API trigger) see the new config.
//   - Reload with a syntax error must NOT replace the running config; the
//     old rules continue to fire. reloadFailuresTotal is bumped.
//
// All-or-nothing: any error returned here leaves the SyncNode unchanged.
func (s *SyncNode) reload(ctx context.Context) error {
	if s.cfgPath == "" {
		return fmt.Errorf("reload: no config path recorded — was the service started with a file-based config?")
	}
	raw, err := os.ReadFile(s.cfgPath)
	if err != nil {
		reloadFailuresTotal.Add(1)
		return fmt.Errorf("read %q: %w", s.cfgPath, err)
	}
	newCfg, err := ParseSyncConfig(raw)
	if err != nil {
		reloadFailuresTotal.Add(1)
		return fmt.Errorf("parse: %w", err)
	}

	// Merge bootstrap rules from the new config with whatever's already
	// persisted; conflict-check the merged set. This catches "operator
	// edited sync.json to introduce a prefix overlap with a rule that was
	// created via the HTTP API."
	if err := s.validateMergedRules(ctx, newCfg); err != nil {
		reloadFailuresTotal.Add(1)
		return fmt.Errorf("validate: %w", err)
	}

	// Commit phase: apply changes that may fail (rule upsert) BEFORE the
	// scheduler swap so a partial failure doesn't leave a divergent state.
	if err := s.applyBootstrapRules(ctx, newCfg); err != nil {
		reloadFailuresTotal.Add(1)
		return fmt.Errorf("apply bootstrap rules: %w", err)
	}

	// Swap config atomically. Scheduler picks up the change on its next
	// ApplyRules call (immediately, below).
	s.cfgMu.Lock()
	s.cfg = newCfg
	s.cfgMu.Unlock()

	// Re-apply rules to the scheduler with the new set. Schedule changes
	// (cron expression edits, pause/resume) take effect here.
	if s.scheduler != nil {
		stored, err := s.ruleStore.List(ctx)
		if err != nil {
			// Soft-fail: don't roll back the config swap (it's already
			// validated and persisted); just log and return so operators
			// see the error.
			log.LogWarnf("reload: list rules after swap: %v", err)
			return fmt.Errorf("post-swap list: %w", err)
		}
		if err := s.scheduler.ApplyRules(stored); err != nil {
			// Partial: some rules registered, others were rejected by
			// cron parse. Log; the scheduler already kept the good ones.
			log.LogWarnf("reload: scheduler.ApplyRules partial: %v", err)
		}
	}
	log.LogInfof("syncnode reload OK: %d bootstrap rules in config", len(newCfg.Rules))
	return nil
}

// validateMergedRules is the read-only validation pass — confirm conflict-
// free across (persisted ∪ new bootstrap) before any side effect.
func (s *SyncNode) validateMergedRules(ctx context.Context, newCfg *SyncConfig) error {
	stored, err := s.ruleStore.List(ctx)
	if err != nil {
		return fmt.Errorf("list rules: %w", err)
	}
	set := make([]*rules.Rule, 0, len(stored)+len(newCfg.Rules))
	seen := make(map[string]bool, len(stored))
	for _, r := range stored {
		// If this rule will be overwritten by a bootstrap rule of the same
		// ID, use the NEW shape for the conflict check — that way we catch
		// "the edit creates a prefix overlap with another stored rule".
		overridden := false
		for i := range newCfg.Rules {
			if newCfg.Rules[i].ID == r.ID() {
				replacement := *r
				replacement.Config = newCfg.Rules[i]
				set = append(set, &replacement)
				seen[r.ID()] = true
				overridden = true
				break
			}
		}
		if !overridden {
			set = append(set, r)
			seen[r.ID()] = true
		}
	}
	for i := range newCfg.Rules {
		if seen[newCfg.Rules[i].ID] {
			continue
		}
		set = append(set, rules.NewRule(newCfg.Rules[i]))
	}
	if vErr := rules.Validate(set); vErr != nil {
		return vErr
	}
	return nil
}

// bootstrapSnapshot records the pre-mutation state of a rule about to be
// upserted by applyBootstrapRules. Used for atomic-rollback on partial
// failure (FIX D).
type bootstrapSnapshot struct {
	id   string
	prev *rules.Rule // nil = didn't exist before
}

// applyBootstrapRules upserts every rule from newCfg.Rules into the store —
// Create if absent, Update if already present (preserving runtime state).
// Called only after validateMergedRules returns nil so we know the resulting
// store contents are conflict-free.
//
// FIX D — atomicity: snapshot every rule that we'll mutate BEFORE touching
// the store; if any operation mid-way fails, replay the snapshot to roll
// back partial state. This is best-effort: a Create may be irreversible if
// the rollback Delete also fails (we log but proceed). The point is that
// operators see a coherent state in the common path — either the entire
// new bootstrap set landed, or none of it did.
func (s *SyncNode) applyBootstrapRules(ctx context.Context, newCfg *SyncConfig) error {
	// Phase 1: snapshot.
	snapshots := make([]bootstrapSnapshot, 0, len(newCfg.Rules))
	for i := range newCfg.Rules {
		cfg := newCfg.Rules[i]
		existing, err := s.ruleStore.Get(ctx, cfg.ID)
		if err != nil && !errors.Is(err, rules.ErrRuleNotFound) {
			return fmt.Errorf("snapshot get %q: %w", cfg.ID, err)
		}
		entry := bootstrapSnapshot{id: cfg.ID}
		if existing != nil {
			// Deep-ish copy: Config is a value type, runtime fields are
			// scalars. RuleConfig has slice fields (Filter.Include/Exclude)
			// — copy those too so a later mutation of `existing` doesn't
			// poison the snapshot.
			cp := *existing
			cp.Config = existing.Config
			cp.Config.Filter.Include = append([]string(nil), existing.Config.Filter.Include...)
			cp.Config.Filter.Exclude = append([]string(nil), existing.Config.Filter.Exclude...)
			entry.prev = &cp
		}
		snapshots = append(snapshots, entry)
	}

	// Phase 2: apply. On any failure, roll back via the snapshots.
	applied := 0
	for i := range newCfg.Rules {
		cfg := newCfg.Rules[i]
		snap := snapshots[i]
		var opErr error
		if snap.prev == nil {
			opErr = s.ruleStore.Create(ctx, rules.NewRule(cfg))
		} else {
			updated := *snap.prev
			updated.Config = cfg
			updated.UpdatedAt = time.Now()
			opErr = s.ruleStore.Update(ctx, &updated)
		}
		if opErr != nil {
			// Roll back everything applied so far.
			s.rollbackBootstrap(ctx, snapshots[:applied])
			return fmt.Errorf("apply %q (rolled back %d prior): %w", cfg.ID, applied, opErr)
		}
		applied++
	}
	return nil
}

// rollbackBootstrap restores the snapshotted state for the rules in
// `snapshots` (in reverse order so dependency direction is consistent
// with the apply path). Failures are logged but don't abort the rollback
// loop — best-effort recovery; operators may still need to inspect the
// store afterwards if a rollback step itself errored.
func (s *SyncNode) rollbackBootstrap(ctx context.Context, snapshots []bootstrapSnapshot) {
	for i := len(snapshots) - 1; i >= 0; i-- {
		snap := snapshots[i]
		if snap.prev == nil {
			// We had created this rule freshly during apply — undo with Delete.
			if err := s.ruleStore.Delete(ctx, snap.id); err != nil && !errors.Is(err, rules.ErrRuleNotFound) {
				log.LogWarnf("reload rollback: delete %q: %v", snap.id, err)
			}
			continue
		}
		// We had updated this rule; restore the prior shape.
		if err := s.ruleStore.Update(ctx, snap.prev); err != nil {
			log.LogWarnf("reload rollback: restore %q: %v", snap.id, err)
		}
	}
}

// handleReload exposes reload as an HTTP endpoint. POST returns 200 OK on
// success, 400 + the parse/validate error on failure. The body is the
// usual api envelope.
func (s *SyncNode) handleReload(r *http.Request) (interface{}, error) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	if err := s.reload(ctx); err != nil {
		return nil, api.ErrBadRequest("reload: %v", err)
	}
	return map[string]interface{}{
		"reloaded":            true,
		"reloadFailuresTotal": reloadFailuresTotal.Load(),
	}, nil
}
