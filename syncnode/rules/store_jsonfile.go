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
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// rulesFileName is the basename used inside the configured directory for
// the persisted rule map.
const rulesFileName = "rules.json"

// jsonFileStore persists the rule map to disk as a single JSON file. Every
// mutating operation re-serialises the full map to "<dir>/rules.json" using
// a tmp-rename atomic write. Reads are served from the embedded memory
// cache so the hot path stays lock-free of disk I/O.
//
// This is the Phase E-2 default; Phase F-2 will replace this with BoltDB
// without disturbing the Store interface.
type jsonFileStore struct {
	mem  *memoryStore
	dir  string
	path string
	// writeMu serialises file writes; the embedded memoryStore handles its
	// own RWMutex for the in-memory snapshot.
	writeMu sync.Mutex
}

// NewJSONFileStore opens (or creates) the rule store at dir. The directory
// is created if missing. If the on-disk file exists it is loaded and any
// JSON parse error is returned so a corrupt store doesn't silently start
// empty.
func NewJSONFileStore(dir string) (*jsonFileStore, error) {
	if dir == "" {
		return nil, errors.New("jsonFileStore: dir is required")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create rules dir: %w", err)
	}
	s := &jsonFileStore{
		mem:  NewMemoryStore(),
		dir:  dir,
		path: filepath.Join(dir, rulesFileName),
	}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

// load reads the JSON file (if present) and populates the in-memory map.
// Missing file is treated as empty; parse errors are surfaced.
func (s *jsonFileStore) load() error {
	raw, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read %s: %w", s.path, err)
	}
	if len(raw) == 0 {
		return nil
	}
	var byID map[string]*Rule
	if err := json.Unmarshal(raw, &byID); err != nil {
		return fmt.Errorf("parse %s: %w", s.path, err)
	}
	// Direct map assignment; nothing else can race during constructor.
	s.mem.rules = byID
	return nil
}

// persist serialises the current in-memory map to disk via tmp + rename.
// Caller must hold writeMu. The memory store's RLock is taken to grab a
// consistent snapshot.
func (s *jsonFileStore) persist() error {
	s.mem.mu.RLock()
	// Sort keys so the on-disk file is deterministic — helps diffs in ops
	// reviews and keeps tests stable.
	ids := make([]string, 0, len(s.mem.rules))
	for id := range s.mem.rules {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	ordered := make(map[string]*Rule, len(ids))
	for _, id := range ids {
		ordered[id] = s.mem.rules[id]
	}
	raw, err := json.MarshalIndent(ordered, "", "  ")
	s.mem.mu.RUnlock()
	if err != nil {
		return fmt.Errorf("marshal rules: %w", err)
	}

	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return fmt.Errorf("write tmp file: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename tmp file: %w", err)
	}
	return nil
}

// List proxies to the in-memory store.
func (s *jsonFileStore) List(ctx context.Context) ([]*Rule, error) {
	return s.mem.List(ctx)
}

// Get proxies to the in-memory store.
func (s *jsonFileStore) Get(ctx context.Context, id string) (*Rule, error) {
	return s.mem.Get(ctx, id)
}

// Create writes through to memory then persists. On persist failure the
// in-memory create is rolled back so disk and memory stay in sync.
func (s *jsonFileStore) Create(ctx context.Context, r *Rule) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.mem.Create(ctx, r); err != nil {
		return err
	}
	if err := s.persist(); err != nil {
		_ = s.mem.Delete(ctx, r.Config.ID)
		return err
	}
	return nil
}

// Update writes through to memory then persists. On persist failure the
// previous snapshot is restored.
func (s *jsonFileStore) Update(ctx context.Context, r *Rule) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	// Snapshot the existing record so we can roll back on persist failure.
	prev, err := s.mem.Get(ctx, r.Config.ID)
	if err != nil {
		return err
	}
	if err := s.mem.Update(ctx, r); err != nil {
		return err
	}
	if err := s.persist(); err != nil {
		_ = s.mem.Update(ctx, prev)
		return err
	}
	return nil
}

// Delete writes through to memory then persists. On persist failure the
// removed record is reinserted.
func (s *jsonFileStore) Delete(ctx context.Context, id string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	prev, err := s.mem.Get(ctx, id)
	if err != nil {
		return err
	}
	if err := s.mem.Delete(ctx, id); err != nil {
		return err
	}
	if err := s.persist(); err != nil {
		_ = s.mem.Create(ctx, prev)
		return err
	}
	return nil
}

// SetState writes through to memory then persists.
func (s *jsonFileStore) SetState(ctx context.Context, id string, st State) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	prev, err := s.mem.Get(ctx, id)
	if err != nil {
		return err
	}
	if err := s.mem.SetState(ctx, id, st); err != nil {
		return err
	}
	if err := s.persist(); err != nil {
		_ = s.mem.SetState(ctx, id, prev.State)
		return err
	}
	return nil
}

// UpdateLastRun writes through to memory then persists.
func (s *jsonFileStore) UpdateLastRun(ctx context.Context, id string, last LastRunSummary) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	prev, err := s.mem.Get(ctx, id)
	if err != nil {
		return err
	}
	if err := s.mem.UpdateLastRun(ctx, id, last); err != nil {
		return err
	}
	if err := s.persist(); err != nil {
		// Restore previous last-run fields on rollback.
		_ = s.mem.UpdateLastRun(ctx, id, LastRunSummary{
			At:     prev.LastRunAt,
			Status: prev.LastRunStatus,
			Error:  prev.LastRunError,
		})
		return err
	}
	return nil
}

// Close is a no-op — every mutation already flushed to disk synchronously.
func (s *jsonFileStore) Close() error { return nil }
