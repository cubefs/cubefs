// Copyright 2018 The CubeFS Authors.
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

package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadNodeIDFromFile_NotFound(t *testing.T) {
	dir := t.TempDir()
	id, ok := LoadNodeIDFromFile(dir)
	if ok || id != 0 {
		t.Fatalf("expected (0, false), got (%v, %v)", id, ok)
	}
}

func TestPersistAndLoadNodeID(t *testing.T) {
	dir := t.TempDir()
	nodeID := uint64(12345)
	if err := PersistNodeIDToFile(dir, nodeID); err != nil {
		t.Fatalf("PersistNodeIDToFile: %v", err)
	}
	loaded, ok := LoadNodeIDFromFile(dir)
	if !ok {
		t.Fatal("LoadNodeIDFromFile: expected ok true")
	}
	if loaded != nodeID {
		t.Fatalf("expected nodeID %v, got %v", nodeID, loaded)
	}
}

func TestLoadNodeIDFromFile_InvalidContent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, NodeIDFile)
	if err := os.WriteFile(path, []byte("not-a-number"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	id, ok := LoadNodeIDFromFile(dir)
	if ok || id != 0 {
		t.Fatalf("expected (0, false) for invalid content, got (%v, %v)", id, ok)
	}
}
