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

//go:build !linux

package cfs

import (
	"errors"
	"testing"

	"github.com/cubefs/cubefs/syncnode/backend"
)

// On non-linux platforms the stub must:
//   1. Register itself under kind "cfs" via init()
//   2. Return ErrConfigInvalid from New() (wrapped, but errors.Is should
//      match)
//   3. Compile — these tests only run when GOOS != linux which is what
//      the build tag guards.

func TestStub_NewReturnsErrConfigInvalid(t *testing.T) {
	got, err := New(&Config{Masters: []string{"x"}, Volume: "v"})
	if got != nil {
		t.Fatalf("expected nil Backend, got %T", got)
	}
	if err == nil {
		t.Fatal("expected non-nil error from stub")
	}
	if !errors.Is(err, backend.ErrConfigInvalid) {
		t.Errorf("expected error to wrap ErrConfigInvalid, got %v", err)
	}
	if msg := err.Error(); msg == "" {
		t.Error("error message must be non-empty")
	}
}

func TestStub_RegisteredKind(t *testing.T) {
	// New() should be reachable via the registry; on non-linux it still
	// returns ErrConfigInvalid but the wiring through Register must work.
	b, err := backend.New("cfs", &Config{})
	if b != nil {
		t.Fatalf("expected nil Backend via registry, got %T", b)
	}
	if !errors.Is(err, backend.ErrConfigInvalid) {
		t.Errorf("registry New(cfs) should return ErrConfigInvalid, got %v", err)
	}
}

func TestStub_ConfigZeroValueOK(t *testing.T) {
	// The stub must not panic on a zero-value config; callers may build
	// it from a JSON unmarshal that produces empty slices.
	_, _ = New(&Config{})
}
