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

package bolt

import (
	"context"
	"errors"

	"github.com/cubefs/cubefs/syncnode/executor"
)

// ExecutorAdapter wraps a bolt-backed InProgressStore so it satisfies the
// executor.InProgressStore contract. The two interfaces share the same
// shape but exchange package-local Breakpoint structs; this thin shim
// translates between them and lets the executor package stay free of a
// dependency on bolt (avoiding the import cycle bolt→executor→bolt).
type ExecutorAdapter struct {
	Store InProgressStore
}

// AdaptForExecutor wraps the bolt.InProgressStore so it can be passed to
// executor.WithInProgressStore. Returns nil when s is nil so callers can
// invoke this unconditionally.
func AdaptForExecutor(s InProgressStore) executor.InProgressStore {
	if s == nil {
		return nil
	}
	return &ExecutorAdapter{Store: s}
}

// Put translates the executor breakpoint into the bolt schema and stores it.
func (a *ExecutorAdapter) Put(ctx context.Context, bp *executor.Breakpoint) error {
	if bp == nil {
		return errors.New("bolt: nil breakpoint")
	}
	return a.Store.Put(ctx, &Breakpoint{
		TaskID:    bp.TaskID,
		Key:       bp.Key,
		BytesDone: bp.BytesDone,
		UploadID:  bp.UploadID,
		UpdatedAt: bp.UpdatedAt,
	})
}

// Get pulls the bolt breakpoint and translates it into the executor schema.
// Returns ErrBreakpointNotFound passthrough so callers can errors.Is.
func (a *ExecutorAdapter) Get(ctx context.Context, key string) (*executor.Breakpoint, error) {
	bp, err := a.Store.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if bp == nil {
		return nil, ErrBreakpointNotFound
	}
	return &executor.Breakpoint{
		TaskID:    bp.TaskID,
		Key:       bp.Key,
		BytesDone: bp.BytesDone,
		UploadID:  bp.UploadID,
		UpdatedAt: bp.UpdatedAt,
	}, nil
}

// Delete forwards to the underlying store.
func (a *ExecutorAdapter) Delete(ctx context.Context, key string) error {
	return a.Store.Delete(ctx, key)
}
