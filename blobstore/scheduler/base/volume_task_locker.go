// Copyright 2022 The CubeFS Authors.
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

package base

import (
	"context"
	"errors"
	"sync"

	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

// make sure only one task in same volume to run in cluster
var (
	// ErrVidTaskConflict vid task conflict
	ErrVidTaskConflict = errors.New("vid task conflict")
)

// VolTaskLocker volume task locker
type VolTaskLocker struct {
	taskMap map[proto.Vid]struct{}
	mu      sync.Mutex
}

// TryLock try lock task volume and return error if there is task doing
func (m *VolTaskLocker) TryLock(ctx context.Context, vid proto.Vid) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	span := trace.SpanFromContextSafe(ctx)
	span.Infof("vid %d mutex try lock", vid)

	if _, ok := m.taskMap[vid]; ok {
		return ErrVidTaskConflict
	}
	m.taskMap[vid] = struct{}{}
	return nil
}

// Unlock unlock task volume
func (m *VolTaskLocker) Unlock(ctx context.Context, vid proto.Vid) {
	m.mu.Lock()
	defer m.mu.Unlock()

	span := trace.SpanFromContextSafe(ctx)
	span.Infof("vid %d mutex unlock", vid)

	delete(m.taskMap, vid)
}

var volTaskLocker *VolTaskLocker

// NewVolTaskLockerOnce singleton mode:make sure only one instance in global
var NewVolTaskLockerOnce sync.Once

// VolTaskLockerInst ensure that only one background task is executing on the same volume
func VolTaskLockerInst() *VolTaskLocker {
	NewVolTaskLockerOnce.Do(func() {
		volTaskLocker = &VolTaskLocker{
			taskMap: make(map[proto.Vid]struct{}),
		}
	})
	return volTaskLocker
}
