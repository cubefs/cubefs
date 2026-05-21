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

package executor

import (
	"context"
	"time"
)

// breakpointKey composes the bolt key for an in-progress file breakpoint.
// Format: "<taskID>:<entryKey>". Single bucket, no schema migration needed.
func breakpointKey(taskID, entryKey string) string {
	return taskID + ":" + entryKey
}

// Breakpoint is the resume-from-N info for one in-flight file. It mirrors
// the bolt-backed schema (`bolt.Breakpoint`) but lives in the executor
// package so the executor does not depend on the bolt persistence layer.
// `bolt.inProgressStore` (or any other implementation) is plugged in via
// WithInProgressStore as long as it satisfies the InProgressStore interface
// below, with adapters bridging the two struct types.
type Breakpoint struct {
	TaskID    string
	Key       string
	BytesDone int64
	UploadID  string
	UpdatedAt time.Time
}

// InProgressStore is the minimal contract required by syncOneFile to persist
// per-file breakpoints (size + offset + s3 multipart UploadID). The bolt
// implementation in `syncnode/bolt` satisfies this via a thin adapter so the
// executor package remains free of a hard dependency on bolt.
//
// Wired to the executor via WithInProgressStore. The executor consults this
// store only when Task.ResumeEnabled is true; legacy callers and tests that
// don't opt into P2 leave it nil.
type InProgressStore interface {
	Put(ctx context.Context, bp *Breakpoint) error
	Get(ctx context.Context, key string) (*Breakpoint, error)
	Delete(ctx context.Context, key string) error
}
