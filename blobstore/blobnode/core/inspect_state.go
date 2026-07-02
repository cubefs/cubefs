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

package core

import (
	"context"
	"errors"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/proto"
)

const (
	CycleDayDuration            = 24 * time.Hour
	DefaultInspectBatchReadSize = 16 << 20 // 16 MB
	DefaultInspectCycleDays     = 90
)

// ErrInspectStopped is the shared control-stop sentinel of the data-inspect flow,
// returned when the switch is off, the disk is closing, or the service is closing.
// It lives in core so both the disk scan engine and the service-layer scheduler
// can recognize it with errors.Is.
var ErrInspectStopped = errors.New("inspect: stopped")

// InspectStateStore is the narrow per-disk inspect progress API.
// DiskAPI exposes it via InspectState()
// DataInspectMgr and inspect query handlers are the intended callers.
type InspectStateStore interface {
	LoadInspectDiskState(ctx context.Context) (st InspectDiskState, err error)
	LoadInspectChunkState(ctx context.Context, vuid proto.Vuid) (st InspectChunkState, err error)
	RangeInspectChunkState(ctx context.Context, fn func(st *InspectChunkState) bool) error
	StoreInspectDiskState(ctx context.Context, st InspectDiskState) error
	StoreInspectChunkState(ctx context.Context, st InspectChunkState) error
	FlushInspectState(ctx context.Context)
}

// CycleExpired reports whether the current inspect cycle has passed its hard
// deadline: cycleDays days after CycleStartAt. A cycle that has never started
// (CycleStartAt == 0) is never expired.
func (st InspectDiskState) CycleExpired(cycleDays int) bool {
	deadline := time.Duration(cycleDays) * CycleDayDuration
	return st.CycleElapsed() >= deadline
}

// CycleElapsed returns the time elapsed since the current cycle started, or 0
// when the cycle has never started (CycleStartAt == 0).
func (st InspectDiskState) CycleElapsed() time.Duration {
	if st.CycleStartAt == 0 {
		return 0
	}
	return time.Since(time.Unix(0, st.CycleStartAt))
}

// CycleCnt==-1 means has not been counted.
func (st InspectChunkState) NeedCount() bool {
	return st.CycleCnt < 0
}

// CycleDone derives scan completion from the current snapshot: the chunk has been
// counted and either holds no shards (CycleCnt == 0) or the cursor reached the
// cycle's snapshot bound (CycleMaxBid).
func (st InspectChunkState) CycleDone() bool {
	return st.CycleCnt >= 0 && (st.CycleCnt == 0 || st.Cursor >= st.CycleMaxBid)
}

// ResetForCycle starts a fresh scan window for the given cycle, preserving the
// bad-bid memory across cycle resets.
func (st *InspectChunkState) ResetForCycle(cycleID uint64) {
	st.CycleID = cycleID
	st.Cursor = proto.InValidBlobID
	st.CycleMaxBid = 0
	st.CycleCnt = -1
	st.CycleScanned = 0
}
