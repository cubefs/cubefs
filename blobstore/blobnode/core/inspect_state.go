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

	// MaxInspectBadBids bounds the per-chunk bad-bid map so a pathological disk
	// cannot grow the persisted inspect state without limit.
	MaxInspectBadBids = 1000
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
	AddBadBid(ctx context.Context, vuid proto.Vuid, bid proto.BlobID, meta BadBidMeta) (added bool, err error)
	DeleteBadBid(ctx context.Context, vuid proto.Vuid, bid proto.BlobID) (cleared bool, err error)
}
