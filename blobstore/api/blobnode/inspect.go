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

package blobnode

import (
	"context"
	"fmt"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/proxy"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/recordlog"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
)

type InspectRateArgs struct {
	Rate int `json:"rate"`
}

type InspectCycleArgs struct {
	Days int `json:"days"`
}

type InspectCleanMetricArgs struct {
	DiskID proto.DiskID `json:"diskid"`
}

type DataInspectConf struct {
	IntervalSec   int   `json:"interval_sec"`
	RateLimit     int   `json:"rate_limit"`
	BatchReadSize int64 `json:"batch_read_size"` // max data bytes per BatchRead call; default 16MB
	CycleDays     int   `json:"cycle_days"`      // full inspect cycle length in days; default 90

	Proxy  proxy.LbConfig   `json:"proxy"` // used to send crc repair messages.
	Record recordlog.Config `json:"record"`
}

type DataInspectStat struct {
	DataInspectConf
	Open bool `json:"open"` // data_inspect enabled
}

// InspectDiskState is the per-disk persistent inspect cycle, stored by
// core/disk.SuperBlock via the shared kv meta store.
type InspectDiskState struct {
	DiskID       proto.DiskID `json:"disk_id"`
	CycleStartAt int64        `json:"cycle_start_at"` // UnixNano; 0 = not initialized
	CycleID      uint64       `json:"cycle_id"`       // per-disk inspect cycle
}

// CycleExpired reports whether the current inspect cycle has passed its hard
// deadline: cycleDays days after CycleStartAt. A cycle that has never started
// (CycleStartAt == 0) is never expired.
func (st InspectDiskState) CycleExpired(cycleDays int) bool {
	deadline := time.Duration(cycleDays) * (24 * time.Hour)
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

// BadBidMeta is the per-bid metadata kept in InspectChunkState.BadBids.
type BadBidMeta struct {
	FoundAt int64  `json:"found_at"`         // UnixNano, the first time the bid was flagged bad
	Reason  string `json:"reason,omitempty"` // error text captured
}

// InspectChunkState is the per-chunk persistent inspect progress, keyed by vuid.
type InspectChunkState struct {
	Vuid proto.Vuid `json:"vuid"`

	CycleID      uint64       `json:"cycle_id"`      // inspect cycle this state belongs to
	Cursor       proto.BlobID `json:"cursor"`        // next ListShards start point; InValidBlobID means scan from the beginning
	CycleMaxBid  proto.BlobID `json:"cycle_max_bid"` // highest bid recorded by count-only mode; 0 means has not been counted yet
	CycleScanned int64        `json:"cycle_scanned"` // number of shards scanned so far in this cycle, used for window tuning only
	CycleCnt     int64        `json:"cycle_cnt"`     // shard count snapshot; -1 until count-only has run, >= 0 means counted

	// bad-bid memory, preserved across cycle resets
	BadBids map[proto.BlobID]BadBidMeta `json:"bad_bids,omitempty"` // bid -> metadata
}

// NeedCount reports whether count-only has not run yet for this cycle
// (CycleCnt == -1).
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

// BadShard is the inspect-domain bad shard result shared by the local inspect
// flow and the inspect HTTP/CLI response. Err is kept only for internal logging
// and error classification, and record reason, and is not exposed to users.
type BadShard struct {
	DiskID proto.DiskID `json:"diskid"`
	Vuid   proto.Vuid   `json:"vuid"`
	Bid    proto.BlobID `json:"bid"`
	Err    error        `json:"-"`
}

// ---------------------------------------------------------------------------
// HTTP client
// ---------------------------------------------------------------------------

func (c *client) InspectChunk(ctx context.Context, host string, args *ChunkInspectArgs) (ret []BadShard, err error) {
	urlStr := fmt.Sprintf("%v/chunk/inspect/diskid/%v/vuid/%v", host, args.DiskID, args.Vuid)
	ret = make([]BadShard, 0)
	err = c.PostWith(ctx, urlStr, &ret, rpc.NoneBody)
	return
}

func (c *client) GetInspectStat(ctx context.Context, host string) (stat *DataInspectStat, err error) {
	urlStr := fmt.Sprintf("%v/inspect/stat", host)
	stat = new(DataInspectStat)
	err = c.GetWith(ctx, urlStr, stat)
	return
}

func (c *client) GetInspectDiskState(ctx context.Context, host string, args *DiskStatArgs) (stat map[proto.DiskID]InspectDiskState, err error) {
	urlStr := fmt.Sprintf("%v/inspect/stat/diskid/%v", host, args.DiskID)
	stat = make(map[proto.DiskID]InspectDiskState)
	err = c.GetWith(ctx, urlStr, &stat)
	return
}

func (c *client) GetInspectChunkState(ctx context.Context, host string, args *ChunkInspectArgs) (stat map[proto.Vuid]InspectChunkState, err error) {
	urlStr := fmt.Sprintf("%v/inspect/stat/diskid/%v/vuid/%v", host, args.DiskID, args.Vuid)
	stat = make(map[proto.Vuid]InspectChunkState)
	err = c.GetWith(ctx, urlStr, &stat)
	return
}
