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

package chunk

import (
	"sync/atomic"
	"time"
)

// Chunk Stats
type ChunkStats struct {
	// counts
	TotalWriteCnt         uint64 `json:"total_write_cnt"`
	TotalReadCnt          uint64 `json:"total_read_cnt"`
	TotalRangeReadCnt     uint64 `json:"total_rangeread_cnt"`
	TotalReadShardMetaCnt uint64 `json:"total_readmeta_cnt"`
	TotalMarkDelCnt       uint64 `json:"total_markdel_cnt"`
	TotalDelCnt           uint64 `json:"total_del_cnt"`
	TotalListShardCnt     uint64 `json:"total_listshard_cnt"`
	// in queue
	WriteInque         int64 `json:"write_inque"`
	ReadInque          int64 `json:"read_inque"`
	RangeReadInque     int64 `json:"rangeread_inque"`
	ReadShardMetaInque int64 `json:"readmeta_inque"`
	MarkDelInque       int64 `json:"markdel_inque"`
	DelInque           int64 `json:"del_inque"`
	ListShardInque     int64 `json:"listshard_inque"`
	// bytes
	TotalReadBytes      uint64 `json:"total_read_bytes"`
	TotalRangeReadBytes uint64 `json:"total_rangeread_bytes"`
	TotalWriteBytes     uint64 `json:"total_write_bytes"`
	// latency
	TotalWriteDelay         uint64 `json:"total_write_delay"`
	TotalReadDelay          uint64 `json:"total_read_delay"`
	TotalRangeReadDelay     uint64 `json:"total_rangeread_delay"`
	TotalReadShardMetaDelay uint64 `json:"total_readmeta_delay"`
	TotalMarkDelDelay       uint64 `json:"total_markdel_delay"`
	TotalDelDelay           uint64 `json:"total_del_delay"`
	TotalListShardDelay     uint64 `json:"total_listshard_dalay"`
}

func (stat *ChunkStats) writeBefore() {
	atomic.AddInt64(&stat.WriteInque, 1)
}

func (stat *ChunkStats) writeAfter(size uint64, t time.Time) {
	atomic.AddInt64(&stat.WriteInque, -1)

	latency := time.Since(t).Nanoseconds()

	atomic.AddUint64(&stat.TotalWriteCnt, 1)
	atomic.AddUint64(&stat.TotalWriteBytes, uint64(size))
	atomic.AddUint64(&stat.TotalWriteDelay, uint64(latency))
}

func (stat *ChunkStats) readBefore() {
	atomic.AddInt64(&stat.ReadInque, 1)
}

func (stat *ChunkStats) readAfter(size uint64, t time.Time) {
	atomic.AddInt64(&stat.ReadInque, -1)

	latency := time.Since(t).Nanoseconds()

	atomic.AddUint64(&stat.TotalReadCnt, 1)
	atomic.AddUint64(&stat.TotalReadBytes, uint64(size))
	atomic.AddUint64(&stat.TotalReadDelay, uint64(latency))
}

func (stat *ChunkStats) rangereadBefore() {
	atomic.AddInt64(&stat.RangeReadInque, 1)
}

func (stat *ChunkStats) rangereadAfter(size uint64, t time.Time) {
	atomic.AddInt64(&stat.RangeReadInque, -1)

	latency := time.Since(t).Nanoseconds()

	atomic.AddUint64(&stat.TotalRangeReadCnt, 1)
	atomic.AddUint64(&stat.TotalRangeReadBytes, uint64(size))
	atomic.AddUint64(&stat.TotalRangeReadDelay, uint64(latency))
}

func (stat *ChunkStats) readmetaBefore() {
	atomic.AddInt64(&stat.ReadShardMetaInque, 1)
}

func (stat *ChunkStats) readmetaAfter(t time.Time) {
	atomic.AddInt64(&stat.ReadShardMetaInque, -1)

	latency := time.Since(t).Nanoseconds()

	atomic.AddUint64(&stat.TotalReadShardMetaCnt, 1)
	atomic.AddUint64(&stat.TotalReadShardMetaDelay, uint64(latency))
}

func (stat *ChunkStats) markdeleteBefore() {
	atomic.AddInt64(&stat.MarkDelInque, 1)
}

func (stat *ChunkStats) markdeleteAfter(t time.Time) {
	atomic.AddInt64(&stat.MarkDelInque, -1)

	latency := time.Since(t).Nanoseconds()

	atomic.AddUint64(&stat.TotalMarkDelCnt, 1)
	atomic.AddUint64(&stat.TotalMarkDelDelay, uint64(latency))
}

func (stat *ChunkStats) deleteBefore() {
	atomic.AddInt64(&stat.DelInque, 1)
}

func (stat *ChunkStats) deleteAfter(t time.Time) {
	atomic.AddInt64(&stat.DelInque, -1)

	latency := time.Since(t).Nanoseconds()

	atomic.AddUint64(&stat.TotalDelCnt, 1)
	atomic.AddUint64(&stat.TotalDelDelay, uint64(latency))
}

func (stat *ChunkStats) listshardBefore() {
	atomic.AddInt64(&stat.ListShardInque, 1)
}

func (stat *ChunkStats) listshardAfter(t time.Time) {
	atomic.AddInt64(&stat.ListShardInque, -1)

	latency := time.Since(t).Nanoseconds()

	atomic.AddUint64(&stat.TotalListShardCnt, 1)
	atomic.AddUint64(&stat.TotalListShardDelay, uint64(latency))
}
