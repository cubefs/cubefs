// Copyright 2025 The CubeFS Authors.
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

package storage

import (
	"context"
	"math/rand"
	"sync/atomic"

	"github.com/cubefs/cubefs/blobstore/common/trace"
	snproto "github.com/cubefs/cubefs/blobstore/shardnode/proto"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/cubefs/cubefs/blobstore/util/statistic"
)

type MetaStatsConfig struct {
	RangeCount         int     `json:"-"`
	RequestSampleRatio float64 `json:"request_sample_ratio,omitempty"`
	TDigestCompression uint64  `json:"tdigest_compression,omitempty"`
}

type shardMetaStatsRecorder struct {
	cfg MetaStatsConfig
	v   atomic.Value
	r   []*statistic.DualTDigest // hash median recorder for each range
}

func newShardMetaStatsRecorder(cfg MetaStatsConfig) *shardMetaStatsRecorder {
	r := make([]*statistic.DualTDigest, cfg.RangeCount)
	for i := range r {
		r[i] = statistic.NewDualTDigest(cfg.TDigestCompression)
	}
	return &shardMetaStatsRecorder{
		v: atomic.Value{},
		r: r,
	}
}

// load load the stats from storage
func (m *shardMetaStatsRecorder) load(stats snproto.ShardMetaStats) error {
	m.v.Store(stats)
	if len(stats.PositiveTdigests) != len(m.r) ||
		len(stats.PositiveTdigests) != len(stats.NegativeTdigests) {
		return errors.New("tdigests snapshot count mismatch")
	}
	for i := range m.r {
		m.r[i] = statistic.NewDualTDigestFromSnapshot(
			m.cfg.TDigestCompression,
			stats.PositiveTdigests[i],
			stats.NegativeTdigests[i],
		)
	}
	return nil
}

// set save newest stats
func (m *shardMetaStatsRecorder) set(stats snproto.ShardMetaStats) {
	if stats.PositiveTdigests == nil {
		stats.PositiveTdigests = make([]statistic.TDigestSnapshot, len(m.r))
	}
	if stats.NegativeTdigests == nil {
		stats.NegativeTdigests = make([]statistic.TDigestSnapshot, len(m.r))
	}

	for i := range m.r {
		stats.PositiveTdigests[i], stats.NegativeTdigests[i] = m.r[i].Snapshot()
	}
	m.v.Store(stats)
}

func (m *shardMetaStatsRecorder) get() snproto.ShardMetaStats {
	if m == nil {
		return snproto.ShardMetaStats{}
	}

	val := m.v.Load()
	if val == nil {
		return snproto.ShardMetaStats{}
	}

	stats := val.(snproto.ShardMetaStats)
	if stats.SpaceMetaStats != nil {
		newMap := make(map[uint64]snproto.SpaceMetaStats, len(stats.SpaceMetaStats))
		for k, v := range stats.SpaceMetaStats {
			newMap[k] = v
		}
		stats.SpaceMetaStats = newMap
	}

	return stats
}

func (s *shard) updateMetaStatsByMetaOpUnit(ctx context.Context, metaOpUnits []*metaOpUnit, index uint64) {
	span := trace.SpanFromContextSafe(ctx)
	stats := s.metaStats.get()
	if stats.AppliedIndex >= index {
		return
	}
	stats.AppliedIndex = index
	for _, mou := range metaOpUnits {
		if mou == nil {
			continue
		}
		// update stats based on operation type first
		switch mou.op {
		case metaOpInsert:
			s.handleInsertOp(&stats, mou)
		case metaOpUpdate:
			s.handleUpdateOp(&stats, mou)
		case metaOpDelete:
			s.handleDeleteOp(&stats, mou)
		default:
			span.Errorf("unknown meta operation type: %d", mou.op)
		}
	}
	s.metaStats.set(stats)
}

// handleInsertOp handles insert operation for all data types
func (s *shard) handleInsertOp(stats *snproto.ShardMetaStats, mou *metaOpUnit) {
	for _, hashValue := range mou.hashValues {
		rd := rand.Float64()
		if hashValue > 0 && rd < s.metaStats.cfg.RequestSampleRatio {
			s.metaStats.r[hashValue].Add(hashValue)
		}
	}

	d := uint64(mou.keySize + mou.newValueSize)

	switch mou.dataType {
	case metaDataTypeItem:
		spaceStats := s.getOrCreateSpaceStats(stats, mou.spaceID)
		if spaceStats == nil {
			return
		}
		stats.ItemCount++
		spaceStats.ItemCount++
		stats.ItemSize += d
		spaceStats.ItemSize += d
		stats.SpaceMetaStats[mou.spaceID] = *spaceStats
	case metaDataTypeBlob:
		spaceStats := s.getOrCreateSpaceStats(stats, mou.spaceID)
		if spaceStats == nil {
			return
		}
		stats.BlobCount++
		spaceStats.BlobCount++
		stats.BlobSize += d
		spaceStats.BlobSize += d
		stats.SpaceMetaStats[mou.spaceID] = *spaceStats
	default:
		log.Errorf("unknown meta data type: %d", mou.dataType)
	}
}

// handleUpdateOp handles update operation for all data types
func (s *shard) handleUpdateOp(stats *snproto.ShardMetaStats, mou *metaOpUnit) {
	d := int64(mou.newValueSize - mou.oldValueSize)

	switch mou.dataType {
	case metaDataTypeItem:
		spaceStats := s.getOrCreateSpaceStats(stats, mou.spaceID)
		if spaceStats == nil {
			return
		}

		if d > 0 {
			spaceStats.ItemSize += uint64(d)
		} else {
			spaceStats.ItemSize -= uint64(-d)
		}
		stats.SpaceMetaStats[mou.spaceID] = *spaceStats
	case metaDataTypeBlob:
		spaceStats := s.getOrCreateSpaceStats(stats, mou.spaceID)
		if spaceStats == nil {
			return
		}

		if d > 0 {
			spaceStats.BlobSize += uint64(d)
		} else {
			spaceStats.BlobSize -= uint64(-d)
		}
		stats.SpaceMetaStats[mou.spaceID] = *spaceStats
	default:
		log.Errorf("unknown meta data type: %d", mou.dataType)
	}
}

// handleDeleteOp handles delete operation for all data types
func (s *shard) handleDeleteOp(stats *snproto.ShardMetaStats, mou *metaOpUnit) {
	rd := rand.Float64()
	for _, hashValue := range mou.hashValues {
		if hashValue > 0 && rd < s.metaStats.cfg.RequestSampleRatio {
			s.metaStats.r[hashValue].Remove(hashValue)
		}
	}
	d := uint64(mou.keySize + mou.oldValueSize)
	switch mou.dataType {
	case metaDataTypeItem:
		spaceStats := s.getOrCreateSpaceStats(stats, mou.spaceID)
		if spaceStats == nil {
			return
		}

		if stats.ItemCount > 0 {
			stats.ItemCount--
		}
		stats.ItemSize = safeSubtract(stats.ItemSize, d)

		if spaceStats.ItemCount > 0 {
			spaceStats.ItemCount--
		}
		spaceStats.ItemSize = safeSubtract(spaceStats.ItemSize, d)
		stats.SpaceMetaStats[mou.spaceID] = *spaceStats
	case metaDataTypeBlob:
		spaceStats := s.getOrCreateSpaceStats(stats, mou.spaceID)
		if spaceStats == nil {
			return
		}

		if stats.BlobCount > 0 {
			stats.BlobCount--
		}
		stats.BlobSize = safeSubtract(stats.BlobSize, d)

		if spaceStats.BlobCount > 0 {
			spaceStats.BlobCount--
		}
		spaceStats.BlobSize = safeSubtract(spaceStats.BlobSize, d)
		stats.SpaceMetaStats[mou.spaceID] = *spaceStats
	default:
		log.Errorf("unknown meta data type: %d", mou.dataType)
	}
}

// getOrCreateSpaceStats gets or creates space stats for Item and Blob types
func (s *shard) getOrCreateSpaceStats(stats *snproto.ShardMetaStats, spaceID uint64) *snproto.SpaceMetaStats {
	if spaceID == 0 {
		return nil
	}
	if stats.SpaceMetaStats == nil {
		stats.SpaceMetaStats = make(map[uint64]snproto.SpaceMetaStats)
	}
	spaceStats, ok := stats.SpaceMetaStats[spaceID]
	if !ok {
		spaceStats = snproto.SpaceMetaStats{}
	}
	return &spaceStats
}

// safeSubtract safely subtracts value from target, ensuring it doesn't go below 0
func safeSubtract(target uint64, value uint64) uint64 {
	if target >= value {
		target -= value
		return target
	}
	return 0
}
