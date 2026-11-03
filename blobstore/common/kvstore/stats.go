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

package kvstore

import (
	"strconv"
	"strings"

	"github.com/cubefs/cubefs/blobstore/common/kvstat"
)

// Stats collects engine-level statistics from the RocksDB instance.
// For OpenDB (single CF): reads all CF-level properties from the default CF, no ColumnFamilies output.
// For OpenDBWithCF (multiple CFs): reads CF-level properties per CF, aggregates following §3 rules.
func (s *instance) Stats() (kvstat.Stats, error) {
	s.lock.RLock()
	cfCount := len(s.cfTables)
	// snapshot cfTables under lock
	tables := make(map[string]*table, cfCount)
	for name, t := range s.cfTables {
		tables[name] = t
	}
	s.lock.RUnlock()

	st := kvstat.Stats{}

	// LiveSstFilesSize: sum of all live SST file sizes (covers all CFs)
	meta := s.db.GetLiveFilesMetaData()
	for i := range meta {
		st.LiveSstFilesSize += uint64(meta[i].Size)
	}

	// DB-level / shared-cache / default-level properties
	st.MemoryUsage.BlockCacheCapacity = parseUintProp(s.db.GetProperty(kvstat.PropertyBlockCacheCapacity))
	st.MemoryUsage.BlockCacheUsage = parseUintProp(s.db.GetProperty(kvstat.PropertyBlockCacheUsage))
	st.MemoryUsage.BlockCachePinnedUsage = parseUintProp(s.db.GetProperty(kvstat.PropertyBlockCachePinnedUsage))

	// delayed write rate / write stop: DB-level
	st.ActualDelayedWriteRate = parseUintProp(s.db.GetProperty(kvstat.PropertyActualDelayedWriteRate))
	st.IsWriteStopped = parseBoolProp(s.db.GetProperty(kvstat.PropertyIsWriteStopped))

	// running flush/compaction: DB-level
	st.NumRunningFlushes = parseUintProp(s.db.GetProperty(kvstat.PropertyNumRunningFlushes))
	st.NumRunningCompactions = parseUintProp(s.db.GetProperty(kvstat.PropertyNumRunningCompactions))

	// background-errors: default CF level
	st.BackgroundErrors = parseUintProp(s.db.GetProperty(kvstat.PropertyBackgroundErrors))

	// snapshots: DB-level
	st.NumSnapshots = parseUintProp(s.db.GetProperty(kvstat.PropertyNumSnapshots))
	st.OldestSnapshotTime = parseUintProp(s.db.GetProperty(kvstat.PropertyOldestSnapshotTime))

	if cfCount == 0 {
		// OpenDB: single default CF, read all CF-level props from default
		st.MemoryUsage.EstimateTableReadersMem = parseUintProp(s.db.GetProperty(kvstat.PropertyEstimateTableReadersMem))
		st.MemoryUsage.CurSizeAllMemTables = parseUintProp(s.db.GetProperty(kvstat.PropertyCurSizeAllMemTables))
		st.NumImmutableMemTable = parseUintProp(s.db.GetProperty(kvstat.PropertyNumImmutableMemTable))
		st.NumFilesAtLevel0 = parseUintProp(s.db.GetProperty(kvstat.PropertyNumFilesAtLevel0))
		st.NumFilesAtLevel1 = parseUintProp(s.db.GetProperty(kvstat.PropertyNumFilesAtLevel1))
		st.MemTableFlushPending = parseBoolProp(s.db.GetProperty(kvstat.PropertyMemTableFlushPending))
		st.CompactionPending = parseBoolProp(s.db.GetProperty(kvstat.PropertyCompactionPending))
		st.EstimatePendingCompactionBytes = parseUintProp(s.db.GetProperty(kvstat.PropertyEstimatePendingCompactionBytes))
		st.EstimateNumKeys = parseUintProp(s.db.GetProperty(kvstat.PropertyEstimateNumKeys))
		st.EstimateLiveDataSize = parseUintProp(s.db.GetProperty(kvstat.PropertyEstimateLiveDataSize))
	} else {
		// OpenDBWithCF: collect per-CF stats and aggregate
		cfs := make([]kvstat.CFStats, 0, len(tables))
		var (
			tableReadersMemSum   uint64
			memTablesSizeSum     uint64
			immTableSum          uint64
			l0Max                uint64
			l1Max                uint64
			memTableFlushPending bool
			compactionPending    bool
			compactionBytesSum   uint64
			numKeysSum           uint64
			liveDataSizeSum      uint64
		)
		for name, t := range tables {
			cf := kvstat.CFStats{
				Name:                           name,
				LiveSstFilesSize:               parseUintProp(s.db.GetPropertyCF(kvstat.PropertyLiveSstFilesSize, t.cf)),
				EstimateTableReadersMem:        parseUintProp(s.db.GetPropertyCF(kvstat.PropertyEstimateTableReadersMem, t.cf)),
				CurSizeAllMemTables:            parseUintProp(s.db.GetPropertyCF(kvstat.PropertyCurSizeAllMemTables, t.cf)),
				NumImmutableMemTable:           parseUintProp(s.db.GetPropertyCF(kvstat.PropertyNumImmutableMemTable, t.cf)),
				NumFilesAtLevel0:               parseUintProp(s.db.GetPropertyCF(kvstat.PropertyNumFilesAtLevel0, t.cf)),
				NumFilesAtLevel1:               parseUintProp(s.db.GetPropertyCF(kvstat.PropertyNumFilesAtLevel1, t.cf)),
				MemTableFlushPending:           parseBoolProp(s.db.GetPropertyCF(kvstat.PropertyMemTableFlushPending, t.cf)),
				CompactionPending:              parseBoolProp(s.db.GetPropertyCF(kvstat.PropertyCompactionPending, t.cf)),
				EstimatePendingCompactionBytes: parseUintProp(s.db.GetPropertyCF(kvstat.PropertyEstimatePendingCompactionBytes, t.cf)),
				EstimateNumKeys:                parseUintProp(s.db.GetPropertyCF(kvstat.PropertyEstimateNumKeys, t.cf)),
				EstimateLiveDataSize:           parseUintProp(s.db.GetPropertyCF(kvstat.PropertyEstimateLiveDataSize, t.cf)),
			}
			cfs = append(cfs, cf)

			tableReadersMemSum += cf.EstimateTableReadersMem
			memTablesSizeSum += cf.CurSizeAllMemTables
			immTableSum += cf.NumImmutableMemTable
			if cf.NumFilesAtLevel0 > l0Max {
				l0Max = cf.NumFilesAtLevel0
			}
			if cf.NumFilesAtLevel1 > l1Max {
				l1Max = cf.NumFilesAtLevel1
			}
			memTableFlushPending = memTableFlushPending || cf.MemTableFlushPending
			compactionPending = compactionPending || cf.CompactionPending
			compactionBytesSum += cf.EstimatePendingCompactionBytes
			numKeysSum += cf.EstimateNumKeys
			liveDataSizeSum += cf.EstimateLiveDataSize
		}
		st.ColumnFamilies = cfs
		st.MemoryUsage.EstimateTableReadersMem = tableReadersMemSum
		st.MemoryUsage.CurSizeAllMemTables = memTablesSizeSum
		st.NumImmutableMemTable = immTableSum
		st.NumFilesAtLevel0 = l0Max
		st.NumFilesAtLevel1 = l1Max
		st.MemTableFlushPending = memTableFlushPending
		st.CompactionPending = compactionPending
		st.EstimatePendingCompactionBytes = compactionBytesSum
		st.EstimateNumKeys = numKeysSum
		st.EstimateLiveDataSize = liveDataSizeSum
	}

	// EstimatedMemoryUsage = block cache usage + table readers mem + memtables
	// (pinned is a subset of usage)
	st.MemoryUsage.EstimatedMemoryUsage = st.MemoryUsage.BlockCacheUsage +
		st.MemoryUsage.EstimateTableReadersMem + st.MemoryUsage.CurSizeAllMemTables

	return st, nil
}

// parseUintProp parses a RocksDB property string to uint64.
// Returns 0 on empty string or parse error.
func parseUintProp(val string) uint64 {
	if val == "" {
		return 0
	}
	n, err := strconv.ParseUint(strings.TrimSpace(val), 10, 64)
	if err != nil {
		return 0
	}
	return n
}

// parseBoolProp parses a RocksDB property string ("0" or "1") to bool.
// Returns false on empty string or non-"1" value.
func parseBoolProp(val string) bool {
	return strings.TrimSpace(val) == "1"
}
