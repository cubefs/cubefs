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

// Package kvstat defines the pure-Go statistics structures of the kv engine
// (RocksDB). It carries no cgo dependency, so upper layers (api, sdk, tools)
// can import these types without linking the rocksdb library.
//
// Field names and json tags are derived from the RocksDB property names; values
// aggregated over column families keep the property name, and the aggregation
// (sum or max over all column families) is described in the field comment.
//
// The two kinds of tags are independent:
//   - "json" is the wire format of the /debug/meta/stats payload;
//   - "metric" is the prometheus item name, only fields carrying it are
//     reported, and metric:"-" means the field is not reported at all.
package kvstat

// RocksDB property names used for stats collection, as exported by the engine
// and accepted by DB::GetProperty / DB::GetPropertyCF.
const (
	PropertyLiveSstFilesSize               = "rocksdb.live-sst-files-size"
	PropertyBlockCacheCapacity             = "rocksdb.block-cache-capacity"
	PropertyBlockCacheUsage                = "rocksdb.block-cache-usage"
	PropertyBlockCachePinnedUsage          = "rocksdb.block-cache-pinned-usage"
	PropertyEstimateTableReadersMem        = "rocksdb.estimate-table-readers-mem"
	PropertyCurSizeAllMemTables            = "rocksdb.cur-size-all-mem-tables"
	PropertyNumImmutableMemTable           = "rocksdb.num-immutable-mem-table"
	PropertyNumFilesAtLevel0               = "rocksdb.num-files-at-level0"
	PropertyNumFilesAtLevel1               = "rocksdb.num-files-at-level1"
	PropertyMemTableFlushPending           = "rocksdb.mem-table-flush-pending"
	PropertyCompactionPending              = "rocksdb.compaction-pending"
	PropertyEstimatePendingCompactionBytes = "rocksdb.estimate-pending-compaction-bytes"
	PropertyEstimateNumKeys                = "rocksdb.estimate-num-keys"
	PropertyEstimateLiveDataSize           = "rocksdb.estimate-live-data-size"
	PropertyActualDelayedWriteRate         = "rocksdb.actual-delayed-write-rate"
	PropertyIsWriteStopped                 = "rocksdb.is-write-stopped"
	PropertyNumRunningFlushes              = "rocksdb.num-running-flushes"
	PropertyNumRunningCompactions          = "rocksdb.num-running-compactions"
	PropertyBackgroundErrors               = "rocksdb.background-errors"
	PropertyNumSnapshots                   = "rocksdb.num-snapshots"
	PropertyOldestSnapshotTime             = "rocksdb.oldest-snapshot-time"
)

// Stats holds the aggregated RocksDB engine-level statistics.
type Stats struct {
	// rocksdb.live-sst-files-size, summed over all column families.
	LiveSstFilesSize uint64      `json:"live_sst_files_size" metric:"live_sst_files_size"`
	MemoryUsage      MemoryUsage `json:"memory_usage"`

	// rocksdb.num-files-at-level0, max over all column families.
	NumFilesAtLevel0 uint64 `json:"num_files_at_level0" metric:"num_files_at_level0"`
	// rocksdb.num-files-at-level1, max over all column families.
	NumFilesAtLevel1 uint64 `json:"num_files_at_level1" metric:"num_files_at_level1"`

	// rocksdb.num-immutable-mem-table, summed over all column families.
	NumImmutableMemTable uint64 `json:"num_immutable_mem_table" metric:"num_immutable_mem_table"`
	// rocksdb.mem-table-flush-pending, true if any column family is pending.
	MemTableFlushPending bool `json:"mem_table_flush_pending" metric:"mem_table_flush_pending"`
	// rocksdb.compaction-pending, true if any column family is pending.
	CompactionPending bool `json:"compaction_pending" metric:"compaction_pending"`
	// rocksdb.estimate-pending-compaction-bytes, summed over all column
	// families; the per-column-family values are in ColumnFamilies.
	EstimatePendingCompactionBytes uint64 `json:"estimate_pending_compaction_bytes" metric:"estimate_pending_compaction_bytes"`

	// rocksdb.estimate-num-keys, summed over all column families.
	EstimateNumKeys uint64 `json:"estimate_num_keys" metric:"estimate_num_keys"`
	// rocksdb.estimate-live-data-size, summed over all column families.
	EstimateLiveDataSize uint64 `json:"estimate_live_data_size" metric:"estimate_live_data_size"`

	// rocksdb.actual-delayed-write-rate, 0 means no write slowdown.
	ActualDelayedWriteRate uint64 `json:"actual_delayed_write_rate" metric:"actual_delayed_write_rate"`
	// rocksdb.is-write-stopped.
	IsWriteStopped bool `json:"is_write_stopped" metric:"is_write_stopped"`
	// rocksdb.num-running-flushes.
	NumRunningFlushes uint64 `json:"num_running_flushes" metric:"num_running_flushes"`
	// rocksdb.num-running-compactions.
	NumRunningCompactions uint64 `json:"num_running_compactions" metric:"num_running_compactions"`

	// rocksdb.background-errors.
	BackgroundErrors uint64 `json:"background_errors" metric:"background_errors"`
	// rocksdb.num-snapshots.
	NumSnapshots uint64 `json:"num_snapshots" metric:"num_snapshots"`
	// rocksdb.oldest-snapshot-time.
	OldestSnapshotTime uint64 `json:"oldest_snapshot_time" metric:"oldest_snapshot_time"`

	// Per-column-family details, only filled when the db is opened with CFs.
	ColumnFamilies []CFStats `json:"column_families,omitempty" metric:"-"`
}

// MemoryUsage groups the memory related RocksDB properties.
type MemoryUsage struct {
	// rocksdb.block-cache-capacity, shared by all column families.
	BlockCacheCapacity uint64 `json:"block_cache_capacity" metric:"block_cache_capacity"`
	// rocksdb.block-cache-usage, shared by all column families.
	BlockCacheUsage uint64 `json:"block_cache_usage" metric:"block_cache_usage"`
	// rocksdb.block-cache-pinned-usage, shared by all column families.
	BlockCachePinnedUsage uint64 `json:"block_cache_pinned_usage" metric:"block_cache_pinned_usage"`
	// rocksdb.estimate-table-readers-mem (index and filter blocks), summed over
	// all column families.
	EstimateTableReadersMem uint64 `json:"estimate_table_readers_mem" metric:"estimate_table_readers_mem"`
	// rocksdb.cur-size-all-mem-tables, summed over all column families.
	CurSizeAllMemTables uint64 `json:"cur_size_all_mem_tables" metric:"cur_size_all_mem_tables"`
	// Derived: block cache usage + table readers mem + memtables, an estimate of
	// the total memory occupied by the engine. Pinned usage is a subset of the
	// block cache usage and is not counted again.
	EstimatedMemoryUsage uint64 `json:"estimated_memory_usage" metric:"estimated_memory_usage"`
}

// CFStats holds per-column-family RocksDB statistics, the values are the raw
// properties of one column family.
type CFStats struct {
	Name string `json:"name" metric:"-"`

	// rocksdb.live-sst-files-size.
	LiveSstFilesSize uint64 `json:"live_sst_files_size" metric:"live_sst_files_size"`
	// rocksdb.estimate-table-readers-mem.
	EstimateTableReadersMem uint64 `json:"estimate_table_readers_mem" metric:"estimate_table_readers_mem"`
	// rocksdb.cur-size-all-mem-tables.
	CurSizeAllMemTables uint64 `json:"cur_size_all_mem_tables" metric:"cur_size_all_mem_tables"`

	// rocksdb.num-immutable-mem-table.
	NumImmutableMemTable uint64 `json:"num_immutable_mem_table" metric:"num_immutable_mem_table"`
	// rocksdb.num-files-at-level0.
	NumFilesAtLevel0 uint64 `json:"num_files_at_level0" metric:"num_files_at_level0"`
	// rocksdb.num-files-at-level1.
	NumFilesAtLevel1 uint64 `json:"num_files_at_level1" metric:"num_files_at_level1"`

	// rocksdb.mem-table-flush-pending.
	MemTableFlushPending bool `json:"mem_table_flush_pending" metric:"mem_table_flush_pending"`
	// rocksdb.compaction-pending.
	CompactionPending bool `json:"compaction_pending" metric:"compaction_pending"`
	// rocksdb.estimate-pending-compaction-bytes.
	EstimatePendingCompactionBytes uint64 `json:"estimate_pending_compaction_bytes" metric:"estimate_pending_compaction_bytes"`

	// rocksdb.estimate-num-keys.
	EstimateNumKeys uint64 `json:"estimate_num_keys" metric:"estimate_num_keys"`
	// rocksdb.estimate-live-data-size.
	EstimateLiveDataSize uint64 `json:"estimate_live_data_size" metric:"estimate_live_data_size"`
}
