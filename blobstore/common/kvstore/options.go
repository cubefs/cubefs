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

package kvstore

import (
	rdb "github.com/tecbot/gorocksdb"
)

var defaultRocksLogLevel uint8 = 1

var defaultRocksDBOption = RocksDBOption{
	// readonly mode
	ReadOnly: false,
	// background thread count
	BackThreadCount: 8,
	// background compaction thread count
	MaxBackgroundCompaction: 8,
	// the maximum number of concurrent background memtable flush jobs, submitted to the HIGH priority thread pool, By default, all background jobs (major compaction and memtable flush) go to the LOW priority pool. If this option is set to a positive number, memtable flush jobs will be submitted to the HIGH priority pool. It is important when the same Env is shared by multiple db instances. Without a separate pool, long running major compaction jobs could potentially block memtable flush jobs of other db instances, leading to unnecessary Put stalls.
	MaxBackgroundFlushes: 0,
	// write buffer size
	WriteBufferSize: 4 * 1024 * 1024,
	// the maximum number of write buffers that are built up in memory
	MaxWriteBufferNumber: 2,
	// the number of files to trigger level-0 compaction.
	Level0FileNumCompactionTrigger: 4,
	// the soft limit on number of level-0 files
	Level0SlowdownWritesTrigger: 20,
	// the maximum number of level-0 files. We stop writes at this point.
	Level0StopWritesTrigger: 36,
	// max open file num, -1 mean no limit
	MaxOpenFiles: -1,
	// block_size -- RocksDB packs user data in blocks. When reading a key-value pair from a table file, an entire block is loaded into memory. Block size is 4KB by default. Each table file contains an index that lists offsets of all blocks. Increasing block_size means that the index contains fewer entries (since there are fewer blocks per file) and is thus smaller. Increasing block_size decreases memory usage and space amplification, but increases read amplification.
	BlockSize: 4096 * 4,
	// create database if not exist
	CreateIfMissing: true,
	// return error if database already exist
	ErrorIfExists: false,
	// LRU cache size
	LRUCache: 3 << 30,
	// bloom filter size limit
	BloomFilterSize: 10,
	// target_file_size_base and target_file_size_multiplier -- Files in level 1 will have target_file_size_base bytes. Each next level's file size will be target_file_size_multiplier bigger than previous one. However, by default target_file_size_multiplier is 1, so files in all L1..Lmax levels are equal. Increasing target_file_size_base will reduce total number of database files, which is generally a good thing. We recommend setting target_file_size_base to be max_bytes_for_level_base / 10, so that there are 10 files in level 1.
	TargetFileSizeBase: 32 << 20,
	// max_bytes_for_level_base and max_bytes_for_level_multiplier -- max_bytes_for_level_base is total size of level 1. As mentioned, we recommend that this be around the size of level 0. Each subsequent level is max_bytes_for_level_multiplier larger than previous one. The default is 10 and we do not recommend changing that.
	MaxBytesForLevelBase: 256 << 20,
	// SetTargetFileSizeMultiplier sets the target file size multiplier for compaction. Default: 1
	TargetFileSizeMultiplier: 1,
	// num_levels -- It is safe for num_levels to be bigger than expected number of levels in the database. Some higher levels may be empty, but this will not impact performance in any way. Only change this option if you expect your number of levels will be greater than 7 (default).
	NumLevels: 7,
	// enable statistic
	EnableStatistics: false,
	// use mmap read
	AllowMMAPRead: false,
	// use mmap write
	AllowMMAPWrite: false,
	// update memory table if meet request
	InplaceUpdateSupport: false,
	// log level
	InfoLogLevel: &defaultRocksLogLevel,
	// statistic dump period time
	StatsDumpPeriodSec: 3600,
	// create if column families not exit
	CreateIfMissingColumnFamilies: true,
	MaxLogFileSize:                1024 * 1024 * 1024,
	KeepLogFileNum:                20,
}

type RocksDBOption struct {
	ReadOnly                       bool   `json:"readonly"`
	InfoLogLevel                   *uint8 `json:"log_level"`
	BackThreadCount                int    `json:"back_thread_count"`
	MaxBackgroundCompaction        int    `json:"max_background_compactions"`
	MaxBackgroundFlushes           int    `json:"max_background_flushes"`
	WriteBufferSize                int    `json:"write_buffer_size"`
	MaxWriteBufferNumber           int    `json:"max_write_buffer_number"`
	MaxOpenFiles                   int    `json:"max_open_files"`
	BlockSize                      int    `json:"block_size"`
	CreateIfMissing                bool   `json:"create_if_missing"`
	ErrorIfExists                  bool   `json:"error_if_exists"`
	LRUCache                       int    `json:"lrucache"`
	BloomFilterSize                int    `json:"bloom_filter_size"`
	TargetFileSizeBase             uint64 `json:"target_file_size_base"`
	MaxBytesForLevelBase           uint64 `json:"max_bytes_for_level_base"`
	TargetFileSizeMultiplier       int    `json:"target_file_size_multiplie"`
	NumLevels                      int    `json:"num_levels"`
	Level0FileNumCompactionTrigger int    `json:"level0_file_num_compaction_trigger"`
	Level0SlowdownWritesTrigger    int    `json:"level0_slowdown_writes_trigger"`
	Level0StopWritesTrigger        int    `json:"level0_stop_writes_trigger"`
	EnableStatistics               bool   `json:"enable_statistics"`
	AllowMMAPRead                  bool   `json:"allow_mmap_reads"`
	AllowMMAPWrite                 bool   `json:"allow_mmap_writes"`
	InplaceUpdateSupport           bool   `json:"inplace_update_support"`
	StatsDumpPeriodSec             uint   `json:"stats_dump_period_sec"`
	CreateIfMissingColumnFamilies  bool   `json:"create_if_missing_column_families"`
	MaxLogFileSize                 int    `json:"max_log_file_size"`
	KeepLogFileNum                 int    `json:"keep_log_file_num"`
}

type DbOptions func(*RocksDBOption)

func (dbOpt *RocksDBOption) applyOpts(opts []DbOptions) {
	for _, opt := range opts {
		opt(dbOpt)
	}
}

func fixWithDefaultRocksDBOption(opt RocksDBOption) RocksDBOption {
	def := defaultRocksDBOption
	if opt.InfoLogLevel == nil {
		opt.InfoLogLevel = def.InfoLogLevel
	}
	if opt.BackThreadCount == 0 {
		opt.BackThreadCount = def.BackThreadCount
	}
	if opt.MaxBackgroundCompaction == 0 {
		opt.MaxBackgroundCompaction = def.MaxBackgroundCompaction
	}
	if opt.MaxBackgroundFlushes == 0 {
		opt.MaxBackgroundFlushes = def.MaxBackgroundFlushes
	}
	if opt.WriteBufferSize == 0 {
		opt.WriteBufferSize = def.WriteBufferSize
	}
	if opt.MaxWriteBufferNumber == 0 {
		opt.MaxWriteBufferNumber = def.MaxWriteBufferNumber
	}
	if opt.MaxOpenFiles == 0 {
		opt.MaxOpenFiles = def.MaxOpenFiles
	}
	if opt.BlockSize == 0 {
		opt.BlockSize = def.BlockSize
	}
	if opt.LRUCache == 0 {
		opt.LRUCache = def.LRUCache
	}
	if opt.BloomFilterSize == 0 {
		opt.BloomFilterSize = def.BloomFilterSize
	}
	if opt.TargetFileSizeBase == 0 {
		opt.TargetFileSizeBase = def.TargetFileSizeBase
	}
	if opt.MaxBytesForLevelBase == 0 {
		opt.MaxBytesForLevelBase = def.MaxBytesForLevelBase
	}
	if opt.TargetFileSizeMultiplier == 0 {
		opt.TargetFileSizeMultiplier = def.TargetFileSizeMultiplier
	}
	if !opt.CreateIfMissing {
		opt.CreateIfMissing = def.CreateIfMissing
	}
	if opt.NumLevels == 0 {
		opt.NumLevels = def.NumLevels
	}
	if opt.Level0FileNumCompactionTrigger == 0 {
		opt.Level0FileNumCompactionTrigger = def.Level0FileNumCompactionTrigger
	}
	if opt.Level0SlowdownWritesTrigger == 0 {
		opt.Level0SlowdownWritesTrigger = def.Level0SlowdownWritesTrigger
	}
	if opt.Level0StopWritesTrigger == 0 {
		opt.Level0StopWritesTrigger = def.Level0StopWritesTrigger
	}
	if opt.StatsDumpPeriodSec == 0 {
		opt.StatsDumpPeriodSec = def.StatsDumpPeriodSec
	}
	if !opt.CreateIfMissingColumnFamilies {
		opt.CreateIfMissingColumnFamilies = def.CreateIfMissingColumnFamilies
	}
	if opt.MaxLogFileSize == 0 {
		opt.MaxLogFileSize = def.MaxLogFileSize
	}
	if opt.KeepLogFileNum == 0 {
		opt.KeepLogFileNum = def.KeepLogFileNum
	}
	return opt
}

func genRocksdbOpts(opt *RocksDBOption) (opts *rdb.Options) {
	newOpt := defaultRocksDBOption
	if opt != nil {
		newOpt = fixWithDefaultRocksDBOption(*opt)
	}

	opts = rdb.NewDefaultOptions()
	blockBaseOpt := rdb.NewDefaultBlockBasedTableOptions()
	LRUCache := rdb.NewLRUCache(uint64(newOpt.LRUCache))
	blockBaseOpt.SetBlockCache(LRUCache)
	blockBaseOpt.SetBlockSize(newOpt.BlockSize)
	if newOpt.BloomFilterSize > 0 {
		bf := rdb.NewBloomFilter(newOpt.BloomFilterSize)
		blockBaseOpt.SetFilterPolicy(bf)
	}

	opts.SetInfoLogLevel(rdb.InfoLogLevel(*newOpt.InfoLogLevel))

	opts.SetWriteBufferSize(newOpt.WriteBufferSize)
	opts.SetMaxWriteBufferNumber(newOpt.MaxWriteBufferNumber)
	opts.SetCreateIfMissing(newOpt.CreateIfMissing)
	opts.SetErrorIfExists(newOpt.ErrorIfExists)
	opts.SetMaxOpenFiles(newOpt.MaxOpenFiles)
	opts.SetTargetFileSizeBase(newOpt.TargetFileSizeBase)
	opts.SetMaxBytesForLevelBase(newOpt.MaxBytesForLevelBase)
	opts.SetTargetFileSizeMultiplier(newOpt.TargetFileSizeMultiplier)
	opts.SetNumLevels(newOpt.NumLevels)
	opts.SetLevel0FileNumCompactionTrigger(newOpt.Level0FileNumCompactionTrigger)
	opts.SetLevel0SlowdownWritesTrigger(newOpt.Level0SlowdownWritesTrigger)
	opts.SetLevel0StopWritesTrigger(newOpt.Level0StopWritesTrigger)
	opts.SetInplaceUpdateSupport(newOpt.InplaceUpdateSupport)
	opts.SetStatsDumpPeriodSec(newOpt.StatsDumpPeriodSec)
	opts.SetAllowMmapReads(newOpt.AllowMMAPRead)
	opts.SetAllowMmapWrites(newOpt.AllowMMAPWrite)
	opts.SetMaxBackgroundCompactions(newOpt.MaxBackgroundCompaction)
	opts.SetBlockBasedTableFactory(blockBaseOpt)
	opts.SetCreateIfMissingColumnFamilies(newOpt.CreateIfMissingColumnFamilies)
	opts.SetMaxLogFileSize(newOpt.MaxLogFileSize)
	opts.SetKeepLogFileNum(newOpt.KeepLogFileNum)
	if newOpt.EnableStatistics {
		opts.EnableStatistics()
	}
	env := rdb.NewDefaultEnv()
	env.SetBackgroundThreads(newOpt.BackThreadCount)
	opts.SetEnv(env)
	return
}
