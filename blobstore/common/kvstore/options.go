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
	readOnly: false,
	// sync mode, default false
	sync: false,
	// background thread count
	backThreadCount: 4,
	// background compaction thread count
	maxBackgroundCompaction: 4,
	// the maximum number of concurrent background memtable flush jobs, submitted to the HIGH priority thread pool, By default, all background jobs (major compaction and memtable flush) go to the LOW priority pool. If this option is set to a positive number, memtable flush jobs will be submitted to the HIGH priority pool. It is important when the same Env is shared by multiple db instances. Without a separate pool, long running major compaction jobs could potentially block memtable flush jobs of other db instances, leading to unnecessary Put stalls.
	maxBackgroundFlushes: 0,
	// write buffer size
	writeBufferSize: 4 * 1024 * 1024,
	// the maximum number of write buffers that are built up in memory
	maxWriteBufferNumber: 2,
	// the number of files to trigger level-0 compaction.
	level0FileNumCompactionTrigger: 4,
	// the soft limit on number of level-0 files
	level0SlowdownWritesTrigger: 20,
	// the maximum number of level-0 files. We stop writes at this point.
	level0StopWritesTrigger: 36,
	// max open file num, -1 mean no limit
	maxOpenFiles: -1,
	// block_size -- RocksDB packs user data in blocks. When reading a key-value pair from a table file, an entire block is loaded into memory. Block size is 4KB by default. Each table file contains an index that lists offsets of all blocks. Increasing block_size means that the index contains fewer entries (since there are fewer blocks per file) and is thus smaller. Increasing block_size decreases memory usage and space amplification, but increases read amplification.
	blockSize: 4096 * 4,
	// create database if not exist
	createIfMissing: true,
	// return error if database already exist
	errorIfExists: false,
	// LRU cache size
	lRUCache: 1 << 30,
	// bloom filter size limit
	bloomFilterSize: 10,
	// target_file_size_base and target_file_size_multiplier -- Files in level 1 will have target_file_size_base bytes. Each next level's file size will be target_file_size_multiplier bigger than previous one. However, by default target_file_size_multiplier is 1, so files in all L1..Lmax levels are equal. Increasing target_file_size_base will reduce total number of database files, which is generally a good thing. We recommend setting target_file_size_base to be max_bytes_for_level_base / 10, so that there are 10 files in level 1.
	targetFileSizeBase: 32 << 20,
	// max_bytes_for_level_base and max_bytes_for_level_multiplier -- max_bytes_for_level_base is total size of level 1. As mentioned, we recommend that this be around the size of level 0. Each subsequent level is max_bytes_for_level_multiplier larger than previous one. The default is 10 and we do not recommend changing that.
	maxBytesForLevelBase: 256 << 20,
	// SetTargetFileSizeMultiplier sets the target file size multiplier for compaction. Default: 1
	targetFileSizeMultiplier: 1,
	// num_levels -- It is safe for num_levels to be bigger than expected number of levels in the database. Some higher levels may be empty, but this will not impact performance in any way. Only change this option if you expect your number of levels will be greater than 7 (default).
	numLevels: 7,
	// enable statistic
	enableStatistics: false,
	// use mmap read
	allowMMAPRead: false,
	// use mmap write
	allowMMAPWrite: false,
	// update memory table if meet request
	inplaceUpdateSupport: false,
	// log level
	infoLogLevel: defaultRocksLogLevel,
	// statistic dump period time
	statsDumpPeriodSec: 3600,
	// create if column families not exit
	createIfMissingColumnFamilies: true,
	maxLogFileSize:                1024 * 1024 * 1024,
	keepLogFileNum:                20,
	readCocurrency:                8,
	queueLen:                      128,
}

type RocksDBOption struct {
	readOnly                       bool
	sync                           bool
	infoLogLevel                   uint8
	backThreadCount                int
	maxBackgroundCompaction        int
	maxBackgroundFlushes           int
	writeBufferSize                int
	maxWriteBufferNumber           int
	maxOpenFiles                   int
	blockSize                      int
	createIfMissing                bool
	errorIfExists                  bool
	lRUCache                       uint64
	bloomFilterSize                int
	targetFileSizeBase             uint64
	maxBytesForLevelBase           uint64
	targetFileSizeMultiplier       int
	numLevels                      int
	level0FileNumCompactionTrigger int
	level0SlowdownWritesTrigger    int
	level0StopWritesTrigger        int
	enableStatistics               bool
	allowMMAPRead                  bool
	allowMMAPWrite                 bool
	inplaceUpdateSupport           bool
	statsDumpPeriodSec             uint
	createIfMissingColumnFamilies  bool
	maxLogFileSize                 int
	keepLogFileNum                 int
	readCocurrency                 int
	queueLen                       int
}

type DbOptions func(*RocksDBOption)

func (dbOpt *RocksDBOption) applyOpts(opts []DbOptions) {
	for _, opt := range opts {
		opt(dbOpt)
	}
}

func WithReadConcurrency(concurrency int) DbOptions {
	return func(opt *RocksDBOption) {
		if concurrency > 0 {
			opt.readCocurrency = concurrency
		}
	}
}

func WithQueueLen(queueLen int) DbOptions {
	return func(opt *RocksDBOption) {
		if queueLen > 0 {
			opt.queueLen = queueLen
		}
	}
}

func WithWriteBufferSize(size int) DbOptions {
	return func(opt *RocksDBOption) {
		if size > 0 {
			opt.writeBufferSize = size
		}
	}
}

func WithCatchSize(size uint64) DbOptions {
	return func(opt *RocksDBOption) {
		if size > 0 {
			opt.lRUCache = size
		}
	}
}

func WithReadonly(readonly bool) DbOptions {
	return func(opt *RocksDBOption) {
		opt.readOnly = readonly
	}
}

func WithSyncMode(sync bool) DbOptions {
	return func(opt *RocksDBOption) {
		opt.sync = sync
	}
}

func genRocksdbOpts(opt *RocksDBOption) (opts *rdb.Options) {
	opts = rdb.NewDefaultOptions()
	blockBaseOpt := rdb.NewDefaultBlockBasedTableOptions()
	lRUCache := rdb.NewLRUCache(opt.lRUCache)
	blockBaseOpt.SetBlockCache(lRUCache)
	blockBaseOpt.SetBlockSize(opt.blockSize)
	if opt.bloomFilterSize > 0 {
		bf := rdb.NewBloomFilter(opt.bloomFilterSize)
		blockBaseOpt.SetFilterPolicy(bf)
	}

	opts.SetInfoLogLevel(rdb.InfoLogLevel(opt.infoLogLevel))

	opts.SetWriteBufferSize(opt.writeBufferSize)
	opts.SetMaxWriteBufferNumber(opt.maxWriteBufferNumber)
	opts.SetCreateIfMissing(opt.createIfMissing)
	opts.SetErrorIfExists(opt.errorIfExists)
	opts.SetMaxOpenFiles(opt.maxOpenFiles)
	opts.SetTargetFileSizeBase(opt.targetFileSizeBase)
	opts.SetMaxBytesForLevelBase(opt.maxBytesForLevelBase)
	opts.SetTargetFileSizeMultiplier(opt.targetFileSizeMultiplier)
	opts.SetNumLevels(opt.numLevels)
	opts.SetLevel0FileNumCompactionTrigger(opt.level0FileNumCompactionTrigger)
	opts.SetLevel0SlowdownWritesTrigger(opt.level0SlowdownWritesTrigger)
	opts.SetLevel0StopWritesTrigger(opt.level0StopWritesTrigger)
	opts.SetInplaceUpdateSupport(opt.inplaceUpdateSupport)
	opts.SetStatsDumpPeriodSec(opt.statsDumpPeriodSec)
	opts.SetAllowMmapReads(opt.allowMMAPRead)
	opts.SetAllowMmapWrites(opt.allowMMAPWrite)
	opts.SetMaxBackgroundCompactions(opt.maxBackgroundCompaction)
	opts.SetBlockBasedTableFactory(blockBaseOpt)
	opts.SetCreateIfMissingColumnFamilies(opt.createIfMissingColumnFamilies)
	opts.SetMaxLogFileSize(opt.maxLogFileSize)
	opts.SetKeepLogFileNum(opt.keepLogFileNum)
	if opt.enableStatistics {
		opts.EnableStatistics()
	}
	env := rdb.NewDefaultEnv()
	env.SetBackgroundThreads(opt.backThreadCount)
	opts.SetEnv(env)
	return
}
