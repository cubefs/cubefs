// Copyright 2023 The Cuber Authors.
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
	"context"
	"errors"
)

const (
	defaultCF = "default"

	RocksdbLsmKVType = LsmKVType("rocksdb")

	FIFOStyle      = CompactionStyle("fifo")
	LevelStyle     = CompactionStyle("level")
	UniversalStyle = CompactionStyle("universal")

	defaultReadConcurrency  = 10
	defaultReadQueueLen     = 10
	defaultWriteConcurrency = 4
	defaultWriteQueueLen    = 10
)

var (
	ErrNotFound       = errors.New("key not found")
	ErrKVTypeNotFound = errors.New("kv type not found")
)

type (
	CF              string
	LsmKVType       string
	CompactionStyle string

	Store interface {
		NewSnapshot() Snapshot
		CreateColumn(col CF) error
		GetAllColumns() []CF
		CheckColumns(col CF) bool
		Get(ctx context.Context, col CF, key []byte, opts ...ReadOptFunc) (value ValueGetter, err error)
		GetRaw(ctx context.Context, col CF, key []byte, opts ...ReadOptFunc) (value []byte, err error)
		MultiGet(ctx context.Context, col CF, keys [][]byte, opts ...ReadOptFunc) (values []ValueGetter, err error)
		SetRaw(ctx context.Context, col CF, key []byte, value []byte, opts ...WriteOptFunc) error
		Delete(ctx context.Context, col CF, key []byte, opts ...WriteOptFunc) error
		DeleteRange(ctx context.Context, col CF, start, end []byte, opts ...WriteOptFunc) error
		List(ctx context.Context, col CF, prefix []byte, marker []byte, readOpt ReadOption) ListReader
		Write(ctx context.Context, batch WriteBatch, opts ...WriteOptFunc) error
		Read(ctx context.Context, cols []CF, keys [][]byte, opts ...ReadOptFunc) (values []ValueGetter, err error)
		GetOptionHelper() (helper OptionHelper)
		NewReadOption() (readOption ReadOption)
		NewWriteOption() (writeOption WriteOption)
		NewWriteBatch() (writeBatch WriteBatch)
		FlushCF(ctx context.Context, col CF) error
		Stats(ctx context.Context) (Stats, error)
		Close()
	}
	OptionHelper interface {
		GetOption() Option
		SetMaxBackgroundJobs(value int) error
		SetMaxBackgroundCompactions(value int) error
		SetMaxSubCompactions(value int) error
		SetMaxOpenFiles(value int) error
		SetMaxWriteBufferNumber(value int) error
		SetWriteBufferSize(size int) error
		SetArenaBlockSize(size int) error
		SetTargetFileSizeBase(value uint64) error
		SetMaxBytesForLevelBase(value uint64) error
		SetLevel0SlowdownWritesTrigger(value int) error
		SetLevel0StopWritesTrigger(value int) error
		SetSoftPendingCompactionBytesLimit(value uint64) error
		SetHardPendingCompactionBytesLimit(value uint64) error
		SetBlockSize(size int) error
		SetFIFOCompactionMaxTableFileSize(size int) error
		SetFIFOCompactionAllow(value bool) error
	}
	ReadOption interface {
		SetSnapShot(snap Snapshot)
		Close()
	}
	ReadOptFunc func(opts *readOpts)

	WriteOption interface {
		SetSync(value bool)
		DisableWAL(value bool)
		Close()
	}
	WriteOptFunc func(*writeOpts)

	LruCache interface {
		GetUsage() uint64
		GetPinnedUsage() uint64
		Close()
	}
	WriteBufferManager interface {
		Close()
	}
	RateLimiter interface {
		SetBytesPerSec(value int64)
		Close()
	}
	ListReader interface {
		ReadNext() (key KeyGetter, val ValueGetter, err error)
		ReadNextCopy() (key []byte, value []byte, err error)
		ReadPrev() (key KeyGetter, val ValueGetter, err error)
		ReadPrevCopy() (key []byte, value []byte, err error)
		ReadLast() (key KeyGetter, val ValueGetter, err error)
		SeekToLast()
		SeekForPrev(key []byte) (err error)
		Seek(key []byte)
		SetFilterKey(key []byte)
		Close()
	}
	KeyGetter interface {
		Key() []byte
		Close()
	}
	ValueGetter interface {
		Value() []byte
		Read([]byte) (n int, err error)
		Size() int
		Close()
	}
	Snapshot interface {
		Close()
	}
	Env interface {
		SetLowPriorityBackgroundThreads(n int)
		SetHighPriorityBackgroundThreads(n int)
		Close()
	}
	SstFileManager interface {
		Close()
	}
	WriteBatch interface {
		Put(col CF, key, value []byte)
		Delete(col CF, key []byte)
		DeleteRange(col CF, startKey, endKey []byte)
		Data() []byte
		From(data []byte)
		Count() int
		Clear()
		Close()
		// Iterator()
	}

	Stats struct {
		Used              uint64
		MemoryUsage       MemoryUsage
		Level0FileNum     uint64
		WriteSlowdown     bool
		WriteStop         bool
		RunningFlush      uint64
		PendingFlush      bool
		RunningCompaction uint64
		PendingCompaction bool
		BackgroundErrors  uint64
	}
	MemoryUsage struct {
		BlockCacheUsage     uint64
		IndexAndFilterUsage uint64
		MemtableUsage       uint64
		BlockPinnedUsage    uint64
		Total               uint64
	}
	Option struct {
		Sync                             bool                 `json:"sync,omitempty"`
		DisableWal                       bool                 `json:"disable_wal,omitempty"`
		ColumnFamily                     []CF                 `json:"column_family,omitempty"`
		CreateIfMissing                  bool                 `json:"create_if_missingC"`
		BlockSize                        int                  `json:"block_size,omitempty"`
		BlockCache                       uint64               `json:"block_cache,omitempty"`
		EnablePipelinedWrite             bool                 `json:"enable_pipelined_write,omitempty"`
		MaxBackgroundJobs                int                  `json:"max_background_jobs,omitempty"`
		MaxBackgroundCompactions         int                  `json:"max_background_compactions,omitempty"`
		MaxBackgroundFlushes             int                  `json:"max_background_flushes,omitempty"`
		MaxSubCompactions                int                  `json:"max_sub_compactions,omitempty"`
		LevelCompactionDynamicLevelBytes bool                 `json:"level_compaction_dynamic_level_bytes,omitempty"`
		MaxOpenFiles                     int                  `json:"max_open_files,omitempty"`
		MinWriteBufferNumberToMerge      int                  `json:"min_write_buffer_number_to_merge,omitempty"`
		MaxWriteBufferNumber             int                  `json:"max_write_buffer_number,omitempty"`
		WriteBufferSize                  int                  `json:"write_buffer_size,omitempty"`
		ArenaBlockSize                   int                  `json:"arena_block_size,omitempty"`
		TargetFileSizeBase               uint64               `json:"target_file_size_base,omitempty"`
		MaxBytesForLevelBase             uint64               `json:"max_bytes_for_level_base,omitempty"`
		KeepLogFileNum                   int                  `json:"keep_log_file_num,omitempty"`
		MaxLogFileSize                   int                  `json:"max_log_file_size,omitempty"`
		Level0SlowdownWritesTrigger      int                  `json:"level_0_slowdown_writes_trigger,omitempty"`
		Level0StopWritesTrigger          int                  `json:"level_0_stop_writes_trigger,omitempty"`
		SoftPendingCompactionBytesLimit  uint64               `json:"soft_pending_compaction_bytes_limit,omitempty"`
		HardPendingCompactionBytesLimit  uint64               `json:"hard_pending_compaction_bytes_limit,omitempty"`
		MaxWalLogSize                    uint64               `json:"max_wal_log_size,omitempty"`
		CompactionStyle                  CompactionStyle      `json:"compaction_style,omitempty"`
		CompactionOptionFIFO             CompactionOptionFIFO `json:"compaction_option_fifo,omitempty"`

		Cache              LruCache
		WriteBufferManager WriteBufferManager
		Env                Env
		SstFileManager     SstFileManager
		HandleError        HandleError

		ReadConcurrency  int `json:"read_concurrency,omitempty"`
		ReadQueueLen     int `json:"read_queue_len,omitempty"`
		WriteConcurrency int `json:"write_concurrency,omitempty"`
		WriteQueueLen    int `json:"write_queue_len,omitempty"`
	}
	CompactionOptionFIFO struct {
		MaxTableFileSize int  `json:"max_table_file_size,omitempty"`
		AllowCompaction  bool `json:"allow_compaction,omitempty"`
	}
	HandleError func(ctx context.Context, err error)

	readOpts struct {
		opt         ReadOption
		withNoMerge bool
	}
	writeOpts struct {
		opt         WriteOption
		withNoMerge bool
	}
)

func NewKVStore(ctx context.Context, path string, lsmType LsmKVType, option *Option) (Store, error) {
	switch lsmType {
	case RocksdbLsmKVType:
		return newRocksdb(ctx, path, option)
	default:
		return nil, ErrNotFound
	}
}

func NewCache(ctx context.Context, lsmType LsmKVType, size uint64) LruCache {
	switch lsmType {
	case RocksdbLsmKVType:
		return newRocksdbLruCache(ctx, size)
	default:
		return nil
	}
}

func NewWriteBufferManager(ctx context.Context, lsmType LsmKVType, size uint64) WriteBufferManager {
	switch lsmType {
	case RocksdbLsmKVType:
		return newRocksdbWriteBufferManager(ctx, size)
	default:
		return nil
	}
}

func NewEnv(ctx context.Context, lsmType LsmKVType) Env {
	switch lsmType {
	case RocksdbLsmKVType:
		return newRocksdbEnv(ctx)
	default:
		return nil
	}
}

func NewSstFileManager(ctx context.Context, lsmType LsmKVType, env Env) SstFileManager {
	switch lsmType {
	case RocksdbLsmKVType:
		return newRocksdbSstFileManager(ctx, env)
	default:
		return nil
	}
}

func WithReadOption(opt ReadOption) ReadOptFunc {
	return func(ro *readOpts) {
		ro.opt = opt
	}
}

func WithNoMergeRead() ReadOptFunc {
	return func(ro *readOpts) {
		ro.withNoMerge = true
	}
}

func WithWriteOption(opt WriteOption) WriteOptFunc {
	return func(wo *writeOpts) {
		wo.opt = opt
	}
}

func WithNoMergeWrite() WriteOptFunc {
	return func(wo *writeOpts) {
		wo.withNoMerge = true
	}
}

func (cf CF) String() string {
	return string(cf)
}

func (ro *readOpts) applyOptions(opts []ReadOptFunc) {
	for _, opt := range opts {
		if opt != nil {
			opt(ro)
		}
	}
}

func (wo *writeOpts) applyOptions(opts []WriteOptFunc) {
	for _, opt := range opts {
		if opt != nil {
			opt(wo)
		}
	}
}
