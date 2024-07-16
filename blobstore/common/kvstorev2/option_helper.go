package kvstore

import (
	"fmt"
	"strconv"

	rdb "github.com/tecbot/gorocksdb"
)

func (oph *optHelper) GetOption() Option {
	oph.lock.RLock()
	opt := *oph.opt
	oph.lock.RUnlock()
	return opt
}

func (oph *optHelper) SetMaxBackgroundJobs(value int) error {
	oph.lock.Lock()
	defer oph.lock.Unlock()
	if err := oph.db.SetDBOptions([]string{"max_background_jobs"}, []string{strconv.Itoa(value)}); err != nil {
		return err
	}
	oph.opt.MaxBackgroundJobs = value
	return nil
}

func (oph *optHelper) SetMaxBackgroundCompactions(value int) error {
	oph.lock.Lock()
	defer oph.lock.Unlock()
	if err := oph.db.SetDBOptions([]string{"max_background_compactions"}, []string{strconv.Itoa(value)}); err != nil {
		return err
	}
	oph.opt.MaxBackgroundCompactions = value
	return nil
}

func (oph *optHelper) SetMaxSubCompactions(value int) error {
	oph.lock.Lock()
	// todo
	oph.opt.MaxSubCompactions = value
	oph.lock.Unlock()
	return nil
}

func (oph *optHelper) SetMaxOpenFiles(value int) error {
	oph.lock.Lock()
	defer oph.lock.Unlock()
	if err := oph.db.SetDBOptions([]string{"max_open_files"}, []string{strconv.Itoa(value)}); err != nil {
		return err
	}
	oph.opt.MaxOpenFiles = value
	return nil
}

func (oph *optHelper) SetMaxWriteBufferNumber(value int) error {
	oph.lock.Lock()
	defer oph.lock.Unlock()
	if err := oph.db.SetOptions([]string{"max_write_buffer_number"}, []string{strconv.Itoa(value)}); err != nil {
		return err
	}
	oph.opt.MaxWriteBufferNumber = value
	return nil
}

func (oph *optHelper) SetWriteBufferSize(size int) error {
	oph.lock.Lock()
	defer oph.lock.Unlock()
	if err := oph.db.SetOptions([]string{"write_buffer_size"}, []string{strconv.Itoa(size)}); err != nil {
		return err
	}
	oph.opt.WriteBufferSize = size
	return nil
}

func (oph *optHelper) SetArenaBlockSize(size int) error {
	oph.lock.Lock()
	defer oph.lock.Unlock()
	if err := oph.db.SetOptions([]string{"arena_block_size"}, []string{strconv.Itoa(size)}); err != nil {
		return err
	}
	oph.opt.ArenaBlockSize = size
	return nil
}

func (oph *optHelper) SetTargetFileSizeBase(value uint64) error {
	oph.lock.Lock()
	defer oph.lock.Unlock()
	if err := oph.db.SetOptions([]string{"target_file_size_base"}, []string{strconv.FormatUint(value, 10)}); err != nil {
		return err
	}
	oph.opt.TargetFileSizeBase = value
	return nil
}

func (oph *optHelper) SetMaxBytesForLevelBase(value uint64) error {
	oph.lock.Lock()
	defer oph.lock.Unlock()
	if err := oph.db.SetOptions([]string{"max_bytes_for_level_base"}, []string{strconv.FormatUint(value, 10)}); err != nil {
		return err
	}
	oph.opt.MaxBytesForLevelBase = value
	return nil
}

func (oph *optHelper) SetLevel0SlowdownWritesTrigger(value int) error {
	oph.lock.Lock()
	defer oph.lock.Unlock()
	if err := oph.db.SetOptions([]string{"level0_slowdown_writes_trigger"}, []string{strconv.Itoa(value)}); err != nil {
		return err
	}
	oph.opt.Level0SlowdownWritesTrigger = value
	return nil
}

func (oph *optHelper) SetLevel0StopWritesTrigger(value int) error {
	oph.lock.Lock()
	defer oph.lock.Unlock()
	if err := oph.db.SetOptions([]string{"level0_stop_writes_trigger"}, []string{strconv.Itoa(value)}); err != nil {
		return err
	}
	oph.opt.Level0StopWritesTrigger = value
	return nil
}

func (oph *optHelper) SetSoftPendingCompactionBytesLimit(value uint64) error {
	oph.lock.Lock()
	defer oph.lock.Unlock()
	if err := oph.db.SetOptions([]string{"soft_pending_compaction_bytes_limit"}, []string{strconv.FormatUint(value, 10)}); err != nil {
		return err
	}
	oph.opt.SoftPendingCompactionBytesLimit = value
	return nil
}

func (oph *optHelper) SetHardPendingCompactionBytesLimit(value uint64) error {
	oph.lock.Lock()
	defer oph.lock.Unlock()
	if err := oph.db.SetOptions([]string{"hard_pending_compaction_bytes_limit"}, []string{strconv.FormatUint(value, 10)}); err != nil {
		return err
	}
	oph.opt.HardPendingCompactionBytesLimit = value
	return nil
}

func (oph *optHelper) SetBlockSize(size int) error {
	oph.lock.Lock()
	// todo
	oph.opt.BlockSize = size
	oph.lock.Unlock()
	return nil
}

func (oph *optHelper) SetFIFOCompactionMaxTableFileSize(size int) error {
	oph.lock.Lock()
	defer oph.lock.Unlock()
	if err := oph.db.SetOptions(formatFIFOCompactionOption("max_table_files_size", strconv.Itoa(size))); err != nil {
		return err
	}
	oph.opt.CompactionOptionFIFO.MaxTableFileSize = size
	return nil
}

func (oph *optHelper) SetFIFOCompactionAllow(value bool) error {
	oph.lock.Lock()
	defer oph.lock.Unlock()
	v := "false"
	if value {
		v = "true"
	}
	if err := oph.db.SetOptions(formatFIFOCompactionOption("allow_compaction", v)); err != nil {
		return err
	}
	oph.opt.CompactionOptionFIFO.AllowCompaction = value
	return nil
}

func genRocksdbOpts(opt *Option) (opts *rdb.Options) {
	opts = rdb.NewDefaultOptions()
	blockBaseOpt := rdb.NewDefaultBlockBasedTableOptions()
	fifoCompactionOpt := rdb.NewDefaultFIFOCompactionOptions()
	opts.SetCreateIfMissing(opt.CreateIfMissing)
	if opt.BlockSize > 0 {
		blockBaseOpt.SetBlockSize(opt.BlockSize)
	}
	if opt.Cache != nil {
		blockBaseOpt.SetBlockCache(opt.Cache.(*lruCache).cache)
		// blockBaseOpt.SetCacheIndexAndFilterBlocks(true)
	} else {
		if opt.BlockCache > 0 {
			blockBaseOpt.SetBlockCache(rdb.NewLRUCache(opt.BlockCache))
		}
	}
	opts.SetEnablePipelinedWrite(opt.EnablePipelinedWrite)
	if opt.MaxBackgroundCompactions > 0 {
		opts.SetMaxBackgroundCompactions(opt.MaxBackgroundCompactions)
	}
	if opt.MaxBackgroundFlushes > 0 {
		opts.SetMaxBackgroundFlushes(opt.MaxBackgroundFlushes)
	}
	if opt.MaxSubCompactions > 0 {
		opts.SetMaxSubCompactions(opt.MaxSubCompactions)
	}

	opts.SetLevelCompactionDynamicLevelBytes(opt.LevelCompactionDynamicLevelBytes)
	if opt.MaxOpenFiles > 0 {
		opts.SetMaxOpenFiles(opt.MaxOpenFiles)
	}
	if opt.MinWriteBufferNumberToMerge > 0 {
		opts.SetMinWriteBufferNumberToMerge(opt.MinWriteBufferNumberToMerge)
	}
	if opt.MaxWriteBufferNumber > 0 {
		opts.SetMaxWriteBufferNumber(opt.MaxWriteBufferNumber)
	}
	if opt.WriteBufferSize > 0 {
		opts.SetWriteBufferSize(opt.WriteBufferSize)
	}
	if opt.ArenaBlockSize > 0 {
		opts.SetArenaBlockSize(opt.ArenaBlockSize)
	}
	if opt.TargetFileSizeBase > 0 {
		opts.SetTargetFileSizeBase(opt.TargetFileSizeBase)
	}
	if opt.MaxBytesForLevelBase > 0 {
		opts.SetMaxBytesForLevelBase(opt.MaxBytesForLevelBase)
	}
	if opt.KeepLogFileNum > 0 {
		opts.SetKeepLogFileNum(opt.KeepLogFileNum)
	}
	if opt.MaxLogFileSize > 0 {
		opts.SetMaxLogFileSize(opt.MaxLogFileSize)
	}
	if opt.Level0SlowdownWritesTrigger > 0 {
		opts.SetLevel0SlowdownWritesTrigger(opt.Level0SlowdownWritesTrigger)
	}
	if opt.Level0StopWritesTrigger > 0 {
		opts.SetLevel0StopWritesTrigger(opt.Level0StopWritesTrigger)
	}
	if opt.SoftPendingCompactionBytesLimit > 0 {
		opts.SetSoftPendingCompactionBytesLimit(opt.SoftPendingCompactionBytesLimit)
	}
	if opt.HardPendingCompactionBytesLimit > 0 {
		opts.SetHardPendingCompactionBytesLimit(opt.HardPendingCompactionBytesLimit)
	}
	if len(opt.CompactionStyle) > 0 {
		switch opt.CompactionStyle {
		case FIFOStyle:
			opts.SetCompactionStyle(rdb.FIFOCompactionStyle)
		case LevelStyle:
			opts.SetCompactionStyle(rdb.LevelCompactionStyle)
		case UniversalStyle:
			opts.SetCompactionStyle(rdb.UniversalCompactionStyle)
		default:
		}
	}
	if opt.CompactionOptionFIFO.MaxTableFileSize > 0 {
		fifoCompactionOpt.SetMaxTableFilesSize(uint64(opt.CompactionOptionFIFO.MaxTableFileSize))
	}
	if opt.WriteBufferManager != nil {
		opts.SetWriteBufferManager(opt.WriteBufferManager.(*writeBufferManager).manager)
	}
	if opt.MaxWalLogSize > 0 {
		opts.SetMaxTotalWalSize(opt.MaxWalLogSize)
	}
	if opt.Env != nil {
		opts.SetEnv(opt.Env.(*env).Env)
	} else {
		opts.SetEnv(rdb.NewDefaultEnv())
	}
	if opt.SstFileManager != nil {
		opts.SetSstFileManager(opt.SstFileManager.(*sstFileManager).SstFileManager)
	}

	opts.SetStatsDumpPeriodSec(0)
	opts.SetStatsPersistPeriodSec(0)
	opts.SetBlockBasedTableFactory(blockBaseOpt)
	opts.SetFIFOCompactionOptions(fifoCompactionOpt)
	opts.SetCreateIfMissingColumnFamilies(true)

	return
}

func formatFIFOCompactionOption(key, value string) ([]string, []string) {
	s := fmt.Sprintf("%s=%s;", key, value)
	return []string{"compaction_options_fifo"}, []string{s}
}
