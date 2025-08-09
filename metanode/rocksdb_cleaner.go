package metanode

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"sync"
	"time"

	"github.com/cubefs/cubefs/util/log"
)

const (
	cleanRecordDir = "clean_records"
)

type RocksdbTree struct {
	inodeTree      InodeTree     // btree for inodes
	dentryTree     DentryTree    // btree for dentries
	extendTree     ExtendTree    // btree for inode extend (XAttr) management
	multipartTree  MultipartTree // collection for multipart management
	txTree         TransactionTree
	txRbInodeTree  TransactionRollbackInodeTree  // key: inode id
	txRbDentryTree TransactionRollbackDentryTree // key: parentId_name
	rocksdbManager RocksdbManager
	db             *RocksdbOperator
	RocksDBDir     string
	PartitionId    uint64
}

// CleanRecord 记录清理任务的信息
type CleanRecord struct {
	PartitionId uint64    `json:"partition_id"`
	RocksDBDir  string    `json:"rocks_db_dir"`
	RootDir     string    `json:"root_dir"`
	CleanTime   time.Time `json:"clean_time"`
	Status      string    `json:"status"`          // "pending", "success" or "failed"
	Error       string    `json:"error,omitempty"` // 如果失败，记录错误信息
}

// RocksDBCleaner 管理RocksDB的异步清理任务
type RocksDBCleaner struct {
	cleanTasks chan *CleanRecord
	wg         sync.WaitGroup
	stopC      chan struct{}
	rootDir    string // 用于存储清理记录的根目录
	rocksdbMgr RocksdbManager
	// 记录清理历史
	historyLock sync.RWMutex
	history     map[uint64]*CleanRecord // key: PartitionId
}

// NewRocksDBCleaner 创建一个新的RocksDB清理器
func NewRocksDBCleaner(rootDir string, rocksdbMgr RocksdbManager) *RocksDBCleaner {
	cleaner := &RocksDBCleaner{
		cleanTasks: make(chan *CleanRecord, 1000),
		stopC:      make(chan struct{}),
		history:    make(map[uint64]*CleanRecord),
		rootDir:    rootDir,
		rocksdbMgr: rocksdbMgr,
	}

	// 创建清理记录目录
	recordDir := path.Join(rootDir, cleanRecordDir)
	if err := os.MkdirAll(recordDir, 0755); err != nil {
		log.LogErrorf("RocksDBCleaner: failed to create clean record directory: %s", err)
	}

	// 加载已有的清理记录
	go cleaner.loadExistingRecords()

	cleaner.start()
	return cleaner
}

// GetCleanHistory 获取指定分区的清理历史
func (c *RocksDBCleaner) GetCleanHistory(partitionId uint64) *CleanRecord {
	c.historyLock.RLock()
	defer c.historyLock.RUnlock()
	return c.history[partitionId]
}

// GetAllCleanHistory 获取所有分区的清理历史
func (c *RocksDBCleaner) GetAllCleanHistory() []*CleanRecord {
	c.historyLock.RLock()
	defer c.historyLock.RUnlock()

	records := make([]*CleanRecord, 0, len(c.history))
	for _, record := range c.history {
		records = append(records, record)
	}
	return records
}

// recordClean 记录清理结果
func (c *RocksDBCleaner) recordClean(mp *metaPartition, err error) {
	c.historyLock.Lock()
	defer c.historyLock.Unlock()

	record := &CleanRecord{
		PartitionId: mp.config.PartitionId,
		RocksDBDir:  mp.config.RocksDBDir,
		CleanTime:   time.Now(),
		Status:      "success",
	}

	if err != nil {
		record.Status = "failed"
		record.Error = err.Error()
	}

	c.history[mp.config.PartitionId] = record
}

// getRecordPath 获取清理记录文件的路径
func (c *RocksDBCleaner) getRecordPath(partitionId uint64) string {
	return path.Join(c.rootDir, cleanRecordDir, fmt.Sprintf("clean_record_%d.json", partitionId))
}

// loadExistingRecords 加载已存在的清理记录
func (c *RocksDBCleaner) loadExistingRecords() {
	recordDir := path.Join(c.rootDir, cleanRecordDir)
	files, err := os.ReadDir(recordDir)
	if err != nil {
		log.LogErrorf("RocksDBCleaner: failed to read clean record directory: %s", err)
		return
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		recordPath := path.Join(recordDir, file.Name())
		data, err := os.ReadFile(recordPath)
		if err != nil {
			log.LogErrorf("RocksDBCleaner: failed to read clean record file %s: %s", recordPath, err)
			continue
		}

		var record CleanRecord
		if err := json.Unmarshal(data, &record); err != nil {
			log.LogErrorf("RocksDBCleaner: failed to unmarshal clean record from %s: %s", recordPath, err)
			continue
		}

		c.historyLock.Lock()
		c.history[record.PartitionId] = &record
		c.historyLock.Unlock()

		c.cleanTasks <- &record
	}
}

// saveRecord 保存清理记录到磁盘
func (c *RocksDBCleaner) saveRecord(record *CleanRecord) error {
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}

	recordPath := c.getRecordPath(record.PartitionId)
	return os.WriteFile(recordPath, data, 0644)
}

// deleteRecord 从磁盘删除清理记录
func (c *RocksDBCleaner) deleteRecord(partitionId uint64) error {
	recordPath := c.getRecordPath(partitionId)
	err := os.Remove(recordPath)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// start 启动清理工作协程
func (c *RocksDBCleaner) start() {
	c.wg.Add(1)
	go c.cleanWorker()
}

// Stop 停止清理工作
func (c *RocksDBCleaner) Stop() {
	close(c.stopC)
	c.wg.Wait()
}

// AddTask 添加一个清理任务
func (c *RocksDBCleaner) AddTask(mp *metaPartition) error {
	if c.IsCleanPending(mp.config.PartitionId) {
		return fmt.Errorf("partition [%d] is cleaning", mp.config.PartitionId)
	}

	startTime := time.Now()
	log.LogInfof("RocksDBCleaner: start cleaning partition [%d], rocksdb dir [%s]",
		mp.config.PartitionId, mp.config.RocksDBDir)

	// 创建并保存清理记录
	record := &CleanRecord{
		PartitionId: mp.config.PartitionId,
		RocksDBDir:  mp.config.RocksDBDir,
		RootDir:     mp.config.RootDir,
		CleanTime:   startTime,
		Status:      "pending",
	}

	if err := c.saveRecord(record); err != nil {
		log.LogErrorf("RocksDBCleaner: failed to save clean record for partition [%d]: %s",
			mp.config.PartitionId, err)
		return err
	}

	c.historyLock.Lock()
	c.history[mp.config.PartitionId] = record
	c.historyLock.Unlock()

	c.cleanTasks <- record

	return nil
}

// cleanWorker 执行清理工作的协程
func (c *RocksDBCleaner) cleanWorker() {
	defer c.wg.Done()

	for {
		select {
		case <-c.stopC:
			return
		case record := <-c.cleanTasks:
			err := c.DoCleanRocksdbData(record)
			if err != nil {
				log.LogErrorf("RocksDBCleaner: failed to clean partition [%d]: %s",
					record.PartitionId, err)
			}
		}
	}
}

// IsCleanPending 检查指定的分区是否正在清理中
func (c *RocksDBCleaner) IsCleanPending(partitionId uint64) bool {
	c.historyLock.RLock()
	defer c.historyLock.RUnlock()

	_, exists := c.history[partitionId]
	return exists
}

// GetCleanStatus 获取分区的清理状态，如果分区不在清理历史中返回空字符串
func (c *RocksDBCleaner) GetCleanStatus(partitionId uint64) string {
	c.historyLock.RLock()
	defer c.historyLock.RUnlock()

	if record, exists := c.history[partitionId]; exists {
		return record.Status
	}
	return ""
}

func (c *RocksDBCleaner) initRocksdbTree(record *CleanRecord) (*RocksdbTree, error) {
	var (
		err  error
		tree *RocksTree
	)
	rocksdbTree := &RocksdbTree{
		rocksdbManager: c.rocksdbMgr,
		RocksDBDir:     record.RocksDBDir,
		PartitionId:    record.PartitionId,
	}

	// 初始化RocksDB
	rocksdbTree.db, err = rocksdbTree.rocksdbManager.OpenRocksdb(rocksdbTree.RocksDBDir, rocksdbTree.PartitionId)
	if err != nil {
		log.LogErrorf("action[initTempMetaPartition] mp(%v) failed to open rocksdb: %v", rocksdbTree.PartitionId, err)
		return nil, err
	}

	if tree, err = DefaultRocksTree(rocksdbTree.db, rocksdbTree.PartitionId); err != nil {
		log.LogErrorf("[initRocksDBTree] default rocks tree dir: %v, id: %v error %v ", rocksdbTree.RocksDBDir, rocksdbTree.PartitionId, err)
		return nil, err
	}
	if rocksdbTree.inodeTree, err = NewInodeRocks(tree); err != nil {
		return nil, err
	}
	if rocksdbTree.dentryTree, err = NewDentryRocks(tree); err != nil {
		return nil, err
	}
	if rocksdbTree.extendTree, err = NewExtendRocks(tree); err != nil {
		return nil, err
	}
	if rocksdbTree.multipartTree, err = NewMultipartRocks(tree); err != nil {
		return nil, err
	}
	if rocksdbTree.txTree, err = NewTransactionRocks(tree); err != nil {
		return nil, err
	}
	if rocksdbTree.txRbInodeTree, err = NewTransactionRollbackInodeRocks(tree); err != nil {
		return nil, err
	}
	if rocksdbTree.txRbDentryTree, err = NewTransactionRollbackDentryRocks(tree); err != nil {
		return nil, err
	}

	return rocksdbTree, nil
}

func (c *RocksDBCleaner) closeRocksdbTree(rocksdbTree *RocksdbTree) error {
	if rocksdbTree.db != nil {
		rocksdbTree.rocksdbManager.CloseRocksdb(rocksdbTree.db)
		rocksdbTree.db = nil
	}

	return nil
}

func (c *RocksDBCleaner) cleanRocksdbTree(rocksdbTree *RocksdbTree) error {
	handle, err := rocksdbTree.inodeTree.CreateBatchWriteHandle()
	if err != nil {
		log.LogErrorf("[Clean] mp(%v) failed to open write handle, err(%v)", rocksdbTree.PartitionId, err)
		return err
	}
	if err = rocksdbTree.inodeTree.Clear(handle); err != nil {
		log.LogErrorf("[Clean] mp(%v) failed to clear inode tree, err(%v)", rocksdbTree.PartitionId, err)
		return err
	}
	if err = rocksdbTree.dentryTree.Clear(handle); err != nil {
		log.LogErrorf("[Clean] mp(%v) failed to clear dentry tree, err(%v)", rocksdbTree.PartitionId, err)
		return err
	}
	if err = rocksdbTree.extendTree.Clear(handle); err != nil {
		log.LogErrorf("[Clean] mp(%v) failed to clear extend tree, err(%v)", rocksdbTree.PartitionId, err)
		return err
	}
	if err = rocksdbTree.multipartTree.Clear(handle); err != nil {
		log.LogErrorf("[Clean] mp(%v) failed to clear multipart tree, err(%v)", rocksdbTree.PartitionId, err)
		return err
	}
	if err = rocksdbTree.txTree.Clear(handle); err != nil {
		log.LogErrorf("[Clean] mp(%v) failed to clear transaction tree, err(%v)", rocksdbTree.PartitionId, err)
		return err
	}
	if err = rocksdbTree.txRbInodeTree.Clear(handle); err != nil {
		log.LogErrorf("[Clean] mp(%v) failed to clear transaction rollback inode tree, err(%v)", rocksdbTree.PartitionId, err)
		return err
	}
	if err = rocksdbTree.txRbDentryTree.Clear(handle); err != nil {
		log.LogErrorf("[Clean] mp(%v) failed to clear transaction rollback dentry tree, err(%v)", rocksdbTree.PartitionId, err)
		return err
	}
	if err = rocksdbTree.inodeTree.DeleteMetadata(handle); err != nil {
		log.LogErrorf("[Clean] mp(%v) failed to delete metadata, err(%v)", rocksdbTree.PartitionId, err)
		return err
	}
	err = rocksdbTree.inodeTree.CommitAndReleaseBatchWriteForClear(handle)
	if err != nil {
		log.LogErrorf("[Clean] mp(%v) failed to commit and release batch write for clear, err(%v)", rocksdbTree.PartitionId, err)
		return err
	}
	err = rocksdbTree.inodeTree.Flush(true)
	if err != nil {
		log.LogErrorf("[Clean] mp(%v) flush failed: %v", rocksdbTree.PartitionId, err)
		return err
	}

	return nil
}

func (c *RocksDBCleaner) DoCleanRocksdbData(record *CleanRecord) error {
	rocksdbTree, err := c.initRocksdbTree(record)
	if err != nil {
		log.LogErrorf("RocksDBCleaner: failed to init rocksdb tree for partition [%d]: %s",
			record.PartitionId, err)
		return err
	}

	defer c.closeRocksdbTree(rocksdbTree)

	// 执行清理操作
	err = c.cleanRocksdbTree(rocksdbTree)
	if err != nil {
		record.Status = "failed"
		record.Error = err.Error()
		if err := c.saveRecord(record); err != nil {
			log.LogErrorf("RocksDBCleaner: failed to update clean record for partition [%d]: %s",
				record.PartitionId, err)
		}
		log.LogErrorf("RocksDBCleaner: failed to clean partition [%d], err: %s",
			record.PartitionId, err)
		return err
	}

	// remove files
	filenames := []string{uniqCheckerFile, verdataFile, applyIDFile}
	for _, filename := range filenames {
		filepath := path.Join(record.RootDir, rocksdbSnapDir, filename)
		if err = os.Remove(filepath); err != nil && !os.IsNotExist(err) {
			return err
		}
		err = nil
	}

	// 清理成功，删除记录
	c.historyLock.Lock()
	delete(c.history, record.PartitionId)
	c.historyLock.Unlock()

	if err := c.deleteRecord(record.PartitionId); err != nil {
		log.LogErrorf("RocksDBCleaner: failed to delete clean record for partition [%d]: %s",
			record.PartitionId, err)
	}

	log.LogInfof("RocksDBCleaner: finished cleaning partition [%d], rocksdb dir [%s]",
		record.PartitionId, record.RocksDBDir)

	return nil
}
