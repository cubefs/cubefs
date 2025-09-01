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

// CleanRecord records information about a cleanup task
type CleanRecord struct {
	PartitionId uint64    `json:"partition_id"`
	RocksDBDir  string    `json:"rocks_db_dir"`
	RootDir     string    `json:"root_dir"`
	CleanTime   time.Time `json:"clean_time"`
	Status      string    `json:"status"`          // "pending", "success" or "failed"
	Error       string    `json:"error,omitempty"` // If failed, record error information
}

// RocksDBCleaner manages asynchronous cleanup tasks for RocksDB
type RocksDBCleaner struct {
	cleanTasks chan *CleanRecord
	wg         sync.WaitGroup
	stopC      chan struct{}
	rootDir    string // Root directory for storing cleanup records
	rocksdbMgr RocksdbManager
	// Record cleanup history
	historyLock sync.RWMutex
	history     map[uint64]*CleanRecord // key: PartitionId
}

// NewRocksDBCleaner creates a new RocksDB cleaner
func NewRocksDBCleaner(rootDir string, rocksdbMgr RocksdbManager) *RocksDBCleaner {
	cleaner := &RocksDBCleaner{
		cleanTasks: make(chan *CleanRecord, 1000),
		stopC:      make(chan struct{}),
		history:    make(map[uint64]*CleanRecord),
		rootDir:    rootDir,
		rocksdbMgr: rocksdbMgr,
	}

	// Create cleanup record directory
	recordDir := path.Join(rootDir, cleanRecordDir)
	if err := os.MkdirAll(recordDir, 0o755); err != nil {
		log.LogErrorf("RocksDBCleaner: failed to create clean record directory: %s", err)
	}

	return cleaner
}

// GetCleanHistory gets the cleanup history for the specified partition
func (c *RocksDBCleaner) GetCleanHistory(partitionId uint64) *CleanRecord {
	c.historyLock.RLock()
	defer c.historyLock.RUnlock()
	return c.history[partitionId]
}

// GetAllCleanHistory gets the cleanup history for all partitions
func (c *RocksDBCleaner) GetAllCleanHistory() []*CleanRecord {
	c.historyLock.RLock()
	defer c.historyLock.RUnlock()

	records := make([]*CleanRecord, 0, len(c.history))
	for _, record := range c.history {
		records = append(records, record)
	}
	return records
}

// getRecordPath gets the file path of the cleanup record
func (c *RocksDBCleaner) getRecordPath(partitionId uint64) string {
	return path.Join(c.rootDir, cleanRecordDir, fmt.Sprintf("clean_record_%d.json", partitionId))
}

// loadExistingRecords loads existing cleanup records
func (c *RocksDBCleaner) loadExistingRecords() {
	recordDir := path.Join(c.rootDir, cleanRecordDir)
	files, err := os.ReadDir(recordDir)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		log.LogErrorf("RocksDBCleaner: failed to read clean record directory: %s", err)
		return
	}

	recordList := make([]*CleanRecord, 0, len(files))
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

		recordList = append(recordList, &record)
	}

	go func() {
		for _, record := range recordList {
			c.cleanTasks <- record
		}
	}()
}

// saveRecord saves the cleanup record to disk
func (c *RocksDBCleaner) saveRecord(record *CleanRecord) error {
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}

	recordPath := c.getRecordPath(record.PartitionId)
	return os.WriteFile(recordPath, data, 0o644)
}

// deleteRecord deletes the cleanup record from disk
func (c *RocksDBCleaner) deleteRecord(partitionId uint64) error {
	recordPath := c.getRecordPath(partitionId)
	err := os.Remove(recordPath)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// start starts the cleanup worker goroutine
func (c *RocksDBCleaner) start() {
	// Load existing cleanup records
	c.loadExistingRecords()

	c.wg.Add(1)
	go c.cleanWorker()
}

// Stop stops the cleanup worker
func (c *RocksDBCleaner) Stop() {
	close(c.stopC)
	c.wg.Wait()
}

// AddTask adds a cleanup task
func (c *RocksDBCleaner) AddTask(mp *metaPartition) error {
	if c.IsCleanPending(mp.config.PartitionId) {
		return fmt.Errorf("partition [%d] is cleaning", mp.config.PartitionId)
	}

	startTime := time.Now()
	log.LogInfof("RocksDBCleaner: start cleaning partition [%d], rocksdb dir [%s]",
		mp.config.PartitionId, mp.config.RocksDBDir)

	// Create and save cleanup record
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

// cleanWorker is the goroutine that performs cleanup work
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

// IsCleanPending checks if the specified partition is being cleaned
func (c *RocksDBCleaner) IsCleanPending(partitionId uint64) bool {
	c.historyLock.RLock()
	defer c.historyLock.RUnlock()

	_, exists := c.history[partitionId]
	return exists
}

// GetCleanStatus gets the cleanup status of the partition, returns empty string if not in history
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

	// Initialize RocksDB
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
		goto errHandler
	}
	if err = rocksdbTree.dentryTree.Clear(handle); err != nil {
		log.LogErrorf("[Clean] mp(%v) failed to clear dentry tree, err(%v)", rocksdbTree.PartitionId, err)
		goto errHandler
	}
	if err = rocksdbTree.extendTree.Clear(handle); err != nil {
		log.LogErrorf("[Clean] mp(%v) failed to clear extend tree, err(%v)", rocksdbTree.PartitionId, err)
		goto errHandler
	}
	if err = rocksdbTree.multipartTree.Clear(handle); err != nil {
		log.LogErrorf("[Clean] mp(%v) failed to clear multipart tree, err(%v)", rocksdbTree.PartitionId, err)
		goto errHandler
	}
	if err = rocksdbTree.txTree.Clear(handle); err != nil {
		log.LogErrorf("[Clean] mp(%v) failed to clear transaction tree, err(%v)", rocksdbTree.PartitionId, err)
		goto errHandler
	}
	if err = rocksdbTree.txRbInodeTree.Clear(handle); err != nil {
		log.LogErrorf("[Clean] mp(%v) failed to clear transaction rollback inode tree, err(%v)", rocksdbTree.PartitionId, err)
		goto errHandler
	}
	if err = rocksdbTree.txRbDentryTree.Clear(handle); err != nil {
		log.LogErrorf("[Clean] mp(%v) failed to clear transaction rollback dentry tree, err(%v)", rocksdbTree.PartitionId, err)
		goto errHandler
	}
	if err = rocksdbTree.inodeTree.DeleteMetadata(handle); err != nil {
		log.LogErrorf("[Clean] mp(%v) failed to delete metadata, err(%v)", rocksdbTree.PartitionId, err)
		goto errHandler
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

errHandler:
	rocksdbTree.inodeTree.ReleaseBatchWriteHandle(handle)

	return err
}

func (c *RocksDBCleaner) DoCleanRocksdbData(record *CleanRecord) error {
	rocksdbTree, err := c.initRocksdbTree(record)
	if err != nil {
		log.LogErrorf("RocksDBCleaner: failed to init rocksdb tree for partition [%d]: %s",
			record.PartitionId, err)
		return err
	}

	defer c.closeRocksdbTree(rocksdbTree)

	// Perform cleanup operation
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

	// Cleanup succeeded, delete record
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
