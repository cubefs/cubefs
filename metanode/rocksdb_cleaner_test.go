package metanode

import (
	"os"
	"path"
	"testing"
	"time"
)

func TestNewRocksDBCleaner_CreatesRecordDir(t *testing.T) {
	rootDir := t.TempDir()
	cleaner := NewRocksDBCleaner(rootDir, nil)
	if cleaner == nil {
		t.Fatalf("NewRocksDBCleaner returned nil")
	}
	recordDir := path.Join(rootDir, cleanRecordDir)
	if st, err := os.Stat(recordDir); err != nil || !st.IsDir() {
		t.Fatalf("record dir not created, err:%v", err)
	}
}

func TestSaveAndLoadExistingRecords(t *testing.T) {
	rootDir := t.TempDir()
	cleaner := NewRocksDBCleaner(rootDir, nil)

	rec := &CleanRecord{
		PartitionId: 42,
		RocksDBDir:  path.Join(rootDir, "rocksdb"),
		RootDir:     rootDir,
		CleanTime:   time.Now(),
		Status:      "pending",
	}
	if err := cleaner.saveRecord(rec); err != nil {
		t.Fatalf("saveRecord error:%v", err)
	}

	// new instance loads existing records
	cleaner2 := NewRocksDBCleaner(rootDir, nil)
	cleaner2.loadExistingRecords()

	select {
	case r := <-cleaner2.cleanTasks:
		if r.PartitionId != rec.PartitionId || r.RocksDBDir != rec.RocksDBDir || r.RootDir != rec.RootDir || r.Status != rec.Status {
			t.Fatalf("loaded record mismatch: expect:%+v, actual:%+v", rec, r)
		}
		if got := cleaner2.GetCleanHistory(rec.PartitionId); got == nil {
			t.Fatalf("history not populated")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for loaded record")
	}
}

func TestAddTaskAndPendingStatus(t *testing.T) {
	rootDir := t.TempDir()
	dbDir := path.Join(rootDir, "rocksdb")
	_ = os.MkdirAll(dbDir, 0o755)

	cleaner := NewRocksDBCleaner(rootDir, nil)
	mp := &metaPartition{config: &MetaPartitionConfig{PartitionId: 1, RootDir: rootDir, RocksDBDir: dbDir}}
	if err := cleaner.AddTask(mp); err != nil {
		t.Fatalf("AddTask error:%v", err)
	}
	if !cleaner.IsCleanPending(1) {
		t.Fatalf("IsCleanPending should be true after AddTask")
	}
	if status := cleaner.GetCleanStatus(1); status != "pending" {
		t.Fatalf("GetCleanStatus mismatch, expect:pending, actual:%s", status)
	}
	// record file exists
	recordPath := cleaner.getRecordPath(1)
	if _, err := os.Stat(recordPath); err != nil {
		t.Fatalf("record file not found: %v", err)
	}
}

func TestDoCleanRocksdbData_Success(t *testing.T) {
	rootDir := t.TempDir()
	// prepare snapshot dir and files
	snapshotDir := path.Join(rootDir, rocksdbSnapDir)
	if err := os.MkdirAll(snapshotDir, 0o755); err != nil {
		t.Fatalf("mkdir snapshot dir error:%v", err)
	}
	for _, fn := range []string{uniqCheckerFile, verdataFile, applyIDFile} {
		if err := os.WriteFile(path.Join(snapshotDir, fn), []byte("x"), 0o644); err != nil {
			t.Fatalf("prepare file %s error:%v", fn, err)
		}
	}

	// setup per-partition rocksdb manager and register db path
	rocksPath := path.Join(rootDir, "rocksdb")
	if err := os.MkdirAll(rocksPath, 0o755); err != nil {
		t.Fatalf("mkdir rocksdb path error:%v", err)
	}
	mgr := NewPerPartitionRocksdbManager(&RocksdbManagerConfig{})
	if err := mgr.Register(rocksPath); err != nil {
		t.Fatalf("register rocks path error:%v", err)
	}

	cleaner := NewRocksDBCleaner(rootDir, mgr)
	rec := &CleanRecord{PartitionId: 7, RocksDBDir: rocksPath, RootDir: rootDir, CleanTime: time.Now(), Status: "pending"}
	// seed history to verify deletion after success
	cleaner.historyLock.Lock()
	cleaner.history[rec.PartitionId] = rec
	cleaner.historyLock.Unlock()
	if err := cleaner.saveRecord(rec); err != nil {
		t.Fatalf("saveRecord error:%v", err)
	}

	if err := cleaner.DoCleanRocksdbData(rec); err != nil {
		t.Fatalf("DoCleanRocksdbData error:%v", err)
	}

	// history should be cleared and record file removed
	if cleaner.IsCleanPending(rec.PartitionId) {
		t.Fatalf("history should be cleared after success")
	}
	if _, err := os.Stat(cleaner.getRecordPath(rec.PartitionId)); !os.IsNotExist(err) {
		t.Fatalf("record file should be removed, err:%v", err)
	}
	// snapshot files should be removed
	for _, fn := range []string{uniqCheckerFile, verdataFile, applyIDFile} {
		if _, err := os.Stat(path.Join(snapshotDir, fn)); !os.IsNotExist(err) {
			t.Fatalf("snapshot file %s should be removed, err:%v", fn, err)
		}
	}
}
