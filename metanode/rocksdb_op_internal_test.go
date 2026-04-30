package metanode

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestGetTableTypeKeyMappings(t *testing.T) {
	// Verify every supported tree type maps to the RocksDB table used for storage.
	cases := []struct {
		name     string
		treeType TreeType
		want     TableType
	}{
		{name: "inode", treeType: InodeType, want: InodeTable},
		{name: "dentry", treeType: DentryType, want: DentryTable},
		{name: "multipart", treeType: MultipartType, want: MultipartTable},
		{name: "extend", treeType: ExtendType, want: ExtendTable},
		{name: "transaction", treeType: TransactionType, want: TransactionTable},
		{name: "transaction rollback inode", treeType: TransactionRollbackInodeType, want: TransactionRollbackInodeTable},
		{name: "transaction rollback dentry", treeType: TransactionRollbackDentryType, want: TransactionRollbackDentryTable},
		{name: "deleted extents", treeType: DeletedExtentsType, want: DeletedExtentsTable},
		{name: "deleted obj extents", treeType: DeletedObjExtentsType, want: DeletedObjExtentsTable},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := getTableTypeKey(tc.treeType); got != tc.want {
				t.Fatalf("getTableTypeKey(%v) = %v, want %v", tc.treeType, got, tc.want)
			}
		})
	}
}

func TestGetTableTypeKeyPanicsForInvalidTreeType(t *testing.T) {
	// Invalid tree types should fail fast instead of silently using a wrong table.
	defer func() {
		if recovered := recover(); recovered != ErrInvalidRocksdbTableType {
			t.Fatalf("panic = %v, want %v", recovered, ErrInvalidRocksdbTableType)
		}
	}()

	_ = getTableTypeKey(MaxType)
}

func TestIsRetryError(t *testing.T) {
	// Retry detection is based on RocksDB's transient "Try again" error text.
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "retry text", err: errors.New("rocksdb busy: Try again"), want: true},
		{name: "wrapped retry text", err: fmt.Errorf("write failed: %w", errors.New("Try again later")), want: true},
		{name: "non retry", err: errors.New("permanent failure"), want: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isRetryError(tc.err); got != tc.want {
				t.Fatalf("isRetryError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestNewRocksdbInitialStateAndStatistics(t *testing.T) {
	// A new operator should be initialized but not yet connected to RocksDB.
	db := NewRocksdb()

	if db == nil {
		t.Fatal("NewRocksdb returned nil")
	}
	if db.state != dbInitSt {
		t.Fatalf("state = %d, want %d", db.state, dbInitSt)
	}
	if db.config == nil {
		t.Fatal("config map should be initialized")
	}
	if db.isFlushing {
		t.Fatal("new operator should not be flushing")
	}
	if got := db.GetStatistics(); got != "" {
		t.Fatalf("GetStatistics on unopened db = %q, want empty", got)
	}
}

func TestNewDefaultRocksDBOptions(t *testing.T) {
	// Default options should carry the caller's directory and the package defaults.
	opts := NewDefaultRocksDBOptions("/tmp/rocksdb-defaults")

	if opts.Dir != "/tmp/rocksdb-defaults" {
		t.Fatalf("Dir = %q, want /tmp/rocksdb-defaults", opts.Dir)
	}
	if opts.WriteBufferSize != DefaultWriteBuffSize ||
		opts.WriteBufferNum != DefaultWriteBuffNum ||
		opts.MinWriteBuffToMerge != DefaultMinWriteBuffToMerge ||
		opts.MaxSubCompactions != DefaultMaxSubCompaction ||
		opts.BlockCacheSize != DefaultCacheSize ||
		opts.MaxLogFileSize != DefaultMaxLogFileSize ||
		opts.LogFileTimeToRoll != DefaultLogFileRollTime ||
		opts.KeepLogFileNum != DefaultKeepLogFileNum ||
		opts.SoftCompactionLimit != DefaultSoftCompactionLimit ||
		opts.HardCompactionLimit != DefaultHardCompactionLimit {
		t.Fatalf("default options mismatch: %+v", opts)
	}
}

func TestSetOptToConfigAndGetOptionsCopy(t *testing.T) {
	// Store a non-default option set so each config key can be asserted directly.
	db := NewRocksdb()
	opts := &RocksDBOptions{
		WriteBufferSize:          1,
		WriteBufferNum:           2,
		MinWriteBuffToMerge:      3,
		MaxSubCompactions:        4,
		BytesPerSync:             5,
		MaxBackgroundCompactions: 6,
		MaxBackgroundFlushes:     7,
		PeriodicCompactSec:       8,
	}

	db.setOptToConfig(opts)
	got := db.GetOptions()

	// The config map stores values in the string format expected by RocksDB SetOptions.
	want := map[string]string{
		"write_buffer_size":                "1",
		"max_write_buffer_number":          "2",
		"min_write_buffer_number_to_merge": "3",
		"max_subcompactions":               "4",
		"bytes_per_sync":                   "5",
		"max_background_compactions":       "6",
		"max_background_flushes":           "7",
		"periodic_compaction_seconds":      "8",
	}
	for key, val := range want {
		if got[key] != val {
			t.Fatalf("config[%s] = %q, want %q", key, got[key], val)
		}
	}

	// GetOptions must return a defensive copy so callers cannot mutate internal state.
	got["write_buffer_size"] = "mutated"
	if db.config["write_buffer_size"] != "1" {
		t.Fatalf("GetOptions should return a copy, internal value = %q", db.config["write_buffer_size"])
	}
}

func TestUnopenedRocksdbOperationsReturnAccessError(t *testing.T) {
	// Operations that require an opened DB should stop at accessDb before touching RocksDB.
	db := NewRocksdb()

	if err := db.accessDb(); err != ErrRocksdbAccess {
		t.Fatalf("accessDb error = %v, want %v", err, ErrRocksdbAccess)
	}
	if snap := db.OpenSnap(); snap != nil {
		t.Fatal("OpenSnap on unopened db should return nil")
	}
	if _, err := db.GetBytes([]byte("key")); err != ErrRocksdbAccess {
		t.Fatalf("GetBytes error = %v, want %v", err, ErrRocksdbAccess)
	}
	if ok, err := db.HasKey([]byte("key")); ok || err != ErrRocksdbAccess {
		t.Fatalf("HasKey = (%v, %v), want (false, %v)", ok, err, ErrRocksdbAccess)
	}
	// Range callbacks must not run when access validation fails.
	if err := db.Range([]byte("a"), []byte("z"), func(k, v []byte) (bool, error) {
		t.Fatal("Range callback should not run when db is unopened")
		return false, nil
	}); err != ErrRocksdbAccess {
		t.Fatalf("Range error = %v, want %v", err, ErrRocksdbAccess)
	}
	if err := db.DescRange([]byte("a"), []byte("z"), func(k, v []byte) (bool, error) {
		t.Fatal("DescRange callback should not run when db is unopened")
		return false, nil
	}); err != ErrRocksdbAccess {
		t.Fatalf("DescRange error = %v, want %v", err, ErrRocksdbAccess)
	}
	if err := db.Put([]byte("key"), []byte("value")); err != ErrRocksdbAccess {
		t.Fatalf("Put error = %v, want %v", err, ErrRocksdbAccess)
	}
	if err := db.Del([]byte("key")); err != ErrRocksdbAccess {
		t.Fatalf("Del error = %v, want %v", err, ErrRocksdbAccess)
	}
	if _, err := db.CreateBatchHandler(); err != ErrRocksdbAccess {
		t.Fatalf("CreateBatchHandler error = %v, want %v", err, ErrRocksdbAccess)
	}
	if err := db.CompactRange([]byte("a"), []byte("z")); err != ErrRocksdbAccess {
		t.Fatalf("CompactRange error = %v, want %v", err, ErrRocksdbAccess)
	}
	if err := db.Flush(false); err != ErrRocksdbAccess {
		t.Fatalf("Flush error = %v, want %v", err, ErrRocksdbAccess)
	}
	if _, err := db.GetBytesFromDisk([]byte("key")); err != ErrRocksdbAccess {
		t.Fatalf("GetBytesFromDisk error = %v, want %v", err, ErrRocksdbAccess)
	}
	if _, err := db.GetApproximateSizes([]byte("a"), []byte("z")); err != ErrRocksdbAccess {
		t.Fatalf("GetApproximateSizes error = %v, want %v", err, ErrRocksdbAccess)
	}
	if _, err := db.GetLevelNum(); err != ErrRocksdbAccess {
		t.Fatalf("GetLevelNum error = %v, want %v", err, ErrRocksdbAccess)
	}
	if _, err := db.GetLevelNumMap(); err != ErrRocksdbAccess {
		t.Fatalf("GetLevelNumMap error = %v, want %v", err, ErrRocksdbAccess)
	}
	if _, err := db.GetProperty("rocksdb.stats"); err != ErrRocksdbAccess {
		t.Fatalf("GetProperty error = %v, want %v", err, ErrRocksdbAccess)
	}
}

func TestInvalidSnapshotAndBatchHandles(t *testing.T) {
	// Snapshot-based APIs validate nil snapshots before checking DB state.
	db := NewRocksdb()
	cb := func(k, v []byte) (bool, error) { return true, nil }

	if err := db.RangeWithSnap([]byte("a"), []byte("z"), nil, cb); err != ErrInvalidRocksdbSnapshot {
		t.Fatalf("RangeWithSnap error = %v, want %v", err, ErrInvalidRocksdbSnapshot)
	}
	if _, err := db.GetBytesWithSnap(nil, []byte("key")); err != ErrInvalidRocksdbSnapshot {
		t.Fatalf("GetBytesWithSnap error = %v, want %v", err, ErrInvalidRocksdbSnapshot)
	}
	if err := db.RangeWithSnapByPrefix([]byte("p"), []byte("a"), []byte("z"), nil, cb); err != ErrInvalidRocksdbSnapshot {
		t.Fatalf("RangeWithSnapByPrefix error = %v, want %v", err, ErrInvalidRocksdbSnapshot)
	}
	if err := db.DescRangeWithSnap([]byte("a"), []byte("z"), nil, cb); err != ErrInvalidRocksdbSnapshot {
		t.Fatalf("DescRangeWithSnap error = %v, want %v", err, ErrInvalidRocksdbSnapshot)
	}

	// Batch helper APIs should reject handles that are not RocksDB write batches.
	badHandle := "not a write batch"
	if err := db.AddItemToBatch(badHandle, []byte("key"), []byte("value")); err != ErrInvalidRocksdbWriteHandle {
		t.Fatalf("AddItemToBatch error = %v, want %v", err, ErrInvalidRocksdbWriteHandle)
	}
	if err := db.DelItemToBatch(badHandle, []byte("key")); err != ErrInvalidRocksdbWriteHandle {
		t.Fatalf("DelItemToBatch error = %v, want %v", err, ErrInvalidRocksdbWriteHandle)
	}
	if err := db.DelRangeToBatch(badHandle, []byte("a"), []byte("z")); err != ErrInvalidRocksdbWriteHandle {
		t.Fatalf("DelRangeToBatch error = %v, want %v", err, ErrInvalidRocksdbWriteHandle)
	}
	if err := db.CommitBatchAndRelease(badHandle); err != ErrInvalidRocksdbWriteHandle {
		t.Fatalf("CommitBatchAndRelease error = %v, want %v", err, ErrInvalidRocksdbWriteHandle)
	}
	if _, err := db.HandleBatchCount(badHandle); err != ErrInvalidRocksdbWriteHandle {
		t.Fatalf("HandleBatchCount error = %v, want %v", err, ErrInvalidRocksdbWriteHandle)
	}
	if err := db.CommitBatch(badHandle); err != ErrInvalidRocksdbWriteHandle {
		t.Fatalf("CommitBatch error = %v, want %v", err, ErrInvalidRocksdbWriteHandle)
	}
	if err := db.ReleaseBatchHandle(badHandle); err != ErrInvalidRocksdbWriteHandle {
		t.Fatalf("ReleaseBatchHandle error = %v, want %v", err, ErrInvalidRocksdbWriteHandle)
	}
	if err := db.ClearBatchWriteHandle(badHandle); err != ErrInvalidRocksdbWriteHandle {
		t.Fatalf("ClearBatchWriteHandle error = %v, want %v", err, ErrInvalidRocksdbWriteHandle)
	}
	// Releasing a nil handle is a no-op used by cleanup paths.
	if err := db.ReleaseBatchHandle(nil); err != nil {
		t.Fatalf("ReleaseBatchHandle(nil) error = %v, want nil", err)
	}
}

func TestRocksdbStateShortCircuitPaths(t *testing.T) {
	// State checks should return before doOpen/CloseDb touches native RocksDB handles.
	db := NewRocksdb()

	if err := db.CloseDb(); err == nil {
		t.Fatal("CloseDb from init state should fail")
	}

	db.state = dbClosedSt
	if err := db.CloseDb(); err != nil {
		t.Fatalf("CloseDb from closed state error = %v, want nil", err)
	}

	db.state = dbOpenedSt
	if err := db.OpenDb(NewDefaultRocksDBOptions("/unused")); err != nil {
		t.Fatalf("OpenDb from opened state error = %v, want nil", err)
	}
	if err := db.ReOpenDb(NewDefaultRocksDBOptions("/unused")); err != nil {
		t.Fatalf("ReOpenDb from opened state error = %v, want nil", err)
	}

	db.state = dbInitSt
	if err := db.ReOpenDb(NewDefaultRocksDBOptions("/unused")); err == nil {
		t.Fatal("ReOpenDb from init state should fail")
	}
}

func TestSetOptionsWithoutOpenDbReturnsAccessError(t *testing.T) {
	// Runtime option changes require a live DB handle.
	db := NewRocksdb()
	if err := db.SetOptions(map[string]string{"periodic_compaction_seconds": "60"}); err != ErrRocksdbAccess {
		t.Fatalf("SetOptions error = %v, want %v", err, ErrRocksdbAccess)
	}
}

func TestDefaultLogRollTimeConstant(t *testing.T) {
	// Keep the documented three-day log roll interval from changing accidentally.
	if DefaultLogFileRollTime != 72*time.Hour {
		t.Fatalf("DefaultLogFileRollTime = %v, want 72h", DefaultLogFileRollTime)
	}
}
