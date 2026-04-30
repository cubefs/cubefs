package metanode

import (
	"errors"
	"strings"
	"testing"
)

func TestPerDiskRocksdbManagerRegisterUnregisterAndStateGuards(t *testing.T) {
	// Build the concrete manager directly so the test can inspect the internal handle map.
	// These checks exercise registration bookkeeping without opening a native RocksDB handle.
	manager := NewPerDiskRocksdbManager(&RocksdbManagerConfig{}).(*PerDiskRocksdbManager)
	dbPath := "/tmp/per-disk-register"

	// Unknown paths must be rejected before the manager tries to touch any DB state.
	if err := manager.Unregister(dbPath); !errors.Is(err, ErrUnregisteredRocksdbPath) {
		t.Fatalf("Unregister unknown path error = %v, want %v", err, ErrUnregisteredRocksdbPath)
	}
	if _, err := manager.OpenRocksdb(dbPath, 1); !errors.Is(err, ErrUnregisteredRocksdbPath) {
		t.Fatalf("OpenRocksdb unknown path error = %v, want %v", err, ErrUnregisteredRocksdbPath)
	}

	// Register should create exactly one RocksdbHandle with an unopened operator.
	if err := manager.Register(dbPath); err != nil {
		t.Fatalf("Register error = %v", err)
	}
	handle := manager.dbs[dbPath]
	if handle == nil || handle.db == nil {
		t.Fatal("Register should initialize a RocksdbHandle and RocksdbOperator")
	}
	if handle.rc != 0 || handle.partitions != 0 || handle.Forbidden {
		t.Fatalf("new handle state = %+v, want zero rc/partitions and not forbidden", handle)
	}

	// Duplicate registration should preserve the original handle and report a stable error.
	if err := manager.Register(dbPath); !errors.Is(err, ErrRocksdbPathRegistered) {
		t.Fatalf("duplicate Register error = %v, want %v", err, ErrRocksdbPathRegistered)
	}
	if manager.dbs[dbPath] != handle {
		t.Fatal("duplicate Register should not replace the existing handle")
	}

	// Unregister is blocked while a handle is referenced by opened partitions.
	handle.rc = 1
	if err := manager.Unregister(dbPath); !errors.Is(err, ErrRocksdbOpened) {
		t.Fatalf("Unregister opened path error = %v, want %v", err, ErrRocksdbOpened)
	}

	// Once the reference count is clear, unregister should remove the path.
	handle.rc = 0
	if err := manager.Unregister(dbPath); err != nil {
		t.Fatalf("Unregister error = %v", err)
	}
	if _, ok := manager.dbs[dbPath]; ok {
		t.Fatal("Unregister should remove the registered path")
	}
}

func TestPerDiskRocksdbManagerPartitionAndForbiddenBookkeeping(t *testing.T) {
	// Per-disk manager keeps partition placement count separately from open reference count.
	manager := NewPerDiskRocksdbManager(&RocksdbManagerConfig{}).(*PerDiskRocksdbManager)
	dbPath := "/tmp/per-disk-partitions"

	// Partition and forbidden operations should reject paths that were never registered.
	if err := manager.AttachPartition(dbPath); !errors.Is(err, ErrUnregisteredRocksdbPath) {
		t.Fatalf("AttachPartition unknown path error = %v, want %v", err, ErrUnregisteredRocksdbPath)
	}
	if err := manager.DetachPartition(dbPath); !errors.Is(err, ErrUnregisteredRocksdbPath) {
		t.Fatalf("DetachPartition unknown path error = %v, want %v", err, ErrUnregisteredRocksdbPath)
	}
	if _, err := manager.GetPartitionCount(dbPath); !errors.Is(err, ErrUnregisteredRocksdbPath) {
		t.Fatalf("GetPartitionCount unknown path error = %v, want %v", err, ErrUnregisteredRocksdbPath)
	}
	if err := manager.SetForbidden(dbPath, true); !errors.Is(err, ErrUnregisteredRocksdbPath) {
		t.Fatalf("SetForbidden unknown path error = %v, want %v", err, ErrUnregisteredRocksdbPath)
	}

	if err := manager.Register(dbPath); err != nil {
		t.Fatalf("Register error = %v", err)
	}

	// AttachPartition increments placement count, and DetachPartition never decrements below zero.
	if err := manager.AttachPartition(dbPath); err != nil {
		t.Fatalf("AttachPartition error = %v", err)
	}
	if got := manager.dbs[dbPath].partitions; got != 1 {
		t.Fatalf("partitions = %d, want 1", got)
	}
	if err := manager.DetachPartition(dbPath); err != nil {
		t.Fatalf("DetachPartition error = %v", err)
	}
	if err := manager.DetachPartition(dbPath); err != nil {
		t.Fatalf("second DetachPartition error = %v", err)
	}
	if got := manager.dbs[dbPath].partitions; got != 0 {
		t.Fatalf("partitions = %d, want 0", got)
	}

	// GetPartitionCount reports the open reference count for the per-disk manager.
	manager.dbs[dbPath].rc = 2
	count, err := manager.GetPartitionCount(dbPath)
	if err != nil {
		t.Fatalf("GetPartitionCount error = %v", err)
	}
	if count != 2 {
		t.Fatalf("partition count = %d, want 2", count)
	}

	// Setting the same forbidden value twice should be idempotent.
	if err := manager.SetForbidden(dbPath, true); err != nil {
		t.Fatalf("SetForbidden(true) error = %v", err)
	}
	if err := manager.SetForbidden(dbPath, true); err != nil {
		t.Fatalf("second SetForbidden(true) error = %v", err)
	}
	if !manager.dbs[dbPath].Forbidden {
		t.Fatal("SetForbidden(true) should mark the handle forbidden")
	}
	if err := manager.SetForbidden(dbPath, false); err != nil {
		t.Fatalf("SetForbidden(false) error = %v", err)
	}
	if manager.dbs[dbPath].Forbidden {
		t.Fatal("SetForbidden(false) should clear the forbidden flag")
	}
}

func TestPerDiskRocksdbManagerConfigAccessorsWithoutOpenDB(t *testing.T) {
	// Config accessors should still enforce registration and DB-access preconditions.
	manager := NewPerDiskRocksdbManager(&RocksdbManagerConfig{}).(*PerDiskRocksdbManager)
	dbPath := "/tmp/per-disk-config"

	if err := manager.UpdateConfig(dbPath, map[string]string{"periodic_compaction_seconds": "60"}); !errors.Is(err, ErrUnregisteredRocksdbPath) {
		t.Fatalf("UpdateConfig unknown path error = %v, want %v", err, ErrUnregisteredRocksdbPath)
	}
	if _, err := manager.GetConfig(dbPath); !errors.Is(err, ErrUnregisteredRocksdbPath) {
		t.Fatalf("GetConfig unknown path error = %v, want %v", err, ErrUnregisteredRocksdbPath)
	}

	if err := manager.Register(dbPath); err != nil {
		t.Fatalf("Register error = %v", err)
	}

	// The registered DB is not opened in this unit test, so SetOptions must fail safely.
	if err := manager.UpdateConfig(dbPath, map[string]string{"periodic_compaction_seconds": "60"}); !errors.Is(err, ErrRocksdbAccess) {
		t.Fatalf("UpdateConfig unopened db error = %v, want %v", err, ErrRocksdbAccess)
	}

	// GetConfig returns a copy of the operator config map even while it is empty.
	config, err := manager.GetConfig(dbPath)
	if err != nil {
		t.Fatalf("GetConfig error = %v", err)
	}
	config["mutated"] = "true"
	if _, ok := manager.dbs[dbPath].db.config["mutated"]; ok {
		t.Fatal("GetConfig should not expose the operator's internal config map")
	}
}

func TestPerDiskRocksdbManagerSelectDiskNoResource(t *testing.T) {
	// With no registered disks, selection should fail before any disk statistics are read.
	manager := NewPerDiskRocksdbManager(&RocksdbManagerConfig{}).(*PerDiskRocksdbManager)
	if _, err := manager.SelectRocksdbDisk(0); !errors.Is(err, ErrRocksdbNoResource) {
		t.Fatalf("SelectRocksdbDisk empty manager error = %v, want %v", err, ErrRocksdbNoResource)
	}

	// A registered but forbidden disk is also unavailable for new partition placement.
	dbPath := t.TempDir()
	if err := manager.Register(dbPath); err != nil {
		t.Fatalf("Register error = %v", err)
	}
	if err := manager.SetForbidden(dbPath, true); err != nil {
		t.Fatalf("SetForbidden error = %v", err)
	}
	if _, err := manager.SelectRocksdbDisk(0); !errors.Is(err, ErrRocksdbNoResource) {
		t.Fatalf("SelectRocksdbDisk forbidden-only manager error = %v, want %v", err, ErrRocksdbNoResource)
	}
}

func TestPerPartitionRocksdbManagerRegisterPartitionAndForbiddenBookkeeping(t *testing.T) {
	// Per-partition manager stores only directory metadata and partition counts in memory.
	manager := NewPerPartitionRocksdbManager(&RocksdbManagerConfig{}).(*PerPartitionRocksdbManager)
	dbPath := "/tmp/per-partition-register"

	if err := manager.Register(dbPath); err != nil {
		t.Fatalf("Register error = %v", err)
	}
	if _, ok := manager.dbs[dbPath]; !ok {
		t.Fatal("Register should create directory metadata")
	}
	if got := manager.partitionCnt[dbPath]; got != 0 {
		t.Fatalf("initial partition count = %d, want 0", got)
	}
	if err := manager.Register(dbPath); !errors.Is(err, ErrRocksdbPathRegistered) {
		t.Fatalf("duplicate Register error = %v, want %v", err, ErrRocksdbPathRegistered)
	}

	// Attach and detach update the explicit partition count and clamp at zero.
	if err := manager.AttachPartition(dbPath); err != nil {
		t.Fatalf("AttachPartition error = %v", err)
	}
	if err := manager.AttachPartition(dbPath); err != nil {
		t.Fatalf("second AttachPartition error = %v", err)
	}
	if got, err := manager.GetPartitionCount(dbPath); err != nil || got != 2 {
		t.Fatalf("GetPartitionCount = (%d, %v), want (2, nil)", got, err)
	}
	if err := manager.DetachPartition(dbPath); err != nil {
		t.Fatalf("DetachPartition error = %v", err)
	}
	if err := manager.DetachPartition(dbPath); err != nil {
		t.Fatalf("second DetachPartition error = %v", err)
	}
	if err := manager.DetachPartition(dbPath); err != nil {
		t.Fatalf("third DetachPartition error = %v", err)
	}
	if got, err := manager.GetPartitionCount(dbPath); err != nil || got != 0 {
		t.Fatalf("GetPartitionCount after detach = (%d, %v), want (0, nil)", got, err)
	}

	// Forbidden toggling is stored in the per-directory metadata.
	if err := manager.SetForbidden(dbPath, true); err != nil {
		t.Fatalf("SetForbidden(true) error = %v", err)
	}
	if !manager.dbs[dbPath].Forbidden {
		t.Fatal("SetForbidden(true) should mark the directory forbidden")
	}
	if err := manager.SetForbidden(dbPath, false); err != nil {
		t.Fatalf("SetForbidden(false) error = %v", err)
	}
	if manager.dbs[dbPath].Forbidden {
		t.Fatal("SetForbidden(false) should clear the forbidden flag")
	}

	// Unregister removes both metadata maps for the directory.
	if err := manager.Unregister(dbPath); err != nil {
		t.Fatalf("Unregister error = %v", err)
	}
	if _, ok := manager.dbs[dbPath]; ok {
		t.Fatal("Unregister should remove directory metadata")
	}
	if _, ok := manager.partitionCnt[dbPath]; ok {
		t.Fatal("Unregister should remove partition count metadata")
	}
}

func TestPerPartitionRocksdbManagerErrorAndUnsupportedPaths(t *testing.T) {
	// Unknown paths should fail consistently for all metadata operations.
	manager := NewPerPartitionRocksdbManager(&RocksdbManagerConfig{}).(*PerPartitionRocksdbManager)
	dbPath := "/tmp/per-partition-errors"

	if err := manager.AttachPartition(dbPath); !errors.Is(err, ErrUnregisteredRocksdbPath) {
		t.Fatalf("AttachPartition unknown path error = %v, want %v", err, ErrUnregisteredRocksdbPath)
	}
	if err := manager.DetachPartition(dbPath); !errors.Is(err, ErrUnregisteredRocksdbPath) {
		t.Fatalf("DetachPartition unknown path error = %v, want %v", err, ErrUnregisteredRocksdbPath)
	}
	if _, err := manager.GetPartitionCount(dbPath); !errors.Is(err, ErrUnregisteredRocksdbPath) {
		t.Fatalf("GetPartitionCount unknown path error = %v, want %v", err, ErrUnregisteredRocksdbPath)
	}
	if _, err := manager.OpenRocksdb(dbPath, 1); !errors.Is(err, ErrUnregisteredRocksdbPath) {
		t.Fatalf("OpenRocksdb unknown path error = %v, want %v", err, ErrUnregisteredRocksdbPath)
	}
	if err := manager.Unregister(dbPath); !errors.Is(err, ErrUnregisteredRocksdbPath) {
		t.Fatalf("Unregister unknown path error = %v, want %v", err, ErrUnregisteredRocksdbPath)
	}
	if err := manager.SetForbidden(dbPath, true); !errors.Is(err, ErrUnregisteredRocksdbPath) {
		t.Fatalf("SetForbidden unknown path error = %v, want %v", err, ErrUnregisteredRocksdbPath)
	}

	// Per-partition mode intentionally does not support runtime RocksDB option access.
	if err := manager.UpdateConfig(dbPath, map[string]string{"k": "v"}); err == nil || !strings.Contains(err.Error(), "does not support update config") {
		t.Fatalf("UpdateConfig error = %v, want unsupported error", err)
	}
	if _, err := manager.GetConfig(dbPath); err == nil || !strings.Contains(err.Error(), "does not support get config") {
		t.Fatalf("GetConfig error = %v, want unsupported error", err)
	}
}

func TestPerPartitionRocksdbManagerSelectDiskNoResource(t *testing.T) {
	// Selection should fail when the manager has no usable registered directories.
	manager := NewPerPartitionRocksdbManager(&RocksdbManagerConfig{}).(*PerPartitionRocksdbManager)
	if _, err := manager.SelectRocksdbDisk(0); !errors.Is(err, ErrRocksdbNoResource) {
		t.Fatalf("SelectRocksdbDisk empty manager error = %v, want %v", err, ErrRocksdbNoResource)
	}

	dbPath := t.TempDir()
	if err := manager.Register(dbPath); err != nil {
		t.Fatalf("Register error = %v", err)
	}
	if err := manager.SetForbidden(dbPath, true); err != nil {
		t.Fatalf("SetForbidden error = %v", err)
	}
	if _, err := manager.SelectRocksdbDisk(0); !errors.Is(err, ErrRocksdbNoResource) {
		t.Fatalf("SelectRocksdbDisk forbidden-only manager error = %v, want %v", err, ErrRocksdbNoResource)
	}
}

func TestRocksdbManagerConstructorsCopyConfig(t *testing.T) {
	// Use distinct values so every constructor-assigned field can be checked independently.
	config := &RocksdbManagerConfig{
		WriteBufferSize:          1,
		WriteBufferNum:           2,
		MinWriteBuffToMerge:      3,
		MaxSubCompactions:        4,
		BlockCacheSize:           5,
		EnableStats:              true,
		BytesPerSync:             6,
		Parallelism:              7,
		MaxBackgroundCompactions: 8,
		MaxBackgroundFlushes:     9,
		SoftCompactionLimit:      10,
		HardCompactionLimit:      11,
		PeriodicCompactSec:       12,
	}

	// Per-disk constructor should copy config values and initialize its handle map.
	perDisk := NewPerDiskRocksdbManager(config).(*PerDiskRocksdbManager)
	if perDisk.writeBufferSize != config.WriteBufferSize ||
		perDisk.writeBufferNum != config.WriteBufferNum ||
		perDisk.minWriteBuffToMerge != config.MinWriteBuffToMerge ||
		perDisk.maxSubCompactions != config.MaxSubCompactions ||
		perDisk.blockCacheSize != config.BlockCacheSize ||
		perDisk.enableStats != config.EnableStats ||
		perDisk.bytesPerSync != config.BytesPerSync ||
		perDisk.parallelism != config.Parallelism ||
		perDisk.maxBackgroundCompactions != config.MaxBackgroundCompactions ||
		perDisk.maxBackgroundFlushes != config.MaxBackgroundFlushes ||
		perDisk.softCompactionLimit != config.SoftCompactionLimit ||
		perDisk.hardCompactionLimit != config.HardCompactionLimit ||
		perDisk.periodicCompactSec != config.PeriodicCompactSec ||
		perDisk.dbs == nil {
		t.Fatalf("per-disk manager config mismatch: %+v", perDisk)
	}

	// Per-partition constructor shares the same config fields and also initializes count maps.
	perPartition := NewPerPartitionRocksdbManager(config).(*PerPartitionRocksdbManager)
	if perPartition.writeBufferSize != config.WriteBufferSize ||
		perPartition.writeBufferNum != config.WriteBufferNum ||
		perPartition.minWriteBuffToMerge != config.MinWriteBuffToMerge ||
		perPartition.maxSubCompactions != config.MaxSubCompactions ||
		perPartition.blockCacheSize != config.BlockCacheSize ||
		perPartition.enableStats != config.EnableStats ||
		perPartition.bytesPerSync != config.BytesPerSync ||
		perPartition.parallelism != config.Parallelism ||
		perPartition.maxBackgroundCompactions != config.MaxBackgroundCompactions ||
		perPartition.maxBackgroundFlushes != config.MaxBackgroundFlushes ||
		perPartition.softCompactionLimit != config.SoftCompactionLimit ||
		perPartition.hardCompactionLimit != config.HardCompactionLimit ||
		perPartition.periodicCompactSec != config.PeriodicCompactSec ||
		perPartition.dbs == nil ||
		perPartition.partitionCnt == nil {
		t.Fatalf("per-partition manager config mismatch: %+v", perPartition)
	}
}

func TestParseRocksdbModeCaseAndFallback(t *testing.T) {
	// ParseRocksdbMode lowercases input and falls back to the configured default for unknown modes.
	cases := []struct {
		option string
		want   RocksdbMode
	}{
		{option: "DISK", want: PerDiskRocksdbMode},
		{option: "Partition", want: PerPartitionRocksdbMode},
		{option: "unknown", want: DefaultRocksdbMode},
		{option: "", want: DefaultRocksdbMode},
	}

	for _, tc := range cases {
		if got := ParseRocksdbMode(tc.option); got != tc.want {
			t.Fatalf("ParseRocksdbMode(%q) = %v, want %v", tc.option, got, tc.want)
		}
	}
}

func TestPerPartitionCloseRocksdbNilIsNoop(t *testing.T) {
	// CloseRocksdb(nil) is used by cleanup paths and must not panic.
	manager := NewPerPartitionRocksdbManager(&RocksdbManagerConfig{}).(*PerPartitionRocksdbManager)
	manager.CloseRocksdb(nil)
}

func TestPerDiskCloseRocksdbNilAndUnknownPathAreNoops(t *testing.T) {
	// Nil and unregistered DB handles should be ignored by the close path.
	manager := NewPerDiskRocksdbManager(&RocksdbManagerConfig{}).(*PerDiskRocksdbManager)
	manager.CloseRocksdb(nil)
	manager.CloseRocksdb(&RocksdbOperator{dir: "/tmp/not-registered"})
}
