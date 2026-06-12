package metanode

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"
	"unsafe"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/buf"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEmptyV4Inode_Marshal(t *testing.T) {
	ino := NewInode(1024, 0)
	ino.StorageClass = proto.StorageClass_Replica_HDD
	ino.PoolId = 101
	data, err := ino.Marshal()
	require.NoError(t, err)
	targetIno := NewInode(0, 0)
	err = targetIno.Unmarshal(data)
	require.NoError(t, err)
	assert.True(t, ino.Equal(targetIno))
}

func TestHDDV4Inode_Marshal(t *testing.T) {
	ino := NewInode(1024, 1)
	ino.StorageClass = proto.StorageClass_Replica_HDD
	ino.PoolId = 101
	ino.HybridCloudExtents.sortedEks = NewSortedExtentsFromEks([]proto.ExtentKey{{
		FileOffset: 11, PartitionId: 12,
		ExtentId: 13, ExtentOffset: 0, Size: 0, CRC: 0,
	}})

	buf1 := GetInodeBuf()
	defer PutInodeBuf(buf1)

	var data []byte
	err := ino.MarshalV2(buf1)
	if err != nil {
		t.Fail()
	}
	data = buf1.Bytes()

	targetIno := NewInode(0, 0)
	targetIno.Unmarshal(data)
	assert.True(t, ino.Equal(targetIno))

	targetInoV2 := NewInode(0, 0)
	err = targetInoV2.Unmarshal(data)
	if err != nil {
		panic(err)
	}
	assert.True(t, ino.Equal(targetInoV2))
}

func TestEmptyEBSV4Inode_Marshal(t *testing.T) {
	ino := NewInode(1024, 0)
	ino.StorageClass = proto.StorageClass_BlobStore
	ino.PoolId = 101
	var data []byte
	data, _ = ino.Marshal()
	targetIno := NewInode(0, 0)
	targetIno.Unmarshal(data)
	assert.True(t, ino.Equal(targetIno))
}

func TestEBSV4Inode_Marshal(t *testing.T) {
	ino := NewInode(1024, 0)
	ino.StorageClass = proto.StorageClass_BlobStore
	ino.PoolId = 101
	ino.HybridCloudExtents.sortedEks = NewSortedObjExtentsFromObjEks(
		[]proto.ObjExtentKey{{Size: uint64(100), FileOffset: uint64(100)}})

	var data []byte
	data, _ = ino.Marshal()
	targetIno := NewInode(0, 0)
	targetIno.Unmarshal(data)
	assert.True(t, ino.Equal(targetIno))
}

func TestSDDToHDDV4Inode_Marshal(t *testing.T) {
	ino := NewInode(1024, 0)
	ino.StorageClass = proto.StorageClass_Replica_SSD
	ino.PoolId = 101
	ino.HybridCloudExtents.sortedEks = NewSortedExtentsFromEks([]proto.ExtentKey{{
		FileOffset: 11, PartitionId: 12,
		ExtentId: 13, ExtentOffset: 0, Size: 0, CRC: 0,
	}})
	ino.HybridCloudExtentsMigration.storageClass = proto.StorageClass_Replica_HDD
	ino.HybridCloudExtentsMigration.poolId = 102
	ino.HybridCloudExtentsMigration.sortedEks = NewSortedExtentsFromEks([]proto.ExtentKey{{
		FileOffset: 11, PartitionId: 14,
		ExtentId: 16, ExtentOffset: 0, Size: 0, CRC: 0,
	}})
	var data []byte
	data, _ = ino.Marshal()
	targetIno := NewInode(0, 0)
	targetIno.Unmarshal(data)
	assert.True(t, ino.Equal(targetIno))
	ino.Copy()
}

func TestSDDToEBSV4Inode_Marshal(t *testing.T) {
	ino := NewInode(1024, 0)
	ino.StorageClass = proto.StorageClass_Replica_SSD
	ino.PoolId = 101
	ino.HybridCloudExtents.sortedEks = NewSortedExtentsFromEks([]proto.ExtentKey{{
		FileOffset: 11, PartitionId: 12,
		ExtentId: 13, ExtentOffset: 0, Size: 0, CRC: 0,
	}})
	ino.HybridCloudExtentsMigration.storageClass = proto.StorageClass_BlobStore
	ino.HybridCloudExtentsMigration.poolId = 103
	ino.HybridCloudExtentsMigration.sortedEks = NewSortedObjExtentsFromObjEks(
		[]proto.ObjExtentKey{{Size: uint64(100), FileOffset: uint64(100)}})
	var data []byte
	data, _ = ino.Marshal()
	targetIno := NewInode(0, 0)
	targetIno.Unmarshal(data)
	assert.True(t, ino.Equal(targetIno))
}

func TestV4InodeCopy(t *testing.T) {
	ino := NewInode(1024, 0)
	ino.StorageClass = proto.StorageClass_BlobStore
	ino.PoolId = 101
	ino.HybridCloudExtents.sortedEks = NewSortedObjExtentsFromObjEks(
		[]proto.ObjExtentKey{{Size: uint64(100), FileOffset: uint64(100)}})
	temp := ino.Copy().(*Inode)
	assert.True(t, ino.Equal(temp))
}

func TestV4InodeCopyDirectly(t *testing.T) {
	ino := NewInode(1024, 0)
	ino.StorageClass = proto.StorageClass_BlobStore
	ino.PoolId = 101
	ino.HybridCloudExtents.sortedEks = NewSortedObjExtentsFromObjEks(
		[]proto.ObjExtentKey{{Size: uint64(100), FileOffset: uint64(100)}})
	temp := ino.CopyDirectly().(*Inode)
	assert.True(t, ino.Equal(temp))
}

func TestV4MigrationInodeCopy(t *testing.T) {
	ino := NewInode(1024, 0)
	ino.StorageClass = proto.StorageClass_Replica_SSD
	ino.PoolId = 101
	ino.HybridCloudExtentsMigration.storageClass = proto.StorageClass_Replica_HDD
	ino.HybridCloudExtentsMigration.poolId = 102
	ino.HybridCloudExtents.sortedEks = NewSortedExtentsFromEks([]proto.ExtentKey{{
		FileOffset: 11, PartitionId: 12,
		ExtentId: 13, ExtentOffset: 0, Size: 0, CRC: 0,
	}})

	ino.HybridCloudExtentsMigration.sortedEks = NewSortedExtentsFromEks([]proto.ExtentKey{{
		FileOffset: 21, PartitionId: 22,
		ExtentId: 23, ExtentOffset: 0, Size: 0, CRC: 0,
	}})
	temp := ino.Copy().(*Inode)
	assert.True(t, ino.Equal(temp))
}

func TestV4MigrationInodeCopyDirectly(t *testing.T) {
	ino := NewInode(1024, 0)
	ino.StorageClass = proto.StorageClass_Replica_SSD
	ino.PoolId = 101
	ino.HybridCloudExtentsMigration.storageClass = proto.StorageClass_BlobStore
	ino.HybridCloudExtentsMigration.poolId = 102
	ino.HybridCloudExtents.sortedEks = NewSortedExtentsFromEks([]proto.ExtentKey{{
		FileOffset: 11, PartitionId: 12,
		ExtentId: 13, ExtentOffset: 0, Size: 0, CRC: 0,
	}})

	ino.HybridCloudExtentsMigration.sortedEks = NewSortedObjExtentsFromObjEks(
		[]proto.ObjExtentKey{{Size: uint64(100), FileOffset: uint64(100)}})
	temp := ino.CopyDirectly().(*Inode)
	assert.True(t, ino.Equal(temp))
}

func TestInodeAlign(t *testing.T) {
	t.Logf("inode dentry size %d", unsafe.Sizeof(Inode{}))
}

func TestUpdateHybridCloudParamsCopiesGeneration(t *testing.T) {
	target := NewInode(100, 0)
	initTestInodeStorage(target)
	target.Generation = 1
	target.LeaseExpireTime = 10

	param := NewInode(100, 0)
	param.Generation = 99
	param.LeaseExpireTime = 200
	param.StorageClass = proto.StorageClass_Replica_HDD
	param.PoolId = proto.DefaultHDDPoolId

	target.UpdateHybridCloudParams(param)

	assert.Equal(t, uint64(99), target.Generation)
	assert.Equal(t, uint64(200), target.LeaseExpireTime)
	assert.Equal(t, param.StorageClass, target.StorageClass)
	assert.Equal(t, param.PoolId, target.PoolId)
}

func TestInodeMarshal(t *testing.T) {
	checkInodeMarshal := func(a *Inode, t *testing.T) {
		data, err := a.Marshal()
		if err != nil {
			t.Fail()
		}

		b := NewInode(0, 0)
		err = b.Unmarshal(data)
		if err != nil {
			t.Fail()
		}

		buff := GetInodeBuf()
		defer PutInodeBuf(buff)
		err = b.MarshalV2(buff)
		if err != nil {
			t.Fail()
		}

		if !bytes.Equal(data, buff.Bytes()) {
			t.Fail()
		}
	}

	oldIno := NewInode(1024, uint32(os.ModeDir))
	oldIno.Uid = 101
	oldIno.Gid = 102
	oldIno.Generation = 104
	oldIno.CreateTime = 105
	oldIno.AccessTime = 106
	oldIno.ModifyTime = 107
	oldIno.LinkTarget = []byte("test op")
	oldIno.NLink = 108
	oldIno.Flag = 109
	oldIno.StorageClass = proto.StorageClass_Replica_SSD
	oldIno.PoolId = 101
	// dir
	oldIno.Type = uint32(os.ModeDir)
	checkInodeMarshal(oldIno, t)

	// empty file
	oldIno.Type = 0
	checkInodeMarshal(oldIno, t)

	oldIno.LinkTarget = []byte("test link")
	checkInodeMarshal(oldIno, t)
	oldIno.LinkTarget = nil

	// old ebs file
	oldIno.HybridCloudExtents.sortedEks = NewSortedObjExtentsFromObjEks(
		[]proto.ObjExtentKey{
			{Size: uint64(100), FileOffset: uint64(100)},
		},
	)
	oldIno.StorageClass = proto.StorageClass_BlobStore
	checkInodeMarshal(oldIno, t)

	// replica file
	oldIno.StorageClass = proto.StorageClass_Replica_HDD
	oldIno.HybridCloudExtents.sortedEks = NewSortedExtentsFromEks([]proto.ExtentKey{{FileOffset: 100}})
	checkInodeMarshal(oldIno, t)

	legacyReplicaStorageClass = proto.StorageClass_Replica_HDD
	checkInodeMarshal(oldIno, t)

	// check for migration empty
	oldIno.HybridCloudExtentsMigration = &SortedHybridCloudExtentsMigration{
		storageClass: proto.StorageClass_Replica_SSD,
		poolId:       103,
	}
	checkInodeMarshal(oldIno, t)

	oldIno.HybridCloudExtentsMigration = &SortedHybridCloudExtentsMigration{
		sortedEks:    NewSortedExtentsFromEks([]proto.ExtentKey{{FileOffset: 1024}}),
		storageClass: proto.StorageClass_Replica_SSD,
		poolId:       103,
	}
	checkInodeMarshal(oldIno, t)

	eks := NewSortedExtents()
	for i := 0; i < 2048; i++ {
		eks.Append(proto.ExtentKey{
			FileOffset: uint64(i + 1024),
			Size:       uint32(i),
		})
	}
	oldIno.HybridCloudExtents.sortedEks = eks
	checkInodeMarshal(oldIno, t)
}

func TestInodeMarshalValue(t *testing.T) {
	oldIno := NewInode(1024, 0)
	oldIno.Uid = 101
	oldIno.Gid = 102
	oldIno.Generation = 104
	oldIno.CreateTime = 105
	oldIno.AccessTime = 106
	oldIno.ModifyTime = 107
	oldIno.NLink = 108
	oldIno.Flag = 109
	oldIno.StorageClass = proto.StorageClass_Replica_SSD
	oldIno.PoolId = 101
	oldIno.HybridCloudExtents.sortedEks = NewSortedExtentsFromEks([]proto.ExtentKey{{FileOffset: 100}})

	buf1 := GetInodeBuf()
	defer PutInodeBuf(buf1)

	// marshalValue & marshalValueV2
	oldIno.MarshalValueV2(buf1)
	data1 := buf1.Bytes()

	data2 := oldIno.MarshalValue()
	if !bytes.Equal(data1, data2) {
		t.Fail()
	}
}

func BenchmarkInodeMarshal(b *testing.B) {
	oldIno := NewInode(1024, 0)
	oldIno.Uid = 101
	oldIno.Gid = 102
	oldIno.Generation = 104
	oldIno.CreateTime = 105
	oldIno.AccessTime = 106
	oldIno.ModifyTime = 107
	oldIno.NLink = 108
	oldIno.Flag = 109
	oldIno.StorageClass = proto.StorageClass_Replica_SSD
	oldIno.HybridCloudExtents.sortedEks = NewSortedExtentsFromEks([]proto.ExtentKey{{FileOffset: 100}})
	oldIno.PoolId = 101
	log.SetLogLevelV2(log.WarnLevel)

	buff := GetInodeBuf()
	defer PutInodeBuf(buff)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := oldIno.MarshalV2(buff)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInodeUnmarshal(b *testing.B) {
	oldIno := NewInode(1024, 0)
	oldIno.Uid = 101
	oldIno.Gid = 102
	oldIno.Generation = 104
	oldIno.CreateTime = 105
	oldIno.AccessTime = 106
	oldIno.ModifyTime = 107
	oldIno.NLink = 108
	oldIno.Flag = 109
	oldIno.StorageClass = proto.StorageClass_Replica_SSD
	oldIno.HybridCloudExtents.sortedEks = NewSortedExtentsFromEks([]proto.ExtentKey{{FileOffset: 100}})
	oldIno.PoolId = 101

	buff := GetInodeBuf()
	defer PutInodeBuf(buff)

	log.SetLogLevelV2(log.WarnLevel)

	err := oldIno.MarshalV2(buff)
	if err != nil {
		b.Fatal(err)
	}
	data := buff.Bytes()
	newInode := NewInode(0, 0)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err = newInode.Unmarshal(data)
		if err != nil {
			b.Fail()
		}
	}

	_ = newInode
}

// TestInodeV5PoolIdCompatibility tests compatibility between old version (v4) and new version (v5) formats
func TestInodeV5PoolIdCompatibility(t *testing.T) {
	// Helper function to create a test inode
	createInode := func(poolId uint8, storageClass uint32) *Inode {
		ino := NewInode(1024, 0)
		ino.Uid = 101
		ino.Gid = 102
		ino.Generation = 104
		ino.CreateTime = 105
		ino.AccessTime = 106
		ino.ModifyTime = 107
		ino.NLink = 108
		ino.Flag = 109
		ino.StorageClass = storageClass
		ino.PoolId = poolId
		ino.ClientID = 1001
		ino.LeaseExpireTime = 2000

		if proto.IsStorageClassReplica(storageClass) {
			ino.HybridCloudExtentsMigration = &SortedHybridCloudExtentsMigration{
				storageClass: storageClass,
				poolId:       101,
				expiredTime:  2000,
				sortedEks:    NewSortedExtentsFromEks([]proto.ExtentKey{{FileOffset: 100}}),
			}
		} else {
			ino.HybridCloudExtentsMigration = &SortedHybridCloudExtentsMigration{
				storageClass: storageClass,
				poolId:       101,
				expiredTime:  2000,
				sortedEks:    NewSortedObjExtentsFromObjEks([]proto.ObjExtentKey{{Size: uint64(100), FileOffset: uint64(100)}}),
			}
		}
		return ino
	}

	marshalOldVersion := func(ino *Inode) ([]byte, error) {
		buff := GetInodeBuf()
		// defer PutInodeBuf(buff)
		oldMarshalInode(ino, buff)
		return buff.Bytes(), nil
	}

	unmarshalOldVersion := func(data []byte) (*Inode, error) {
		buff := GetReadBuf(data)
		defer PutReadBuf(buff)

		ino := NewInode(0, 0)
		err := oldUnmarshalInodeValueV2(ino, buff)
		if err != nil {
			return nil, err
		}
		return ino, nil
	}

	ino := createInode(1, proto.StorageClass_Replica_SSD)
	data1, err := marshalOldVersion(ino)
	require.NoError(t, err)

	targetIno := NewInode(0, 0)
	err = targetIno.UnmarshalInodeValueV2(GetReadBuf(data1))
	require.NoError(t, err)

	data2, err := marshalOldVersion(targetIno)
	require.NoError(t, err)
	assert.Equal(t, data1, data2)

	// Test 1: New version marshal -> New version unmarshal (normal case)
	t.Run("NewVersionRoundTrip", func(t *testing.T) {
		testCases := []struct {
			name           string
			poolId         uint8
			storageClass   uint32
			expectedPoolId uint8
		}{
			{"SSD with PoolId 1", 1, proto.StorageClass_Replica_SSD, 1},
			{"HDD with PoolId 2", 2, proto.StorageClass_Replica_HDD, 2},
			{"BlobStore with PoolId 3", 3, proto.StorageClass_BlobStore, 3},
			{"BlobStore with PoolId 4", 4, proto.StorageClass_BlobStore, 4},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				ino := createInode(tc.poolId, tc.storageClass)
				data, err := ino.Marshal()
				require.NoError(t, err)

				targetIno := NewInode(0, 0)
				err = targetIno.Unmarshal(data)
				require.NoError(t, err)

				assert.Equal(t, tc.expectedPoolId, targetIno.PoolId, "PoolId should match")
				assert.Equal(t, ino.StorageClass, targetIno.StorageClass, "StorageClass should match")
				assert.Equal(t, ino.ClientID, targetIno.ClientID, "ClientID should match")
				assert.Equal(t, ino.HybridCloudExtentsMigration.poolId, targetIno.HybridCloudExtentsMigration.poolId, "HybridCloudExtentsMigration.poolId should match")
			})
		}
	})

	// Test 2: Old version data (v4, without v5) -> New version unmarshal
	// This tests forward compatibility: new code should be able to read old data
	t.Run("OldVersionToNewVersion", func(t *testing.T) {
		testCases := []struct {
			name           string
			storageClass   uint32
			expectedPoolId uint8
		}{
			{"SSD should get default PoolId", proto.StorageClass_Replica_SSD, proto.DefaultSSDPoolId},
			{"HDD should get default PoolId", proto.StorageClass_Replica_HDD, proto.DefaultHDDPoolId},
			{"BlobStore should get default PoolId", proto.StorageClass_BlobStore, proto.DefaultECPoolId},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				// Create inode and marshal with old version (no v5 section)
				ino := createInode(0, tc.storageClass)
				oldData, err := marshalOldVersion(ino)
				require.NoError(t, err)

				// Unmarshal with new version code
				targetIno := NewInode(0, 0)
				buf := GetReadBuf(oldData)
				err = targetIno.UnmarshalInodeValueV2(buf)
				require.NoError(t, err, "New version should be able to unmarshal old version data")

				// New version should automatically assign PoolId based on StorageClass
				assert.Equal(t, tc.expectedPoolId, targetIno.PoolId,
					"New version should assign default PoolId based on StorageClass when v5 section is missing")
				assert.Equal(t, ino.StorageClass, targetIno.StorageClass, "StorageClass should match")
				assert.Equal(t, ino.ClientID, targetIno.ClientID, "ClientID should match")
			})
		}
	})

	// Test 3: New version data (v4+v5) -> Old version unmarshal simulation
	// This tests backward compatibility: old code should be able to read new data (ignoring v5)
	// We simulate this by using old version unmarshal function
	t.Run("NewVersionToOldVersion", func(t *testing.T) {
		testCases := []struct {
			name         string
			poolId       uint8
			storageClass uint32
		}{
			{"SSD with PoolId 1", 1, proto.StorageClass_Replica_SSD},
			{"HDD with PoolId 2", 2, proto.StorageClass_Replica_HDD},
			{"BlobStore with PoolId 3", 3, proto.StorageClass_BlobStore},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				// Marshal with new version (includes v5)
				ino := createInode(tc.poolId, tc.storageClass)

				buff := GetInodeBuf()
				ino.MarshalValueV2(buff)

				targetIno, err := unmarshalOldVersion(buff.Bytes())
				require.NoError(t, err, "Old version should be able to unmarshal new data (ignoring v5)")

				// Old version would have PoolId=0, but new version code will set default
				// So we check that StorageClass and other v4 fields are preserved
				assert.Equal(t, uint8(0), targetIno.PoolId, "Old version should have PoolId=0")
				assert.Equal(t, ino.StorageClass, targetIno.StorageClass, "StorageClass should match")
				assert.Equal(t, ino.ClientID, targetIno.ClientID, "ClientID should match")
				assert.Equal(t, ino.LeaseExpireTime, targetIno.LeaseExpireTime, "LeaseExpireTime should match")
			})
		}
	})

	// Test 4: Multiple round trips should be consistent
	t.Run("MultipleRoundTrips", func(t *testing.T) {
		ino := createInode(1, proto.StorageClass_Replica_SSD)

		// First round trip
		data1, err := ino.Marshal()
		require.NoError(t, err)
		ino1 := NewInode(0, 0)
		err = ino1.Unmarshal(data1)
		require.NoError(t, err)

		// Second round trip
		data2, err := ino1.Marshal()
		require.NoError(t, err)
		ino2 := NewInode(0, 0)
		err = ino2.Unmarshal(data2)
		require.NoError(t, err)

		// Should be consistent
		assert.Equal(t, ino.PoolId, ino1.PoolId, "PoolId should be consistent after first round trip")
		assert.Equal(t, ino1.PoolId, ino2.PoolId, "PoolId should be consistent after second round trip")
		assert.Equal(t, ino.StorageClass, ino2.StorageClass, "StorageClass should be consistent")
	})

	// Test 5: v5 section persisted with PoolId=0 should derive pool from StorageClass on load
	t.Run("V5ZeroPoolIdBackfillOnLoad", func(t *testing.T) {
		patchV5PoolIds := func(data []byte, poolId, migPoolId uint8) []byte {
			patched := append([]byte(nil), data...)
			patched[len(patched)-2] = poolId
			patched[len(patched)-1] = migPoolId
			return patched
		}

		testCases := []struct {
			name           string
			storageClass   uint32
			expectedPoolId uint8
		}{
			{"SSD", proto.StorageClass_Replica_SSD, proto.DefaultSSDPoolId},
			{"HDD", proto.StorageClass_Replica_HDD, proto.DefaultHDDPoolId},
			{"BlobStore", proto.StorageClass_BlobStore, proto.DefaultECPoolId},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				ino := createInode(0, tc.storageClass)
				ino.PoolId = 0
				buff := GetInodeBuf()
				ino.MarshalValueV2(buff)
				data := patchV5PoolIds(buff.Bytes(), 0, 101)

				targetIno := NewInode(0, 0)
				err := targetIno.UnmarshalInodeValueV2(GetReadBuf(data))
				require.NoError(t, err)
				assert.EqualValues(t, tc.expectedPoolId, targetIno.PoolId)
			})
		}
	})

	// Test 6: v5 section with migration poolId=0 should backfill from migration storageClass
	t.Run("V5ZeroMigrationPoolIdBackfillOnLoad", func(t *testing.T) {
		ino := createInode(proto.DefaultSSDPoolId, proto.StorageClass_Replica_SSD)
		buff := GetInodeBuf()
		ino.MarshalValueV2(buff)
		data := append([]byte(nil), buff.Bytes()...)
		data[len(data)-1] = 0

		targetIno := NewInode(0, 0)
		err := targetIno.UnmarshalInodeValueV2(GetReadBuf(data))
		require.NoError(t, err)
		assert.EqualValues(t, proto.DefaultSSDPoolId, targetIno.PoolId)
		assert.EqualValues(t, proto.DefaultSSDPoolId, targetIno.HybridCloudExtentsMigration.poolId)
	})

	// Test 7: directory inode with PoolId=0 and Unspecified StorageClass should fail on load
	t.Run("ZeroPoolIdUnspecifiedUnmarshalFails", func(t *testing.T) {
		ino := NewInode(1, uint32(os.ModeDir))
		ino.StorageClass = proto.StorageClass_Unspecified
		ino.PoolId = 0
		oldData, err := marshalOldVersion(ino)
		require.NoError(t, err)

		targetIno := NewInode(0, 0)
		err = targetIno.UnmarshalInodeValueV2(GetReadBuf(oldData))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "PoolId(v5)")
	})

	// Test 8: marshal preserves PoolId=0; backfill happens only on unmarshal
	t.Run("MarshalPreservesZeroPoolId", func(t *testing.T) {
		testCases := []struct {
			name           string
			storageClass   uint32
			expectedPoolId uint8
		}{
			{"SSD", proto.StorageClass_Replica_SSD, proto.DefaultSSDPoolId},
			{"HDD", proto.StorageClass_Replica_HDD, proto.DefaultHDDPoolId},
			{"BlobStore", proto.StorageClass_BlobStore, proto.DefaultECPoolId},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				ino := NewInode(1024, 0)
				ino.StorageClass = tc.storageClass
				ino.PoolId = 0

				buff := GetInodeBuf()
				ino.MarshalValueV2(buff)
				assert.EqualValues(t, 0, ino.PoolId,
					"marshal should not backfill in-memory PoolId")

				targetIno := NewInode(0, 0)
				err := targetIno.UnmarshalInodeValueV2(GetReadBuf(buff.Bytes()))
				require.NoError(t, err)
				assert.EqualValues(t, tc.expectedPoolId, targetIno.PoolId)
			})
		}
	})

	t.Run("MarshalPreservesZeroPoolIdUnspecified", func(t *testing.T) {
		ino := NewInode(1024, uint32(os.ModeDir))
		ino.StorageClass = proto.StorageClass_Unspecified
		ino.PoolId = 0

		buff := GetInodeBuf()
		require.NotPanics(t, func() {
			ino.MarshalValueV2(buff)
		})
		assert.EqualValues(t, 0, ino.PoolId)

		targetIno := NewInode(0, 0)
		err := targetIno.UnmarshalInodeValueV2(GetReadBuf(buff.Bytes()))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "PoolId(v5)")
	})
}

func oldMarshalInode(i *Inode, buff *buf.ByteBufExt) {
	var err error
	skipTimeFields := false

	// reset reserved, V4EBSExtentsFlag maybe changed after migration .eg
	reserved := uint64(0)
	defer func() {
		if err := recover(); err != nil {
			log.LogErrorf("MarshalInodeValue ino(%v)  storageClass(%v) reserved(%d) Recovered from panic:%v",
				i.String(), i.StorageClass, reserved, err)
			log.LogFlush()
			panic(err)
		}
		i.Reserved = reserved
	}()

	if err = buff.PutUint32(uint32(i.Type)); err != nil {
		panic(err)
	}

	if err = buff.PutUint32(uint32(i.Uid)); err != nil {
		panic(err)
	}

	if err = buff.PutUint32(uint32(i.Gid)); err != nil {
		panic(err)
	}

	if err = buff.PutUint64(i.Size); err != nil {
		panic(err)
	}

	if err = buff.PutUint64(i.Generation); err != nil {
		panic(err)
	}

	if !skipTimeFields {
		if err = buff.PutUint64(uint64(i.CreateTime)); err != nil {
			panic(err)
		}

		if err = buff.PutUint64(uint64(i.AccessTime)); err != nil {
			panic(err)
		}

		if err = buff.PutUint64(uint64(i.ModifyTime)); err != nil {
			panic(err)
		}
	}

	// write SymLink
	symSize := uint32(len(i.LinkTarget))

	if err = buff.PutUint32(symSize); err != nil {
		panic(err)
	}

	if _, err = buff.Write(i.LinkTarget); err != nil {
		panic(err)
	}

	if err = buff.PutUint32(i.NLink); err != nil {
		panic(err)
	}

	if err = buff.PutUint32(uint32(i.Flag)); err != nil {
		panic(err)
	}

	enableSnapshot := false
	if i.multiSnap != nil {
		reserved |= V3EnableSnapInodeFlag
		enableSnapshot = true
	}

	reserved |= V4EnableHybridCloud
	isFile := proto.IsRegular(i.Type)
	// to check flag

	if !proto.IsValidStorageClass(i.StorageClass) && isFile && i.Size > 0 {
		panic(fmt.Sprintf("ino(%v) MarshalInodeValue failed, unsupport StorageClass %v", i.Inode, i.StorageClass))
	}

	if proto.IsStorageClassBlobStore(i.StorageClass) {
		if i.HybridCloudExtents.sortedEks != nil {
			ObjExtents := i.HybridCloudExtents.sortedEks.(*SortedObjExtents)
			if ObjExtents != nil && len(ObjExtents.eks) > 0 {
				// i.Reserved |= V4EBSExtentsFlag
				reserved |= V2EnableEbsFlag
			}
		}
	}

	if i.HybridCloudExtentsMigration != nil && i.HybridCloudExtentsMigration.storageClass != proto.StorageClass_Unspecified {
		reserved |= V4MigrationExtentsFlag
	}

	if err = buff.PutUint64(reserved); err != nil {
		panic(err)
	}

	if reserved&V2EnableEbsFlag > 0 {
		// marshal cache ExtentsKey
		if err = buff.PutUint32(uint32(0)); err != nil {
			panic(err)
		}

		ObjExtents := i.HybridCloudExtents.sortedEks.(*SortedObjExtents)
		objExtData, err := ObjExtents.MarshalBinary()
		if err != nil {
			panic(err)
		}
		if err = buff.PutUint32(uint32(len(objExtData))); err != nil {
			panic(err)
		}
		if _, err = buff.Write(objExtData); err != nil {
			panic(err)
		}
	} else {
		var dataLen int
		var extData []byte
		if i.HybridCloudExtents.HasReplicaExts() {
			replicaExtents := i.HybridCloudExtents.sortedEks.(*SortedExtents)
			tmpBuf1 := GetInodeBuf()
			defer PutInodeBuf(tmpBuf1)

			err = replicaExtents.MarshalBinary(tmpBuf1, enableSnapshot)
			if err != nil {
				panic(err)
			}
			extData = tmpBuf1.Bytes()
			dataLen = len(extData)
		}

		if err = buff.PutUint32(uint32(dataLen)); err != nil {
			panic(err)
		}
		if _, err = buff.Write(extData); err != nil {
			panic(err)
		}
	}

	if i.multiSnap != nil {
		if err = buff.PutUint64(i.getVer()); err != nil {
			panic(err)
		}
	}

	if err = buff.PutUint32(i.StorageClass); err != nil {
		panic(err)
	}

	if err = buff.PutUint32(i.ClientID); err != nil {
		panic(err)
	}

	if !skipTimeFields {
		if err = buff.PutUint64(i.LeaseExpireTime); err != nil {
			panic(err)
		}
	}

	if reserved&V4MigrationExtentsFlag > 0 {
		sem := i.HybridCloudExtentsMigration

		if err = buff.PutUint32(sem.storageClass); err != nil {
			panic(err)
		}

		if err = buff.PutUint64(uint64(sem.expiredTime)); err != nil {
			panic(err)
		}

		if sem.Empty() {
			if err = buff.PutUint32(uint32(0)); err != nil {
				panic(err)
			}
			return
		}

		if proto.IsStorageClassReplica(sem.storageClass) {
			replicaExtents, ok := sem.sortedEks.(*SortedExtents)
			if !ok {
				panic(errors.New(fmt.Sprintf("MarshalInodeValue failed, inode(%v) StorageClass(%v) but type of sortedEks not match",
					i.Inode, sem.storageClass)))
			}

			tmpBuf := GetInodeBuf()
			defer PutInodeBuf(tmpBuf)

			err = replicaExtents.MarshalBinary(tmpBuf, enableSnapshot)
			if err != nil {
				panic(err)
			}
			extData := tmpBuf.Bytes()

			if err = buff.PutUint32(uint32(len(extData))); err != nil {
				panic(err)
			}

			if _, err = buff.Write(extData); err != nil {
				panic(err)
			}
		} else if proto.IsStorageClassBlobStore(sem.storageClass) {
			ObjExtents := sem.sortedEks.(*SortedObjExtents)
			objExtData, err := ObjExtents.MarshalBinary()
			if err != nil {
				panic(err)
			}
			if err = binary.Write(buff, binary.BigEndian, uint32(len(objExtData))); err != nil {
				panic(err)
			}
			if _, err = buff.Write(objExtData); err != nil {
				panic(err)
			}
		} else {
			log.LogFlush()
			panic(errors.New(fmt.Sprintf("MarshalInodeValue failed, inode(%v) unsupport migrate StorageClass(%v)",
				i.Inode, sem.storageClass)))
		}
	}
}

func oldUnmarshalInodeValueV2(i *Inode, buff *buf.ReadByteBuff) (err error) {
	if i.Type, err = buff.ReadUint32(); err != nil {
		err = UnmarshalInodeFiledError("Type", err)
		return
	}

	if i.Uid, err = buff.ReadUint32(); err != nil {
		err = UnmarshalInodeFiledError("Uid", err)
		return
	}

	if i.Gid, err = buff.ReadUint32(); err != nil {
		err = UnmarshalInodeFiledError("Gid", err)
		return
	}

	if i.Size, err = buff.ReadUint64(); err != nil {
		err = UnmarshalInodeFiledError("Size", err)
		return
	}

	if i.Generation, err = buff.ReadUint64(); err != nil {
		err = UnmarshalInodeFiledError("Generation", err)
		return
	}

	if i.CreateTime, err = buff.ReadInt64(); err != nil {
		err = UnmarshalInodeFiledError("CreateTime", err)
		return
	}

	if i.AccessTime, err = buff.ReadInt64(); err != nil {
		err = UnmarshalInodeFiledError("AccessTime", err)
		return
	}

	if i.ModifyTime, err = buff.ReadInt64(); err != nil {
		err = UnmarshalInodeFiledError("ModifyTime", err)
		return
	}

	// read symLink
	symSize := uint32(0)
	if symSize, err = buff.ReadUint32(); err != nil {
		err = UnmarshalInodeFiledError("symSize", err)
		return
	}

	if symSize > 0 {
		if symSize > proto.MaxBufferSize {
			return proto.ErrBufferSizeExceedMaximum
		}
		i.LinkTarget = make([]byte, symSize)
		if _, err = io.ReadFull(buff, i.LinkTarget); err != nil {
			err = UnmarshalInodeFiledError("LinkTarget", err)
			return
		}
	}

	if i.NLink, err = buff.ReadUint32(); err != nil {
		err = UnmarshalInodeFiledError("NLink", err)
		return
	}

	flag := uint32(0)
	if flag, err = buff.ReadUint32(); err != nil {
		err = UnmarshalInodeFiledError("Flag", err)
		return
	}
	i.Flag = int32(flag)

	if i.Reserved, err = buff.ReadUint64(); err != nil {
		err = UnmarshalInodeFiledError("Reserved", err)
		return
	}

	if i.HybridCloudExtents == nil {
		i.HybridCloudExtents = NewSortedHybridCloudExtents()
	}

	if i.HybridCloudExtentsMigration == nil {
		i.HybridCloudExtentsMigration = NewSortedHybridCloudExtentsMigration()
	}

	isFile := i.IsFile()
	v3 := i.Reserved&V3EnableSnapInodeFlag > 0
	v4 := i.Reserved&V4EnableHybridCloud > 0

	if i.Reserved == 0 {
		extents := NewSortedExtents()
		if err, _ = extents.UnmarshalBinary(buff.Bytes(), false); err != nil {
			return fmt.Errorf("UnmarshalBinary failed, ino %d, ino %v", i.Inode, i)
		}
		if extents.Len() > 0 {
			i.HybridCloudExtents.sortedEks = extents
		}

		i.StorageClass = legacyReplicaStorageClass
		if i.StorageClass == proto.StorageClass_Unspecified && isFile && extents.Len() > 0 {
			return fmt.Errorf("UnmarshalInodeValue: legacyReplicaStorageClass not set in config, ino %d", i.Inode)
		}
		return
	}

	if i.Reserved&V2EnableEbsFlag > 0 {
		// unmarshal extents cache for old version
		extSize := uint32(0)
		if extSize, err = buff.ReadUint32(); err != nil {
			err = UnmarshalInodeFiledError("extSize(v4)", err)
			return
		}

		// TODO remove in next version
		if extSize > 0 {
			extBytes := make([]byte, extSize)
			log.LogErrorf("attention: ummarshal got cache extents not zero, ino %d, size %d", i.Inode, extSize)
			if _, err = io.ReadFull(buff, extBytes); err != nil {
				err = UnmarshalInodeFiledError("extBytes(v4)", err)
				return
			}
		}

		ObjExtSize := uint32(0)
		if ObjExtSize, err = buff.ReadUint32(); err != nil {
			err = UnmarshalInodeFiledError("HybridCloudExtents.ObjExtSize(v4)", err)
			return
		}

		if ObjExtSize > 0 {
			objExtBytes := make([]byte, ObjExtSize)
			if _, err = io.ReadFull(buff, objExtBytes); err != nil {
				err = UnmarshalInodeFiledError("HybridCloudExtents.objExtBytes(v4)", err)
				return
			}
			ObjExtents := NewSortedObjExtents()
			if err = ObjExtents.UnmarshalBinary(objExtBytes); err != nil {
				err = UnmarshalInodeFiledError("HybridCloudExtents.ObjExtents(v4)", err)
				return
			}
			i.HybridCloudExtents.sortedEks = ObjExtents
		}
		i.StorageClass = proto.StorageClass_BlobStore
	} else {
		extSize := uint32(0)
		if extSize, err = buff.ReadUint32(); err != nil {
			err = UnmarshalInodeFiledError("HybridCloudExtents.extSize(v4)", err)
			return
		}

		if extSize > 0 {
			var ekRef *sync.Map
			var err1 error
			eks := NewSortedExtents()
			ekData, err1 := buff.Next(int(extSize))
			if err1 != nil {
				err = UnmarshalInodeFiledError("Read HybridCloudExtents.SortedExtents(v4)", err1)
				return err
			}

			if err, ekRef = eks.UnmarshalBinary(ekData, v3); err != nil {
				err = UnmarshalInodeFiledError("HybridCloudExtents.SortedExtents(v4)", err)
				return
			}

			i.HybridCloudExtents.sortedEks = eks
			if ekRef != nil {
				if i.multiSnap == nil {
					i.multiSnap = NewMultiSnap(0)
				}
				i.multiSnap.ekRefMap = ekRef
			}
		}
		i.StorageClass = legacyReplicaStorageClass
		if !proto.IsValidStorageClass(i.StorageClass) {
			i.StorageClass = proto.StorageClass_BlobStore
		}
	}

	if v3 {
		var seq uint64
		if seq, err = buff.ReadUint64(); err != nil {
			err = UnmarshalInodeFiledError("multiSnap.verSeq(v4)", err)
			log.LogWarnf("[UnmarshalInodeValue] ino(%v) err[%v]", i, err.Error())
			return
		}

		if seq != 0 {
			i.setVer(seq)
		}
	}

	// hybridcloud format
	if v4 {
		if i.StorageClass, err = buff.ReadUint32(); err != nil {
			err = UnmarshalInodeFiledError("StorageClass(v4)", err)
			return
		}

		if i.ClientID, err = buff.ReadUint32(); err != nil {
			err = UnmarshalInodeFiledError("ForbiddenMigration(v4)", err)
			return
		}

		if i.LeaseExpireTime, err = buff.ReadUint64(); err != nil {
			err = UnmarshalInodeFiledError("LeaseExpireTime(v4)", err)
			return
		}

		if i.StorageClass == proto.StorageClass_Unspecified && isFile {
			i.StorageClass = proto.StorageClass_BlobStore
		}

		if i.Reserved&V4MigrationExtentsFlag > 0 {
			if i.HybridCloudExtentsMigration == nil {
				i.HybridCloudExtentsMigration = NewSortedHybridCloudExtentsMigration()
			}

			if i.HybridCloudExtentsMigration.storageClass, err = buff.ReadUint32(); err != nil {
				err = UnmarshalInodeFiledError("HybridCloudExtentsMigration.storageClass(v4)", err)
				return
			}

			if i.HybridCloudExtentsMigration.expiredTime, err = buff.ReadInt64(); err != nil {
				err = UnmarshalInodeFiledError("HybridCloudExtentsMigration.expiredTime(v4)", err)
				return
			}

			if proto.IsStorageClassReplica(i.HybridCloudExtentsMigration.storageClass) {
				extSize := uint32(0)
				if extSize, err = buff.ReadUint32(); err != nil {
					err = UnmarshalInodeFiledError("HybridCloudExtentsMigration.extSize(v4)", err)
					return
				}

				if extSize > 0 {
					extBytes, err1 := buff.Next(int(extSize))
					if err1 != nil {
						err = UnmarshalInodeFiledError("HybridCloudExtentsMigration.extBytes(v4)", err1)
						return
					}
					i.HybridCloudExtentsMigration.sortedEks = NewSortedExtents()
					if err, _ = i.HybridCloudExtentsMigration.sortedEks.(*SortedExtents).UnmarshalBinary(extBytes, v3); err != nil {
						err = UnmarshalInodeFiledError("HybridCloudExtentsMigration.SortedExtents(v4)", err)
						return
					}
				}

			} else if proto.IsStorageClassBlobStore(i.HybridCloudExtentsMigration.storageClass) {
				ObjExtSize := uint32(0)
				if err = binary.Read(buff, binary.BigEndian, &ObjExtSize); err != nil {
					err = UnmarshalInodeFiledError("HybridCloudExtentsMigration.ObjExtSize(v4)", err)
					return
				}
				log.LogDebugf("[UnmarshalInodeValue] ino(%v) migrateStorageClass(%v) ObjExtSize(%v)",
					i.Inode, i.HybridCloudExtentsMigration.storageClass, ObjExtSize)
				if ObjExtSize > 0 {
					objExtBytes := make([]byte, ObjExtSize)
					if _, err = io.ReadFull(buff, objExtBytes); err != nil {
						err = UnmarshalInodeFiledError("HybridCloudExtentsMigration.objExtBytes(v4)", err)
						return
					}
					ObjExtents := NewSortedObjExtents()
					if err = ObjExtents.UnmarshalBinary(objExtBytes); err != nil {
						err = UnmarshalInodeFiledError("HybridCloudExtentsMigration.ObjExtents(v4)", err)
						return
					}
					i.HybridCloudExtentsMigration.sortedEks = ObjExtents
				}
			}
		}
	}
	return
}
