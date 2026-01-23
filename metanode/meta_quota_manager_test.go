// Copyright 2023 The CubeFS Authors.
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

package metanode

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testVolName = "test_volume"
	testMpID    = uint64(1)
	testQuotaID = uint32(100)
)

func TestMetaQuotaNewQuotaManager(t *testing.T) {
	mgr := NewQuotaManager(testVolName, testMpID)

	assert.NotNil(t, mgr)
	assert.Equal(t, testVolName, mgr.volName)
	assert.Equal(t, testMpID, mgr.mpID)
	assert.NotNil(t, mgr.statisticBase)
	assert.NotNil(t, mgr.storeRebuildBase)
	assert.NotNil(t, mgr.limitedMap)
	assert.False(t, mgr.enable)
	assert.False(t, mgr.rbuildbySnapshot)
}

func TestMetaQuotaInodeMarshalUnmarshal(t *testing.T) {
	// Create test inode with proper StorageClass
	inode := NewInode(1, 2)
	inode.NLink = 1
	inode.Size = 1024
	inode.Type = 0o644
	inode.StorageClass = proto.StorageClass_Replica_SSD // Set valid StorageClass
	inode.PoolId = proto.DefaultSSDPoolId

	quotaIds := []uint32{100, 200, 300}
	qInode := &MetaQuotaInode{
		inode:    inode,
		quotaIds: quotaIds,
	}

	// Test Marshal
	data, err := qInode.Marshal()
	require.NoError(t, err)
	assert.NotEmpty(t, data)

	// Test Unmarshal
	newQInode := &MetaQuotaInode{}
	err = newQInode.Unmarshal(data)
	require.NoError(t, err)

	assert.Equal(t, qInode.inode.Inode, newQInode.inode.Inode)
	assert.Equal(t, qInode.inode.NLink, newQInode.inode.NLink)
	assert.Equal(t, qInode.inode.Size, newQInode.inode.Size)
	assert.Equal(t, qInode.inode.Type, newQInode.inode.Type)
	assert.Equal(t, qInode.quotaIds, newQInode.quotaIds)
}

func TestMetaQuotaInodeMarshalEmptyQuotaIds(t *testing.T) {
	inode := NewInode(1, 2)
	inode.StorageClass = proto.StorageClass_Replica_SSD // Set valid StorageClass
	inode.PoolId = proto.DefaultSSDPoolId
	qInode := &MetaQuotaInode{
		inode:    inode,
		quotaIds: []uint32{},
	}

	data, err := qInode.Marshal()
	require.NoError(t, err)

	newQInode := &MetaQuotaInode{}
	err = newQInode.Unmarshal(data)
	require.NoError(t, err)

	assert.Equal(t, qInode.inode.Inode, newQInode.inode.Inode)
	assert.Empty(t, newQInode.quotaIds)
}

func TestMetaQuotaTxMetaQuotaInodeMarshalUnmarshal(t *testing.T) {
	// Create test txinode with proper StorageClass and TransactionInfo
	txInfo := &proto.TransactionInfo{
		TxID:         "test_tx_123",
		TxType:       proto.TxTypeCreate,
		TmID:         1,
		CreateTime:   time.Now().Unix(),
		Timeout:      30,
		State:        proto.TxStateInit,
		DoneTime:     0,
		RMFinish:     false,
		TxInodeInfos: make(map[uint64]*proto.TxInodeInfo),
	}

	txinode := NewTxInode(1, 2, txInfo)
	txinode.Inode.NLink = 1
	txinode.Inode.Size = 2048
	txinode.Inode.Type = 0o755
	txinode.Inode.StorageClass = proto.StorageClass_Replica_HDD // Set valid StorageClass
	txinode.Inode.PoolId = proto.DefaultHDDPoolId

	quotaIds := []uint32{400, 500}
	txQInode := &TxMetaQuotaInode{
		txinode:  txinode,
		quotaIds: quotaIds,
	}

	// Test Marshal
	data, err := txQInode.Marshal()
	require.NoError(t, err)
	assert.NotEmpty(t, data)

	// Test Unmarshal
	newTxQInode := &TxMetaQuotaInode{}
	err = newTxQInode.Unmarshal(data)
	require.NoError(t, err)

	assert.Equal(t, txQInode.txinode.Inode.Inode, newTxQInode.txinode.Inode.Inode)
	assert.Equal(t, txQInode.txinode.Inode.NLink, newTxQInode.txinode.Inode.NLink)
	assert.Equal(t, txQInode.txinode.Inode.Size, newTxQInode.txinode.Inode.Size)
	assert.Equal(t, txQInode.txinode.Inode.Type, newTxQInode.txinode.Inode.Type)
	assert.Equal(t, txQInode.quotaIds, newTxQInode.quotaIds)
}

func TestMetaQuotaInodeUnmarshalInvalidData(t *testing.T) {
	qInode := &MetaQuotaInode{}

	// Test with empty data
	err := qInode.Unmarshal([]byte{})
	assert.Error(t, err)

	// Test with invalid data
	err = qInode.Unmarshal([]byte{1, 2, 3})
	assert.Error(t, err)
}

func TestMetaQuotaManagerSetQuotaHbInfo(t *testing.T) {
	mgr := NewQuotaManager(testVolName, testMpID)

	// Test setting quota info
	infos := []*proto.QuotaHeartBeatInfo{
		{
			VolName:     testVolName,
			QuotaId:     testQuotaID,
			LimitedInfo: proto.QuotaLimitedInfo{LimitedFiles: true, LimitedBytes: true},
			Enable:      true,
		},
		{
			VolName:     testVolName,
			QuotaId:     200,
			LimitedInfo: proto.QuotaLimitedInfo{LimitedFiles: false, LimitedBytes: true},
			Enable:      true,
		},
	}

	mgr.setQuotaHbInfo(infos)

	assert.True(t, mgr.enable)

	// Check if quota info is stored
	value, exists := mgr.limitedMap.Load(testQuotaID)
	assert.True(t, exists)
	limitedInfo := value.(proto.QuotaLimitedInfo)
	assert.True(t, limitedInfo.LimitedFiles)
	assert.True(t, limitedInfo.LimitedBytes)
}

func TestMetaQuotaManagerSetQuotaHbInfoDifferentVolume(t *testing.T) {
	mgr := NewQuotaManager(testVolName, testMpID)

	infos := []*proto.QuotaHeartBeatInfo{
		{
			VolName:     "different_volume",
			QuotaId:     testQuotaID,
			LimitedInfo: proto.QuotaLimitedInfo{LimitedFiles: true, LimitedBytes: true},
			Enable:      true,
		},
	}

	mgr.setQuotaHbInfo(infos)

	// Should not be enabled for different volume
	assert.False(t, mgr.enable)

	// Quota should not be stored for different volume
	_, exists := mgr.limitedMap.Load(testQuotaID)
	assert.False(t, exists)
}

func TestMetaQuotaManagerSetQuotaHbInfoCleanup(t *testing.T) {
	mgr := NewQuotaManager(testVolName, testMpID)

	// First set some quotas
	infos1 := []*proto.QuotaHeartBeatInfo{
		{
			VolName:     testVolName,
			QuotaId:     100,
			LimitedInfo: proto.QuotaLimitedInfo{LimitedFiles: true, LimitedBytes: true},
			Enable:      true,
		},
		{
			VolName:     testVolName,
			QuotaId:     200,
			LimitedInfo: proto.QuotaLimitedInfo{LimitedFiles: false, LimitedBytes: true},
			Enable:      true,
		},
	}
	mgr.setQuotaHbInfo(infos1)

	// Verify quotas are stored
	_, exists1 := mgr.limitedMap.Load(uint32(100))
	assert.True(t, exists1)
	_, exists2 := mgr.limitedMap.Load(uint32(200))
	assert.True(t, exists2)

	// Update with fewer quotas
	infos2 := []*proto.QuotaHeartBeatInfo{
		{
			VolName:     testVolName,
			QuotaId:     100,
			LimitedInfo: proto.QuotaLimitedInfo{LimitedFiles: true, LimitedBytes: true},
			Enable:      true,
		},
	}
	mgr.setQuotaHbInfo(infos2)

	// Verify cleanup
	_, exists1 = mgr.limitedMap.Load(uint32(100))
	assert.True(t, exists1)
	_, exists2 = mgr.limitedMap.Load(uint32(200))
	assert.False(t, exists2)
}

func TestMetaQuotaManagerGetQuotaReportInfos(t *testing.T) {
	mgr := NewQuotaManager(testVolName, testMpID)

	// Set up test data
	usedInfo := proto.QuotaUsedInfo{
		UsedFiles: 10,
		UsedBytes: 1024,
	}
	mgr.statisticBase.Store(testQuotaID, usedInfo)
	mgr.limitedMap.Store(testQuotaID, proto.QuotaLimitedInfo{LimitedFiles: true, LimitedBytes: true})

	// Get report infos
	infos := mgr.getQuotaReportInfos()

	assert.Len(t, infos, 1)
	assert.Equal(t, testQuotaID, infos[0].QuotaId)
	assert.Equal(t, usedInfo, infos[0].UsedInfo)
}

func TestMetaQuotaManagerGetQuotaReportInfosNoLimitedQuota(t *testing.T) {
	mgr := NewQuotaManager(testVolName, testMpID)

	// Set up test data without limited quota
	usedInfo := proto.QuotaUsedInfo{
		UsedFiles: 10,
		UsedBytes: 1024,
	}
	mgr.statisticBase.Store(testQuotaID, usedInfo)
	// Don't add to limitedMap

	// Get report infos
	infos := mgr.getQuotaReportInfos()

	assert.Empty(t, infos)
}

func TestMetaQuotaManagerStatisticRebuildStart(t *testing.T) {
	mgr := NewQuotaManager(testVolName, testMpID)

	// Test when quota is disabled
	result := mgr.statisticRebuildStart()
	assert.False(t, result)
	assert.False(t, mgr.rbuildbySnapshot)

	// Enable quota
	mgr.enable = true

	// Test first rebuild
	result = mgr.statisticRebuildStart()
	assert.True(t, result)
	assert.True(t, mgr.rbuildbySnapshot)

	// Test second rebuild (should fail)
	result = mgr.statisticRebuildStart()
	assert.False(t, result)
	assert.True(t, mgr.rbuildbySnapshot)
}

func TestMetaQuotaManagerStatisticRebuildFin(t *testing.T) {
	mgr := NewQuotaManager(testVolName, testMpID)
	mgr.rbuildbySnapshot = true

	// Test rebuild = false
	mgr.statisticRebuildFin(false)
	assert.False(t, mgr.rbuildbySnapshot)
	assert.NotNil(t, mgr.storeRebuildBase)

	// Test rebuild = true
	mgr.rbuildbySnapshot = true
	oldStatisticBase := mgr.statisticBase
	mgr.statisticRebuildFin(true)
	assert.False(t, mgr.rbuildbySnapshot)
	assert.Equal(t, oldStatisticBase, mgr.statisticBase)
}

func TestMetaQuotaManagerIsOverQuota(t *testing.T) {
	mgr := NewQuotaManager(testVolName, testMpID)

	// Test when quota is disabled
	status := mgr.IsOverQuota(true, true, testQuotaID)
	assert.Equal(t, uint8(0), status)

	// Enable quota
	mgr.enable = true

	// Test when quota is not found
	status = mgr.IsOverQuota(true, true, testQuotaID)
	assert.Equal(t, uint8(0), status)

	// Set up limited quota
	limitedInfo := proto.QuotaLimitedInfo{
		LimitedFiles: true,
		LimitedBytes: true,
	}
	mgr.limitedMap.Store(testQuotaID, limitedInfo)

	// Test over quota for both size and files
	status = mgr.IsOverQuota(true, true, testQuotaID)
	assert.Equal(t, proto.OpNoSpaceErr, status)

	// Test over quota for size only
	status = mgr.IsOverQuota(true, false, testQuotaID)
	assert.Equal(t, proto.OpNoSpaceErr, status)

	// Test over quota for files only
	status = mgr.IsOverQuota(false, true, testQuotaID)
	assert.Equal(t, proto.OpNoSpaceErr, status)

	// Test not over quota
	status = mgr.IsOverQuota(false, false, testQuotaID)
	assert.Equal(t, uint8(0), status)
}

func TestMetaQuotaManagerIsOverQuotaPartialLimits(t *testing.T) {
	mgr := NewQuotaManager(testVolName, testMpID)
	mgr.enable = true

	// Test with only file limit
	limitedInfo := proto.QuotaLimitedInfo{
		LimitedFiles: true,
		LimitedBytes: false,
	}
	mgr.limitedMap.Store(testQuotaID, limitedInfo)

	// Test size check (should not be over quota)
	status := mgr.IsOverQuota(true, false, testQuotaID)
	assert.Equal(t, uint8(0), status)

	// Test file check (should be over quota)
	status = mgr.IsOverQuota(false, true, testQuotaID)
	assert.Equal(t, proto.OpNoSpaceErr, status)
}

func TestMetaQuotaManagerEnableQuota(t *testing.T) {
	mgr := NewQuotaManager(testVolName, testMpID)

	// Initially disabled
	assert.False(t, mgr.EnableQuota())

	// Enable quota
	mgr.enable = true
	assert.True(t, mgr.EnableQuota())
}

func TestMetaQuotaMarshalQuotaInodeHelper(t *testing.T) {
	inodeBytes := []byte{1, 2, 3, 4, 5}
	quotaIds := []uint32{100, 200, 300}

	data, err := marshalQuotaInode(inodeBytes, quotaIds)
	require.NoError(t, err)
	assert.NotEmpty(t, data)

	// Verify structure
	buff := bytes.NewBuffer(data)
	var inodeLen uint32
	err = binary.Read(buff, binary.BigEndian, &inodeLen)
	require.NoError(t, err)
	assert.Equal(t, uint32(len(inodeBytes)), inodeLen)

	// Read inode bytes
	readInodeBytes := make([]byte, inodeLen)
	_, err = buff.Read(readInodeBytes)
	require.NoError(t, err)
	assert.Equal(t, inodeBytes, readInodeBytes)

	// Read quota IDs
	var readQuotaIds []uint32
	for buff.Len() > 0 {
		var quotaId uint32
		err = binary.Read(buff, binary.BigEndian, &quotaId)
		require.NoError(t, err)
		readQuotaIds = append(readQuotaIds, quotaId)
	}
	assert.Equal(t, quotaIds, readQuotaIds)
}

func TestMetaQuotaUnmarshalQuotaInodeHelper(t *testing.T) {
	// Create test data
	inodeBytes := []byte{1, 2, 3, 4, 5}
	quotaIds := []uint32{100, 200, 300}

	// Marshal first
	data, err := marshalQuotaInode(inodeBytes, quotaIds)
	require.NoError(t, err)

	// Unmarshal
	readInodeBytes, readQuotaIds, err := unmarshalQuotaInode(data)
	require.NoError(t, err)

	assert.Equal(t, inodeBytes, readInodeBytes)
	assert.Equal(t, quotaIds, readQuotaIds)
}

func TestMetaQuotaUnmarshalQuotaInodeHelperInvalidData(t *testing.T) {
	// Test with empty data
	_, _, err := unmarshalQuotaInode([]byte{})
	assert.Error(t, err)

	// Test with invalid data
	_, _, err = unmarshalQuotaInode([]byte{1, 2, 3})
	assert.Error(t, err)
}

func TestMetaQuotaUnmarshalQuotaInodeHelperExceedMaxSize(t *testing.T) {
	// Create data with inode length exceeding MaxBufferSize
	buff := bytes.NewBuffer(make([]byte, 0, 8))
	largeLen := uint32(proto.MaxBufferSize + 1)
	binary.Write(buff, binary.BigEndian, largeLen)

	_, _, err := unmarshalQuotaInode(buff.Bytes())
	assert.Error(t, err)
	assert.Equal(t, proto.ErrBufferSizeExceedMaximum, err)
}

func TestMetaQuotaManagerConcurrentAccess(t *testing.T) {
	mgr := NewQuotaManager(testVolName, testMpID)

	// Test concurrent access to different methods
	done := make(chan bool, 3)

	// Goroutine 1: Set quota info
	go func() {
		infos := []*proto.QuotaHeartBeatInfo{
			{
				VolName:     testVolName,
				QuotaId:     testQuotaID,
				LimitedInfo: proto.QuotaLimitedInfo{LimitedFiles: true, LimitedBytes: true},
				Enable:      true,
			},
		}
		mgr.setQuotaHbInfo(infos)
		done <- true
	}()

	// Goroutine 2: Check quota
	go func() {
		status := mgr.IsOverQuota(true, true, testQuotaID)
		_ = status
		done <- true
	}()

	// Goroutine 3: Get report infos
	go func() {
		infos := mgr.getQuotaReportInfos()
		_ = infos
		done <- true
	}()

	// Wait for all goroutines to complete
	for i := 0; i < 3; i++ {
		<-done
	}
}

func TestMetaQuotaManagerEdgeCases(t *testing.T) {
	_ = NewQuotaManager(testVolName, testMpID)

	// Test with empty quota IDs
	inode := NewInode(1, 2)
	inode.StorageClass = proto.StorageClass_Replica_SSD // Set valid StorageClass
	inode.PoolId = proto.DefaultSSDPoolId

	qInode := &MetaQuotaInode{
		inode:    inode,
		quotaIds: nil,
	}

	data, err := qInode.Marshal()
	require.NoError(t, err)

	newQInode := &MetaQuotaInode{}
	err = newQInode.Unmarshal(data)
	require.NoError(t, err)
	assert.Nil(t, newQInode.quotaIds)

	// Test with single quota ID
	qInode.quotaIds = []uint32{999}
	data, err = qInode.Marshal()
	require.NoError(t, err)

	newQInode = &MetaQuotaInode{}
	err = newQInode.Unmarshal(data)
	require.NoError(t, err)
	assert.Equal(t, []uint32{999}, newQInode.quotaIds)
}

func TestMetaQuotaManagerLargeQuotaIds(t *testing.T) {
	_ = NewQuotaManager(testVolName, testMpID)

	// Test with many quota IDs
	quotaIds := make([]uint32, 1000)
	for i := range quotaIds {
		quotaIds[i] = uint32(i)
	}

	inode := NewInode(1, 2)
	inode.StorageClass = proto.StorageClass_BlobStore // Set valid StorageClass
	inode.PoolId = proto.DefaultECPoolId

	qInode := &MetaQuotaInode{
		inode:    inode,
		quotaIds: quotaIds,
	}

	data, err := qInode.Marshal()
	require.NoError(t, err)

	newQInode := &MetaQuotaInode{}
	err = newQInode.Unmarshal(data)
	require.NoError(t, err)
	assert.Equal(t, quotaIds, newQInode.quotaIds)
}
