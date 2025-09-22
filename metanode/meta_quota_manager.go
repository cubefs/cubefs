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
	"sync"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

type MetaQuotaManager struct {
	statisticBase    *sync.Map // key quotaId, value proto.QuotaUsedInfo
	storeRebuildBase *sync.Map // key quotaId, value proto.QuotaUsedInfo
	limitedMap       *sync.Map
	rbuildbySnapshot bool
	volName          string
	rwlock           sync.RWMutex
	mpID             uint64
	enable           bool
}

type MetaQuotaInode struct {
	inode    *Inode
	quotaIds []uint32
}

type TxMetaQuotaInode struct {
	txinode  *TxInode
	quotaIds []uint32
}

// NewQuotaManager creates a new quota manager instance
func NewQuotaManager(volName string, mpId uint64) *MetaQuotaManager {
	return &MetaQuotaManager{
		statisticBase:    new(sync.Map),
		storeRebuildBase: new(sync.Map),
		limitedMap:       new(sync.Map),
		volName:          volName,
		mpID:             mpId,
	}
}

// marshalQuotaInode is a helper function to marshal quota inode data
func marshalQuotaInode(inodeBytes []byte, quotaIds []uint32) ([]byte, error) {
	buff := bytes.NewBuffer(make([]byte, 0, 128))
	quotaBytes := bytes.NewBuffer(make([]byte, 0, 128))

	inodeLen := uint32(len(inodeBytes))
	if err := binary.Write(buff, binary.BigEndian, inodeLen); err != nil {
		return nil, err
	}

	if _, err := buff.Write(inodeBytes); err != nil {
		return nil, err
	}

	for _, quotaId := range quotaIds {
		if err := binary.Write(quotaBytes, binary.BigEndian, quotaId); err != nil {
			return nil, err
		}
	}

	if _, err := buff.Write(quotaBytes.Bytes()); err != nil {
		return nil, err
	}

	return buff.Bytes(), nil
}

// unmarshalQuotaInode is a helper function to unmarshal quota inode data
func unmarshalQuotaInode(raw []byte) ([]byte, []uint32, error) {
	var inodeLen uint32
	buff := bytes.NewBuffer(raw)

	if err := binary.Read(buff, binary.BigEndian, &inodeLen); err != nil {
		return nil, nil, err
	}

	if inodeLen > proto.MaxBufferSize {
		return nil, nil, proto.ErrBufferSizeExceedMaximum
	}

	inodeBytes := make([]byte, inodeLen)
	if _, err := buff.Read(inodeBytes); err != nil {
		return nil, nil, err
	}

	var quotaIds []uint32
	for buff.Len() > 0 {
		var quotaId uint32
		if err := binary.Read(buff, binary.BigEndian, &quotaId); err != nil {
			return nil, nil, err
		}
		quotaIds = append(quotaIds, quotaId)
	}

	return inodeBytes, quotaIds, nil
}

func (qInode *MetaQuotaInode) Marshal() ([]byte, error) {
	inodeBytes, err := qInode.inode.Marshal()
	if err != nil {
		return nil, err
	}

	result, err := marshalQuotaInode(inodeBytes, qInode.quotaIds)
	if err != nil {
		return nil, err
	}

	log.LogDebugf("MetaQuotaInode Marshal inode[%v] inodeLen [%v] size [%v]",
		qInode.inode.Inode, len(inodeBytes), len(result))
	return result, nil
}

func (qInode *MetaQuotaInode) Unmarshal(raw []byte) error {
	inodeBytes, quotaIds, err := unmarshalQuotaInode(raw)
	if err != nil {
		return err
	}

	log.LogDebugf("MetaQuotaInode Unmarshal inodeLen [%v] size [%v]",
		len(inodeBytes), len(raw))

	qInode.inode = NewInode(0, 0)
	if err := qInode.inode.Unmarshal(inodeBytes); err != nil {
		return err
	}

	qInode.quotaIds = quotaIds
	return nil
}

func (qInode *TxMetaQuotaInode) Marshal() ([]byte, error) {
	inodeBytes, err := qInode.txinode.Marshal()
	if err != nil {
		return nil, err
	}

	result, err := marshalQuotaInode(inodeBytes, qInode.quotaIds)
	if err != nil {
		return nil, err
	}

	log.LogDebugf("TxMetaQuotaInode Marshal inode[%v] inodeLen [%v] size [%v]",
		qInode.txinode.Inode.Inode, len(inodeBytes), len(result))
	return result, nil
}

func (qInode *TxMetaQuotaInode) Unmarshal(raw []byte) error {
	inodeBytes, quotaIds, err := unmarshalQuotaInode(raw)
	if err != nil {
		return err
	}

	log.LogDebugf("TxMetaQuotaInode Unmarshal inodeLen [%v] size [%v]",
		len(inodeBytes), len(raw))

	qInode.txinode = NewTxInode(0, 0, nil)
	if err := qInode.txinode.Unmarshal(inodeBytes); err != nil {
		return err
	}

	qInode.quotaIds = quotaIds
	return nil
}

func (mqMgr *MetaQuotaManager) setQuotaHbInfo(infos []*proto.QuotaHeartBeatInfo) {
	mqMgr.rwlock.Lock()
	defer mqMgr.rwlock.Unlock()

	// Update quota information
	for _, info := range infos {
		if mqMgr.volName != info.VolName {
			continue
		}
		mqMgr.enable = info.Enable
		mqMgr.limitedMap.Store(info.QuotaId, info.LimitedInfo)
		log.LogDebugf("mp[%v] quotaId [%v] limitedInfo [%v]", mqMgr.mpID, info.QuotaId, info.LimitedInfo)
	}

	// Clean up removed quotas
	mqMgr.cleanupRemovedQuotas(infos)
}

// cleanupRemovedQuotas removes quotas that are no longer in the provided infos
func (mqMgr *MetaQuotaManager) cleanupRemovedQuotas(infos []*proto.QuotaHeartBeatInfo) {
	// Create a set of current quota IDs for this volume
	currentQuotaIds := make(map[uint32]bool)
	for _, info := range infos {
		if mqMgr.volName == info.VolName {
			currentQuotaIds[info.QuotaId] = true
		}
	}

	// Remove quotas that are no longer present
	mqMgr.limitedMap.Range(func(key, value interface{}) bool {
		quotaId := key.(uint32)
		if !currentQuotaIds[quotaId] {
			mqMgr.limitedMap.Delete(quotaId)
		}
		return true
	})
}

func (mqMgr *MetaQuotaManager) getQuotaReportInfos() []*proto.QuotaReportInfo {
	mqMgr.rwlock.RLock()
	defer mqMgr.rwlock.RUnlock()

	var infos []*proto.QuotaReportInfo
	mqMgr.statisticBase.Range(func(key, value interface{}) bool {
		quotaId := key.(uint32)
		if _, exists := mqMgr.limitedMap.Load(quotaId); !exists {
			return true
		}

		usedInfo := value.(proto.QuotaUsedInfo)
		reportInfo := &proto.QuotaReportInfo{
			QuotaId:  quotaId,
			UsedInfo: usedInfo,
		}
		infos = append(infos, reportInfo)
		log.LogDebugf("[getQuotaReportInfos] statisticBase mp[%v] key [%v] usedInfo [%v]",
			mqMgr.mpID, quotaId, usedInfo)
		return true
	})

	return infos
}

func (mqMgr *MetaQuotaManager) statisticRebuildStart() bool {
	mqMgr.rwlock.Lock()
	defer mqMgr.rwlock.Unlock()

	if !mqMgr.enable || mqMgr.rbuildbySnapshot {
		return false
	}

	mqMgr.rbuildbySnapshot = true
	return true
}

func (mqMgr *MetaQuotaManager) statisticRebuildFin(rebuild bool) {
	mqMgr.rwlock.Lock()
	defer mqMgr.rwlock.Unlock()

	mqMgr.rbuildbySnapshot = false

	if !rebuild {
		mqMgr.storeRebuildBase = new(sync.Map)
		return
	}

	// Swap the maps
	mqMgr.statisticBase = mqMgr.storeRebuildBase
	mqMgr.storeRebuildBase = new(sync.Map)

	// Log statistics if info logging is enabled
	if log.EnableInfo() {
		mqMgr.logStatistics()
	}
}

// logStatistics logs the current statistics
func (mqMgr *MetaQuotaManager) logStatistics() {
	mqMgr.statisticBase.Range(func(key, value interface{}) bool {
		quotaId := key.(uint32)
		usedInfo := value.(proto.QuotaUsedInfo)
		log.LogInfof("statisticRebuildFin statisticBase mp[%v] quotaId [%v] usedInfo [%v]",
			mqMgr.mpID, quotaId, usedInfo)
		return true
	})
}

func (mqMgr *MetaQuotaManager) IsOverQuota(size bool, files bool, quotaId uint32) uint8 {
	mqMgr.rwlock.RLock()
	defer mqMgr.rwlock.RUnlock()

	if !mqMgr.enable {
		log.LogInfof("IsOverQuota quota [%v] is disable.", quotaId)
		return 0
	}

	value, exists := mqMgr.limitedMap.Load(quotaId)
	if !exists {
		return 0
	}

	limitedInfo := value.(proto.QuotaLimitedInfo)
	var status uint8

	if size && limitedInfo.LimitedBytes {
		status = proto.OpNoSpaceErr
	}

	if files && limitedInfo.LimitedFiles {
		status = proto.OpNoSpaceErr
	}

	log.LogInfof("IsOverQuota quotaId [%v] limitedInfo[%v] status [%v] isFind [%v]",
		quotaId, limitedInfo, status, exists)
	return status
}

func (mqMgr *MetaQuotaManager) EnableQuota() bool {
	return mqMgr.enable
}
