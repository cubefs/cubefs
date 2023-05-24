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
	statisticTemp        *sync.Map // key quotaId, value proto.QuotaUsedInfo
	statisticBase        *sync.Map // key quotaId, value proto.QuotaUsedInfo
	statisticRebuildTemp *sync.Map // key quotaId, value proto.QuotaUsedInfo
	statisticRebuildBase *sync.Map // key quotaId, value proto.QuotaUsedInfo
	limitedMap           *sync.Map
	enable               bool
	rbuilding            bool
	volName              string
	rwlock               sync.RWMutex
	mpID                 uint64
}

type MetaQuotaInode struct {
	inode    *Inode
	quotaIds []uint32
}

type TxMetaQuotaInode struct {
	txinode  *TxInode
	quotaIds []uint32
}

func NewQuotaManager(volName string, mpId uint64) (mqMgr *MetaQuotaManager) {
	mqMgr = &MetaQuotaManager{
		statisticTemp:        new(sync.Map),
		statisticBase:        new(sync.Map),
		statisticRebuildTemp: new(sync.Map),
		statisticRebuildBase: new(sync.Map),
		limitedMap:           new(sync.Map),
		volName:              volName,
		mpID:                 mpId,
	}
	return
}

func (qInode *MetaQuotaInode) Marshal() (result []byte, err error) {
	var (
		inodeBytes []byte
	)
	quotaBytes := bytes.NewBuffer(make([]byte, 0, 128))
	buff := bytes.NewBuffer(make([]byte, 0, 128))
	inodeBytes, err = qInode.inode.Marshal()
	if err != nil {
		return
	}
	inodeLen := uint32(len(inodeBytes))
	if err = binary.Write(buff, binary.BigEndian, inodeLen); err != nil {
		return
	}
	buff.Write(inodeBytes)
	for _, quotaId := range qInode.quotaIds {
		if err = binary.Write(quotaBytes, binary.BigEndian, quotaId); err != nil {
			return
		}
	}
	buff.Write(quotaBytes.Bytes())
	result = buff.Bytes()
	log.LogDebugf("MetaQuotaInode Marshal inode [%v] inodeLen [%v] size [%v]", qInode.inode.Inode, inodeLen, len(result))
	return
}

func (qInode *MetaQuotaInode) Unmarshal(raw []byte) (err error) {
	var inodeLen uint32
	var quotaId uint32
	buff := bytes.NewBuffer(raw)
	if err = binary.Read(buff, binary.BigEndian, &inodeLen); err != nil {
		return
	}
	inodeBytes := make([]byte, inodeLen)
	if _, err = buff.Read(inodeBytes); err != nil {
		return
	}
	log.LogDebugf("MetaQuotaInode Unmarshal inodeLen [%v] size [%v]", inodeBytes, len(raw))
	qInode.inode = NewInode(0, 0)
	if err = qInode.inode.Unmarshal(inodeBytes); err != nil {
		return
	}
	for {
		if buff.Len() == 0 {
			break
		}
		if err = binary.Read(buff, binary.BigEndian, &quotaId); err != nil {
			return
		}
		qInode.quotaIds = append(qInode.quotaIds, quotaId)
	}
	return
}

func (qInode *TxMetaQuotaInode) Marshal() (result []byte, err error) {
	var (
		inodeBytes []byte
	)
	quotaBytes := bytes.NewBuffer(make([]byte, 0, 128))
	buff := bytes.NewBuffer(make([]byte, 0, 128))
	inodeBytes, err = qInode.txinode.Marshal()
	if err != nil {
		return
	}
	inodeLen := uint32(len(inodeBytes))
	if err = binary.Write(buff, binary.BigEndian, inodeLen); err != nil {
		return
	}
	buff.Write(inodeBytes)
	for _, quotaId := range qInode.quotaIds {
		if err = binary.Write(quotaBytes, binary.BigEndian, quotaId); err != nil {
			return
		}
	}
	buff.Write(quotaBytes.Bytes())
	result = buff.Bytes()
	log.LogDebugf("TxMetaQuotaInode Marshal inode [%v] inodeLen [%v] size [%v]", qInode.txinode.Inode.Inode, inodeLen, len(result))
	return
}

func (qInode *TxMetaQuotaInode) Unmarshal(raw []byte) (err error) {
	var inodeLen uint32
	var quotaId uint32
	buff := bytes.NewBuffer(raw)
	if err = binary.Read(buff, binary.BigEndian, &inodeLen); err != nil {
		return
	}
	inodeBytes := make([]byte, inodeLen)
	if _, err = buff.Read(inodeBytes); err != nil {
		return
	}
	log.LogDebugf("TxMetaQuotaInode Unmarshal inodeLen [%v] size [%v]", inodeBytes, len(raw))
	qInode.txinode = NewTxInode("", 0, 0, 0, nil)
	if err = qInode.txinode.Unmarshal(inodeBytes); err != nil {
		return
	}
	for {
		if buff.Len() == 0 {
			break
		}
		if err = binary.Read(buff, binary.BigEndian, &quotaId); err != nil {
			return
		}
		qInode.quotaIds = append(qInode.quotaIds, quotaId)
	}
	return
}

func (mqMgr *MetaQuotaManager) setQuotaHbInfo(infos []*proto.QuotaHeartBeatInfo) {
	mqMgr.rwlock.Lock()
	defer mqMgr.rwlock.Unlock()

	for _, info := range infos {
		if mqMgr.volName != info.VolName {
			continue
		}
		mqMgr.limitedMap.Store(info.QuotaId, info.LimitedInfo)
		log.LogDebugf("mp [%v] quotaId [%v] limitedInfo [%v]", mqMgr.mpID, info.QuotaId, info.LimitedInfo)
	}
	return
}

func (mqMgr *MetaQuotaManager) getQuotaReportInfos() (infos []*proto.QuotaReportInfo) {
	mqMgr.rwlock.Lock()
	defer mqMgr.rwlock.Unlock()
	var usedInfo proto.QuotaUsedInfo
	mqMgr.statisticTemp.Range(func(key, value interface{}) bool {
		usedInfo = value.(proto.QuotaUsedInfo)
		if value, isFind := mqMgr.statisticBase.Load(key.(uint32)); isFind {
			baseInfo := value.(proto.QuotaUsedInfo)
			usedInfo.Add(&baseInfo)
		}
		mqMgr.statisticBase.Store(key.(uint32), usedInfo)
		log.LogDebugf("[getQuotaReportInfos] statisticTemp mp [%v] key [%v] usedInfo [%v]", mqMgr.mpID, key.(uint32), usedInfo)
		return true
	})
	mqMgr.statisticTemp = new(sync.Map)
	mqMgr.statisticBase.Range(func(key, value interface{}) bool {
		usedInfo = value.(proto.QuotaUsedInfo)
		reportInfo := &proto.QuotaReportInfo{
			QuotaId:  key.(uint32),
			UsedInfo: usedInfo,
		}
		infos = append(infos, reportInfo)
		log.LogDebugf("[getQuotaReportInfos] statisticTemp mp [%v] key [%v] usedInfo [%v]", mqMgr.mpID, key.(uint32), usedInfo)
		return true
	})
	log.LogInfof("[getQuotaReportInfos] end infos [%v]", infos)
	return
}

func (mqMgr *MetaQuotaManager) statisticRebuildStart() {
	mqMgr.rwlock.Lock()
	defer mqMgr.rwlock.Unlock()
	mqMgr.rbuilding = true
}

func (mqMgr *MetaQuotaManager) statisticRebuildFin() {
	mqMgr.rwlock.Lock()
	defer mqMgr.rwlock.Unlock()
	mqMgr.statisticBase = mqMgr.statisticRebuildBase
	mqMgr.statisticTemp = mqMgr.statisticRebuildTemp
	mqMgr.statisticRebuildBase = new(sync.Map)
	mqMgr.statisticRebuildTemp = new(sync.Map)
	mqMgr.rbuilding = false
}

func (mqMgr *MetaQuotaManager) IsOverQuota(size bool, files bool, quotaId uint32) (status uint8) {
	var limitedInfo proto.QuotaLimitedInfo
	mqMgr.rwlock.RLock()
	defer mqMgr.rwlock.RUnlock()
	value, isFind := mqMgr.limitedMap.Load(quotaId)
	if isFind {
		limitedInfo = value.(proto.QuotaLimitedInfo)
		if size && limitedInfo.LimitedBytes {
			status = proto.OpNoSpaceErr
		}

		if files && limitedInfo.LimitedFiles {
			status = proto.OpNoSpaceErr
		}
	}
	log.LogInfof("IsOverQuota quotaId [%v] limitedInfo[%v] status [%v] isFind [%v]", quotaId, limitedInfo, status, isFind)
	return
}

func (mqMgr *MetaQuotaManager) updateUsedInfo(size int64, files int64, quotaId uint32) {
	var baseInfo proto.QuotaUsedInfo
	mqMgr.rwlock.Lock()
	defer mqMgr.rwlock.Unlock()

	value, isFind := mqMgr.statisticTemp.Load(quotaId)
	if isFind {
		baseInfo = value.(proto.QuotaUsedInfo)
	}
	baseInfo.UsedBytes += size
	baseInfo.UsedFiles += files
	mqMgr.statisticTemp.Store(quotaId, baseInfo)
	if mqMgr.rbuilding {
		value, isFind = mqMgr.statisticRebuildTemp.Load(quotaId)
		if isFind {
			baseInfo = value.(proto.QuotaUsedInfo)
		} else {
			baseInfo.UsedBytes = 0
			baseInfo.UsedFiles = 0
		}
		baseInfo.UsedBytes += size
		baseInfo.UsedFiles += files
		mqMgr.statisticRebuildTemp.Store(quotaId, baseInfo)
	}
	log.LogDebugf("updateUsedInfo baseInfo [%v]", baseInfo)
	return
}
