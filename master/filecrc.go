// Copyright 2018 The Containerfs Authors.
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

package master

import (
	"fmt"
	"github.com/tiglabs/containerfs/proto"
	"sort"
	"time"
)

/*use struct define File crc and this crc in all File count*/
type FileCrc struct {
	crc   uint32
	count int
	meta  *FileMetaOnNode
}

func NewFileCrc(volCrc uint32) (fc *FileCrc) {
	fc = new(FileCrc)
	fc.crc = volCrc
	fc.count = 1

	return
}

type FileCrcSorterByCount []*FileCrc

func (fileCrcArr FileCrcSorterByCount) Less(i, j int) bool {
	return fileCrcArr[i].count < fileCrcArr[j].count
}

func (fileCrcArr FileCrcSorterByCount) Swap(i, j int) {
	fileCrcArr[i], fileCrcArr[j] = fileCrcArr[j], fileCrcArr[i]
}

func (fileCrcArr FileCrcSorterByCount) Len() (length int) {
	length = len(fileCrcArr)
	return
}

func (fileCrcArr FileCrcSorterByCount) log() (msg string) {
	for _, fileCrc := range fileCrcArr {
		addr := fileCrc.meta.getLocationAddr()
		count := fileCrc.count
		lastObjId := fileCrc.meta.LastObjID
		needleCnt := fileCrc.meta.NeedleCnt
		crc := fileCrc.crc
		msg = fmt.Sprintf(msg+" addr:%v  count:%v  crc:%v lastObjId:%v needleCnt:%v ", addr, count, crc, lastObjId, needleCnt)

	}

	return
}

func (fc *FileInCore) generateFileCrcTask(partitionID uint64, liveVols []*DataReplica, volType, clusterID string) (tasks []*proto.AdminTask) {
	tasks = make([]*proto.AdminTask, 0)
	if fc.isCheckCrc() == false {
		return
	}

	fms, needRepair := fc.needCrcRepair(liveVols, volType)
	if !needRepair {
		return
	}

	fileCrcArr := fc.calculateCrcCount(fms)
	sort.Sort((FileCrcSorterByCount)(fileCrcArr))
	maxCountFileCrcIndex := len(fileCrcArr) - 1
	if fileCrcArr[maxCountFileCrcIndex].count == 1 {
		msg := fmt.Sprintf("checkFileCrcTaskErr clusterID[%v] partitionID:%v  File:%v  Crc diffrent between all Node  "+
			" it can not repair it ", clusterID, partitionID, fc.Name)
		msg += (FileCrcSorterByCount)(fileCrcArr).log()
		Warn(clusterID, msg)
		return
	}

	for index, crc := range fileCrcArr {
		if index != maxCountFileCrcIndex {
			badNode := crc.meta
			msg := fmt.Sprintf("checkFileCrcTaskErr clusterID[%v] partitionID:%v  File:%v  badCrc On :%v  ",
				clusterID, partitionID, fc.Name, badNode.getLocationAddr())
			msg += (FileCrcSorterByCount)(fileCrcArr).log()
			Warn(clusterID, msg)
		}
	}

	return
}

func (fc *FileInCore) isCheckCrc() bool {
	return time.Now().Unix()-fc.LastModify > DefaultFileDelayCheckCrcSec
}

func (fc *FileInCore) isDelayCheck() bool {
	return time.Now().Unix()-fc.LastModify > DefaultFileDelayCheckLackSec
}

func (fc *FileInCore) needCrcRepair(liveVols []*DataReplica, volType string) (fms []*FileMetaOnNode, needRepair bool) {
	var baseCrc uint32
	fms = make([]*FileMetaOnNode, 0)

	for i := 0; i < len(liveVols); i++ {
		vol := liveVols[i]
		if fm, ok := fc.getFileMetaByAddr(vol); ok {
			fms = append(fms, fm)
		}
	}
	if len(fms) == 0 {
		return
	}

	if !isSameSize(fms) {
		return
	}

	if volType == proto.BlobPartition {
		if !isSameLastObjectID(fms) || !isSameNeedleCnt(fms) {
			return
		}
	}
	baseCrc = fms[0].Crc
	for _, fm := range fms {
		if fm.getFileCrc() != baseCrc {
			needRepair = true
			return
		}
	}

	return
}
func isSameLastObjectID(fms []*FileMetaOnNode) (same bool) {
	sentry := fms[0].LastObjID
	for _, fm := range fms {
		if fm.LastObjID != sentry {
			return
		}
	}
	return true
}

func isSameSize(fms []*FileMetaOnNode) (same bool) {
	sentry := fms[0].Size
	for _, fm := range fms {
		if fm.Size != sentry {
			return
		}
	}
	return true
}

func isSameNeedleCnt(fms []*FileMetaOnNode) (same bool) {
	sentry := fms[0].NeedleCnt
	for _, fm := range fms {
		if fm.NeedleCnt != sentry {
			return
		}
	}
	return true
}

func (fc *FileInCore) calculateCrcCount(badVfNodes []*FileMetaOnNode) (fileCrcArr []*FileCrc) {
	badLen := len(badVfNodes)
	fileCrcArr = make([]*FileCrc, 0)
	for i := 0; i < badLen; i++ {
		crcKey := badVfNodes[i].getFileCrc()
		isFind := false
		var crc *FileCrc
		for _, crc = range fileCrcArr {
			if crc.crc == crcKey {
				isFind = true
				break
			}
		}

		if isFind == false {
			crc = NewFileCrc(crcKey)
			crc.meta = badVfNodes[i]
			fileCrcArr = append(fileCrcArr, crc)
		} else {
			crc.count++
		}
	}

	return
}
