// Copyright 2018 The Chubao Authors.
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
	"github.com/chubaofs/cfs/proto"
	"sort"
	"time"
)

// FileCrc defines the crc of a file
type FileCrc struct {
	crc   uint32
	count int
	meta  *FileMetadata
}

func newFileCrc(volCrc uint32) (fc *FileCrc) {
	fc = new(FileCrc)
	fc.crc = volCrc
	fc.count = 1

	return
}

type fileCrcSorter []*FileCrc

func (fileCrcArr fileCrcSorter) Less(i, j int) bool {
	return fileCrcArr[i].count < fileCrcArr[j].count
}

func (fileCrcArr fileCrcSorter) Swap(i, j int) {
	fileCrcArr[i], fileCrcArr[j] = fileCrcArr[j], fileCrcArr[i]
}

func (fileCrcArr fileCrcSorter) Len() (length int) {
	length = len(fileCrcArr)
	return
}

func (fileCrcArr fileCrcSorter) log() (msg string) {
	for _, fileCrc := range fileCrcArr {
		addr := fileCrc.meta.getLocationAddr()
		count := fileCrc.count
		crc := fileCrc.crc
		msg = fmt.Sprintf(msg+" addr:%v  count:%v  crc:%v ", addr, count, crc)

	}

	return
}

func (fc *FileInCore) createFileCrcTask(partitionID uint64, liveVols []*DataReplica, clusterID string) (tasks []*proto.AdminTask) {
	tasks = make([]*proto.AdminTask, 0)
	if fc.shouldCheckCrc() == false {
		return
	}

	fms, needRepair := fc.needCrcRepair(liveVols)

	if len(fms) < len(liveVols) && (time.Now().Unix()-fc.LastModify) > intervalToCheckMissingReplica {
		liveAddrs := make([]string, 0)
		for _, replica := range liveVols {
			liveAddrs = append(liveAddrs, replica.Addr)
		}
		Warn(clusterID, fmt.Sprintf("partitionid[%v],file[%v],fms[%v],liveAddr[%v]", partitionID, fc.Name, fc.getFileMetaAddrs(), liveAddrs))
	}
	if !needRepair {
		return
	}

	fileCrcArr := fc.calculateCrc(fms)
	sort.Sort(fileCrcSorter(fileCrcArr))
	maxIndex := len(fileCrcArr) - 1
	if fileCrcArr[maxIndex].count == 1 {
		msg := fmt.Sprintf("checkFileCrcTaskErr clusterID[%v] partitionID:%v  File:%v  CRC diffrent between all Node  "+
			" it can not repair it ", clusterID, partitionID, fc.Name)
		msg += (fileCrcSorter(fileCrcArr)).log()
		Warn(clusterID, msg)
		return
	}

	for index, crc := range fileCrcArr {
		if index != maxIndex {
			badNode := crc.meta
			msg := fmt.Sprintf("checkFileCrcTaskErr clusterID[%v] partitionID:%v  File:%v  badCrc On :%v  ",
				clusterID, partitionID, fc.Name, badNode.getLocationAddr())
			msg += (fileCrcSorter)(fileCrcArr).log()
			Warn(clusterID, msg)
		}
	}

	return
}

func (fc *FileInCore) shouldCheckCrc() bool {
	return time.Now().Unix()-fc.LastModify > defaultIntervalToCheckCrc
}

func (fc *FileInCore) needCrcRepair(liveVols []*DataReplica) (fms []*FileMetadata, needRepair bool) {
	var baseCrc uint32
	fms = make([]*FileMetadata, 0)

	for i := 0; i < len(liveVols); i++ {
		vol := liveVols[i]
		if fm, ok := fc.getFileMetaByAddr(vol); ok {
			fms = append(fms, fm)
		}
	}
	if len(fms) == 0 {
		return
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

func hasSameSize(fms []*FileMetadata) (same bool) {
	sentry := fms[0].Size
	for _, fm := range fms {
		if fm.Size != sentry {
			return
		}
	}
	return true
}

func (fc *FileInCore) calculateCrc(badVfNodes []*FileMetadata) (fileCrcArr []*FileCrc) {
	badLen := len(badVfNodes)
	fileCrcArr = make([]*FileCrc, 0)
	for i := 0; i < badLen; i++ {
		crcKey := badVfNodes[i].getFileCrc()
		isFound := false
		var crc *FileCrc
		for _, crc = range fileCrcArr {
			if crc.crc == crcKey {
				isFound = true
				break
			}
		}

		if isFound == false {
			crc = newFileCrc(crcKey)
			crc.meta = badVfNodes[i]
			fileCrcArr = append(fileCrcArr, crc)
		} else {
			crc.count++
		}
	}

	return
}
