// Copyright 2018 The Cubefs Authors.
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
	"sort"
	"strconv"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/storage"
	"github.com/cubefs/cubefs/util/log"
)

// Recover a file if it has bad CRC or it has been timed out before.
func (partition *DataPartition) validateCRC(clusterID string) {
	partition.Lock()
	defer partition.Unlock()
	liveReplicas := partition.liveReplicas(defaultDataPartitionTimeOutSec)
	if len(liveReplicas) == 0 {
		return
	}

	if len(liveReplicas) < int(partition.ReplicaNum) {
		liveAddrs := make([]string, 0)
		for _, replica := range liveReplicas {
			liveAddrs = append(liveAddrs, replica.Addr)
		}
		inactiveAddrs := make([]string, 0)
		for _, host := range partition.Hosts {
			if !contains(liveAddrs, host) {
				inactiveAddrs = append(inactiveAddrs, host)
			}
		}
		Warn(clusterID, fmt.Sprintf("vol[%v],dpId[%v],liveAddrs[%v],inactiveAddrs[%v]", partition.VolName, partition.PartitionID, liveAddrs, inactiveAddrs))
	}
	partition.doValidateCRC(liveReplicas, clusterID)
	return
}

func (partition *DataPartition) doValidateCRC(liveReplicas []*DataReplica, clusterID string) {
	if !proto.IsNormalDp(partition.PartitionType) {
		return
	}

	for _, fc := range partition.FileInCoreMap {
		extentID, err := strconv.ParseUint(fc.Name, 10, 64)
		if err != nil {
			continue
		}
		if storage.IsTinyExtent(extentID) {
			partition.checkTinyExtentFile(fc, liveReplicas, clusterID)
		} else {
			partition.checkExtentFile(fc, liveReplicas, clusterID)
		}
	}
}

func (partition *DataPartition) checkTinyExtentFile(fc *FileInCore, liveReplicas []*DataReplica, clusterID string) {
	if fc.shouldCheckCrc() == false {
		return
	}
	fms, needRepair := fc.needCrcRepair(liveReplicas)
	if !needRepair {
		return
	}
	if !hasSameSize(fms) {
		msg := fmt.Sprintf("CheckFileError size not match,cluster[%v],dpID[%v],", clusterID, partition.PartitionID)
		for _, fm := range fms {
			msg = msg + fmt.Sprintf("fm[%v]:size[%v]\n", fm.locIndex, fm.Size)
		}
		log.LogWarn(msg)
		return
	}
	msg := fmt.Sprintf("CheckFileError crc not match,cluster[%v],dpID[%v]", clusterID, partition.PartitionID)
	for _, fm := range fms {
		msg = msg + fmt.Sprintf("fm[%v]:%v\n", fm.locIndex, fm)
	}
	Warn(clusterID, msg)
	return
}

func (partition *DataPartition) checkExtentFile(fc *FileInCore, liveReplicas []*DataReplica, clusterID string) {
	if fc.shouldCheckCrc() == false {
		return
	}

	fms, needRepair := fc.needCrcRepair(liveReplicas)

	if len(fms) < len(liveReplicas) && (time.Now().Unix()-fc.LastModify) > intervalToCheckMissingReplica {
		lastReportTime, ok := partition.FilesWithMissingReplica[fc.Name]
		if len(partition.FilesWithMissingReplica) > 400 {
			Warn(clusterID, fmt.Sprintf("partitionid[%v] has [%v] files missed replica", partition.PartitionID, len(partition.FilesWithMissingReplica)))
			return
		}

		if !ok {
			partition.FilesWithMissingReplica[fc.Name] = time.Now().Unix()
			return
		}
		if time.Now().Unix()-lastReportTime < intervalToCheckMissingReplica {
			return
		}

		liveAddrs := make([]string, 0)
		for _, replica := range liveReplicas {
			liveAddrs = append(liveAddrs, replica.Addr)
		}
		Warn(clusterID, fmt.Sprintf("partitionid[%v],file[%v],fms[%v],liveAddr[%v]", partition.PartitionID, fc.Name, fc.getFileMetaAddrs(), liveAddrs))
	}
	if !needRepair {
		return
	}

	fileCrcArr := fc.calculateCrc(fms)
	sort.Sort((fileCrcSorter)(fileCrcArr))
	maxCountFileCrcIndex := len(fileCrcArr) - 1
	if fileCrcArr[maxCountFileCrcIndex].count == 1 {
		msg := fmt.Sprintf("checkFileCrcTaskErr clusterID[%v] partitionID:%v  File:%v  ExtentOffset diffrent between all Node  "+
			" it can not repair it ", clusterID, partition.PartitionID, fc.Name)
		msg += (fileCrcSorter)(fileCrcArr).log()
		Warn(clusterID, msg)
		return
	}

	for index, crc := range fileCrcArr {
		if index != maxCountFileCrcIndex {
			badNode := crc.meta
			msg := fmt.Sprintf("checkFileCrcTaskErr clusterID[%v] partitionID:%v  File:%v  badCrc On :%v  ",
				clusterID, partition.PartitionID, fc.Name, badNode.getLocationAddr())
			msg += (fileCrcSorter)(fileCrcArr).log()
			Warn(clusterID, msg)
		}
	}
	return
}
