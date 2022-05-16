// Copyright 2020 The Chubao Authors.
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

package proto

import (
	"math"
)

type IssueMigrationTaskRequest struct {
	VolName         string
	PartitionId     uint64 // data partition id
	CurrentExtentID uint64
	ProfHosts       []string
	EcDataNum       uint8
	EcParityNum     uint8
	Hosts           []string
	EcMaxUnitSize   uint64
}

type TinyDelInfo struct {
	ExtentId      uint64
	Offset        uint64
	Size          uint64
	DeleteStatus  uint32
	HostIndex     uint32
	NeedUpdateParity bool
}

type ExtentCrcResponse struct {
	CRC        uint32
	FinishRead bool
}

const (
	NormalRead  = iota
	DegradeRead //don't need writeBack
	DataRepair
	ScrubRepair
	DeleteRepair
)

const (
	PageSize = 4 * 1024
	EcWriteDeadlineTime            = 10
	EcReadDeadlineTime             = 10
)

func CalStripeUnitSize(extentSize, maxStripeUnitSize, ecDataNum uint64) (size uint64) {
	if extentSize >= (maxStripeUnitSize * ecDataNum) {
		size = maxStripeUnitSize
	} else {
		decimalUintSize := float64(extentSize) / float64(ecDataNum)
		/*n := math.Ceil(math.Log10(decimalUintSize) / math.Log10(float64(2)))
		size = 1 << uint64(n)*/
		size = uint64(math.Ceil(decimalUintSize))
		if size%PageSize != 0 {
			size = (size/PageSize + 1) * PageSize
		}
		if size > maxStripeUnitSize {
			size = maxStripeUnitSize
		}
	}
	return
}

func GetEcHostsByExtentId(nodeNum, extentId uint64, ecHosts []string) (hosts []string) {
	hosts = make([]string, nodeNum)
	nodeIndex := extentId % nodeNum
	for i := 0; i < int(nodeNum); i++ {
		if nodeIndex >= nodeNum {
			nodeIndex = 0
		}

		hosts[i] = ecHosts[nodeIndex]
		nodeIndex = nodeIndex + 1
	}
	return
}

func IsEcParityNode(hosts []string, localHost string, extentId, dataNum, parityNum uint64) bool {
	nodeNum := dataNum + parityNum
	newHosts := GetEcHostsByExtentId(nodeNum, extentId, hosts)
	for i := dataNum; i < nodeNum; i++ {
		if newHosts[i] == localHost {
			return true
		}
	}
	return false
}

func GetEcNodeIndex(nodeAddr string, hosts []string) (nodeIndex int, exist bool) {
	for i, host := range hosts {
		if host == nodeAddr {
			nodeIndex = i
			exist = true
			break
		}
	}
	return
}

func IsEcStatus(status uint8) bool {
	if status == Migrating || status == FinishEC || status == OnlyEcExist || status == MigrateFailed{
		return true
	}
	return false
}

func IsEcFinished(status uint8) bool {
	if status == FinishEC || status == OnlyEcExist {
		return true
	}
	return false
}
