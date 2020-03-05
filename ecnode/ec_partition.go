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

package ecnode

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path"
	"strings"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/storage"
	"github.com/chubaofs/chubaofs/util/log"
)

const (
	EcPartitionPrefix           = "ecpartition"
	EcPartitionMetaDataFileName = "META"
	TempMetaDataFileName        = ".meta"
	TimeLayout                  = "2006-01-02 15:04:05"

	IntervalToUpdatePartitionSize = 60 // interval to update the partition size
)

type EcPartition struct {
	clusterID string

	partitionID   uint64
	partitionSize int
	volumeID      string
	dataNodeNum   int
	parityNodeNum int
	nodeIndex     int
	dataNodes     []string
	parityNodes   []string

	partitionStatus int

	disk        *Disk
	path        string
	used        int
	extentStore *storage.ExtentStore
	storeC      chan uint64
	stopC       chan bool

	intervalToUpdatePartitionSize int64
	loadExtentHeaderStatus        int

	config *EcPartitionCfg
}

type EcPartitionCfg struct {
	VolName       string `json:"vol_name"`
	ClusterID     string `json:"cluster_id"`
	PartitionID   uint64 `json:"partition_id"`
	PartitionSize int    `json:"partition_size"`

	DataNodeNum   int      `json:"data_node_num"`
	ParityNodeNum int      `json:"parity_node_num"`
	NodeIndex     int      `json:node_index`
	DataNodes     []string `json:data_nodes`
	ParityNodes   []string `json:parity_nodes`
}

type EcPartitionMetaData struct {
	PartitionID   uint64
	PartitionSize int
	VolumeID      string
	DataNodeNum   int
	ParityNodeNum int
	NodeIndex     int
	DataNodes     []string
	ParityNodes   []string

	CreateTime string
}

// Disk returns the disk instance.
func (ep *EcPartition) Disk() *Disk {
	return ep.disk
}

func (ep *EcPartition) IsRejectWrite() bool {
	return ep.Disk().RejectWrite
}

// Status returns the partition status.
func (ep *EcPartition) Status() int {
	return ep.partitionStatus
}

// Size returns the partition size.
func (ep *EcPartition) Size() int {
	return ep.partitionSize
}

// Used returns the used space.
func (ep *EcPartition) Used() int {
	return ep.used
}

// Available returns the available space.
func (ep *EcPartition) Available() int {
	return ep.partitionSize - ep.used
}

func (ep *EcPartition) GetExtentCount() int {
	return ep.extentStore.GetExtentCount()
}

func (ep *EcPartition) Path() string {
	return ep.path
}

func (ep *EcPartition) DataNodeNum() int {
	return ep.dataNodeNum
}

func (ep *EcPartition) ParityNodeNum() int {
	return ep.parityNodeNum
}

func (ep *EcPartition) NodeIndex() int {
	return ep.nodeIndex
}

func (ep *EcPartition) DataNodes() []string {
	return ep.dataNodes
}

func (ep *EcPartition) ParityNodes() []string {
	return ep.parityNodes
}

func (dp *EcPartition) computeUsage() {
	var (
		used  int64
		files []os.FileInfo
		err   error
	)
	if time.Now().Unix()-dp.intervalToUpdatePartitionSize < IntervalToUpdatePartitionSize {
		return
	}
	if files, err = ioutil.ReadDir(dp.path); err != nil {
		return
	}
	for _, file := range files {
		isExtent := storage.RegexpExtentFile.MatchString(file.Name())
		if !isExtent {
			continue
		}
		used += file.Size()
	}
	dp.used = int(used)
	dp.intervalToUpdatePartitionSize = time.Now().Unix()
}

func (ep *EcPartition) statusUpdate() {
	status := proto.ReadWrite
	ep.computeUsage()

	if ep.used >= ep.partitionSize {
		status = proto.ReadOnly
	}
	if ep.extentStore.GetExtentCount() >= storage.MaxExtentCount {
		status = proto.ReadOnly
	}
	if ep.Status() == proto.Unavailable {
		status = proto.Unavailable
	}

	ep.partitionStatus = int(math.Min(float64(status), float64(ep.disk.Status)))
}

func (ep *EcPartition) statusUpdateScheduler() {
	ticker := time.NewTicker(time.Minute)
	for {
		select {
		case <-ticker.C:
			ep.statusUpdate()
		case <-ep.stopC:
			ticker.Stop()
			return
		}
	}
}

// PersistMetaData persists the file metadata on the disk
func (ep EcPartition) PersistMetaData() (err error) {
	fileName := path.Join(ep.Path(), TempMetaDataFileName)
	metadataFile, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return
	}
	defer func() {
		metadataFile.Sync()
		metadataFile.Close()
		os.Remove(fileName)
	}()

	md := &EcPartitionMetaData{
		PartitionID:   ep.config.PartitionID,
		PartitionSize: ep.config.PartitionSize,
		VolumeID:      ep.config.VolName,
		DataNodeNum:   ep.config.DataNodeNum,
		ParityNodeNum: ep.config.ParityNodeNum,
		NodeIndex:     ep.config.NodeIndex,
		DataNodes:     ep.config.DataNodes,
		ParityNodes:   ep.config.ParityNodes,

		CreateTime: time.Now().Format(TimeLayout),
	}
	metadata, err := json.Marshal(md)
	if err != nil {
		return
	}

	_, err = metadataFile.Write(metadata)
	if err != nil {
		return
	}
	log.LogInfof("PersistMetaData EcPartition(%v) data(%v)", ep.partitionID, string(metadata))
	err = os.Rename(fileName, path.Join(ep.Path(), EcPartitionMetaDataFileName))
	return
}

// newEcPartition
func newEcPartition(epCfg *EcPartitionCfg, disk *Disk) (ep *EcPartition, err error) {
	partitionID := epCfg.PartitionID
	dataPath := path.Join(disk.Path, fmt.Sprintf(EcPartitionPrefix+"_%v_%v", partitionID, epCfg.PartitionSize))
	partition := &EcPartition{
		clusterID: epCfg.ClusterID,

		partitionID:   epCfg.PartitionID,
		partitionSize: epCfg.PartitionSize,
		volumeID:      epCfg.VolName,
		dataNodeNum:   epCfg.DataNodeNum,
		parityNodeNum: epCfg.ParityNodeNum,
		nodeIndex:     epCfg.NodeIndex,
		dataNodes:     epCfg.DataNodes,
		parityNodes:   epCfg.ParityNodes,

		disk:            disk,
		path:            dataPath,
		stopC:           make(chan bool, 0),
		storeC:          make(chan uint64, 128),
		partitionStatus: proto.ReadWrite,
		config:          epCfg,
	}

	partition.extentStore, err = storage.NewExtentStore(partition.path, epCfg.PartitionID, epCfg.PartitionSize)
	if err != nil {
		return
	}

	disk.AttachEcPartition(partition)
	ep = partition
	go partition.statusUpdateScheduler()
	return
}

// LoadEcPartition load partition from the specified directory when ecnode start
func LoadEcPartition(partitionDir string, disk *Disk) (ep *EcPartition, err error) {
	metaDataRaw, err := ioutil.ReadFile(path.Join(partitionDir, EcPartitionMetaDataFileName))
	if err != nil {
		return
	}

	metaData := &EcPartitionMetaData{}
	err = json.Unmarshal(metaDataRaw, metaData)
	if err != nil {
		return
	}

	volumeID := strings.TrimSpace(metaData.VolumeID)
	if len(volumeID) == 0 || metaData.PartitionID == 0 || metaData.PartitionSize == 0 {
		return
	}

	epCfg := &EcPartitionCfg{
		VolName:       metaData.VolumeID,
		ClusterID:     disk.space.GetClusterID(),
		PartitionID:   metaData.PartitionID,
		PartitionSize: metaData.PartitionSize,

		DataNodeNum:   metaData.DataNodeNum,
		ParityNodeNum: metaData.ParityNodeNum,
		NodeIndex:     metaData.NodeIndex,
		DataNodes:     metaData.DataNodes,
		ParityNodes:   metaData.ParityNodes,
	}

	ep, err = newEcPartition(epCfg, disk)
	if err != nil {
		return
	}

	disk.space.AttachPartition(ep)
	disk.AddSize(uint64(ep.Size()))
	return
}

// CreateEcPartition create ec partition and return its instance
func CreateEcPartition(epCfg *EcPartitionCfg, disk *Disk, request *proto.CreateEcPartitionRequest) (ep *EcPartition, err error) {
	ep, err = newEcPartition(epCfg, disk)
	if err != nil {
		return
	}

	err = ep.PersistMetaData()
	if err != nil {
		return
	}

	disk.AddSize(uint64(ep.Size()))
	return
}
