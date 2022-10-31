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
	"context"
	"encoding/json"
	"fmt"
	"github.com/chubaofs/chubaofs/util/exporter"
	"hash/crc32"
	"io"
	"io/ioutil"
	"math"
	"net"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chubaofs/chubaofs/repl"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/unit"

	"github.com/chubaofs/chubaofs/ecstorage"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/log"
)

const (
	EcPartitionPrefix              = "ecpartition"
	EcPartitionMetaDataFileName    = "META"
	OriginTinyExtentDeleteFileName = "ORIGIN_TINYEXTENT_DELETE"
	TempMetaDataFileName           = ".meta"
	TimeLayout                     = "2006-01-02 15:04:05"
	IntervalToUpdatePartitionSize  = 60 // interval to update the partition size
)

type scrubChanInfo struct {
	extentId          uint64
	isScrubFailHandle bool
}

type repairHostInfo struct {
	nodeAddr           string
	offset             uint64
	stripeUnitFileSize uint64
}

type repairExtentInfo struct {
	needRepairHosts        []*repairHostInfo
	needRepairSize         uint64
	repairStripeUnitOffset uint64
	originExtentSize       uint64
	tinyDelNodeIndex       uint32
}

type holesInfo struct {
	OriginTinyDeleteCount int64 `json:"originTinyDeleteCount"`
	TinyDeleteCount       int64 `json:"tinyDeleteCount"`
}

type createExtentInfo struct {
	hosts            []string
	originExtentSize uint64
}

type repairTinyDelInfo struct {
	needRepairHosts []string
	normalHost      string
	maxTinyDelCount int64
}

type tinyFileRepairData struct {
	MaxTinyDelCount int64  `json:"maxTinyDelCount"`
	NormalHost      string `json:"normalHost"`
}

type EcPartition struct {
	EcPartitionMetaData

	partitionStatus               int
	disk                          *Disk
	ecNode                        *EcNode
	path                          string
	used                          uint64
	extentStore                   *ecstorage.ExtentStore
	storeC                        chan uint64
	stopC                         chan bool
	repairC                       chan struct{}
	originTinyExtentDeleteMap     map[uint64][]*proto.TinyExtentHole
	originTinyExtentDeleteMapLock sync.RWMutex
	originExtentSizeMap           sync.Map
	replicasLock                  sync.RWMutex
	intervalToUpdatePartitionSize int64
	intervalToUpdateReplicas      int64
	loadExtentHeaderStatus        int
	metaFileLock                  sync.Mutex
	failScrubMapLock              sync.Mutex
	isRecover                     bool
	checkChunkData                bool
	ecMigrateStatus               uint8
}

type EcTinyBlockStatus struct {
	status                uint64
	delUpdateSuccessHosts []string
}

type EcTinyDeleteRecordArr []ecstorage.EcTinyDeleteRecord

type EcPartitionMetaData struct {
	PartitionID      uint64            `json:"partition_id"`
	PartitionSize    uint64            `json:"partition_size"`
	VolumeID         string            `json:"vol_name"`
	EcMaxUnitSize    uint64            `json:"max_unit_size"`
	EcDataNum        uint32            `json:"data_node_num"`
	EcParityNum      uint32            `json:"parity_node_num"`
	NodeIndex        uint32            `json:"node_index"`
	Hosts            []string          `json:"hosts"`
	CreateTime       string            `json:"create_time"`
	MaxScrubDoneId   uint64            `json:"maxScrubDoneId"`
	ScrubStartTime   int64             `json:"scrubStartTime"`
	FailScrubExtents map[uint64]uint64 `json:"FailScrubExtents"` //key: extentId value: extentId
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
func (ep *EcPartition) Size() uint64 {
	return ep.PartitionSize
}

// Used returns the used space.
func (ep *EcPartition) Used() uint64 {
	return ep.used
}

// Available returns the available space.
func (ep *EcPartition) Available() uint64 {
	return ep.PartitionSize - ep.used
}

func (ep *EcPartition) Path() string {
	return ep.path
}

func (ep *EcPartition) ExtentStore() *ecstorage.ExtentStore {
	return ep.extentStore
}

func (ep *EcPartition) checkIsDiskError(err error) (diskError bool) {
	if err == nil {
		return
	}

	if IsDiskErr(err.Error()) {
		mesg := fmt.Sprintf("disk path %v error on %v", ep.Path(), localIP)
		log.LogErrorf(mesg)
		exporter.Warning(mesg)
		ep.disk.incReadErrCnt()
		ep.disk.incWriteErrCnt()
		ep.disk.Status = proto.Unavailable
		ep.statusUpdate()
		diskError = true
	}
	return
}

func (ep *EcPartition) checkIsEofError(err error, es *ecStripe, originExtentSize, extentOffset, readSize uint64) (eofError bool) {
	eofError = true
	if err == io.EOF {
		stripeUnitFileSize, errN := es.calcStripeUnitFileSize(originExtentSize, es.localServerAddr)
		if errN != nil {
			return
		}
		if stripeUnitFileSize <= extentOffset+readSize {
			eofError = false
		}
	}
	return
}

func (ep *EcPartition) computeUsage(forceUpdate bool) {
	var (
		used  int64
		files []os.FileInfo
		err   error
	)
	if time.Now().Unix()-ep.intervalToUpdatePartitionSize < IntervalToUpdatePartitionSize && !forceUpdate {
		return
	}
	if files, err = ioutil.ReadDir(ep.path); err != nil {
		return
	}
	for _, file := range files {
		isExtent := ecstorage.RegexpExtentFile.MatchString(file.Name())
		if !isExtent {
			continue
		}
		used += file.Size()
	}
	ep.used = uint64(used)
	ep.intervalToUpdatePartitionSize = time.Now().Unix()
}

func (ep *EcPartition) statusUpdate() {
	status := proto.ReadWrite
	ep.computeUsage(false)

	if ep.used >= ep.PartitionSize {
		status = proto.ReadOnly
	}
	if ep.extentStore.GetExtentCount() >= ecstorage.MaxExtentCount {
		status = proto.ReadOnly
	}
	if ep.Status() == proto.Unavailable {
		status = proto.Unavailable
	}

	if proto.IsEcFinished(ep.ecMigrateStatus) {
		status = proto.ReadOnly
	}

	ep.partitionStatus = int(math.Min(float64(status), float64(ep.disk.Status)))
}

// PersistMetaData persists the file metadata on the disk
func (ep *EcPartition) PersistMetaData() (err error) {
	ep.metaFileLock.Lock()
	defer ep.metaFileLock.Unlock()

	tempFileFilePath := path.Join(ep.Path(), TempMetaDataFileName)
	metadataFile, err := os.OpenFile(tempFileFilePath, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return
	}

	defer func() {
		if err != nil {
			_ = os.Remove(tempFileFilePath)
		}
	}()

	metadata, err := json.Marshal(ep.EcPartitionMetaData)
	if err != nil {
		return
	}

	_, err = metadataFile.Write(metadata)
	if err != nil {
		return
	}

	err = metadataFile.Sync()
	if err != nil {
		return
	}

	err = metadataFile.Close()
	if err != nil {
		return
	}

	log.LogDebugf("PersistMetaData EcPartition(%v) data(%v)", ep.PartitionID, string(metadata))
	err = os.Rename(tempFileFilePath, path.Join(ep.Path(), EcPartitionMetaDataFileName))
	return
}

// newEcPartition
func newEcPartition(epMetaData *EcPartitionMetaData, disk *Disk) (ep *EcPartition, err error) {
	partitionID := epMetaData.PartitionID
	dataPath := path.Join(disk.Path, fmt.Sprintf(EcPartitionPrefix+"_%v_%v", partitionID, epMetaData.PartitionSize))
	partition := &EcPartition{
		EcPartitionMetaData: *epMetaData,
		disk:                disk,
		path:                dataPath,
		stopC:               make(chan bool, 0),
		storeC:              make(chan uint64, 128),
		repairC:             make(chan struct{}, 1),
		partitionStatus:     proto.ReadWrite,
	}

	var cacheListener ecstorage.CacheListener = func(event ecstorage.CacheEvent, e *ecstorage.Extent) {
		switch event {
		case ecstorage.CacheEvent_Add:
			disk.IncreaseFDCount()
		case ecstorage.CacheEvent_Evict:
			disk.DecreaseFDCount()
		}
	}
	log.LogDebugf("partition(%v) partitionSize(%v) dataPath(%v)\n", partition.PartitionID, partition.PartitionSize, partition.path)
	partition.extentStore, err = ecstorage.NewEcExtentStore(partition.path, partition.PartitionID, int(partition.PartitionSize), CacheCapacityPerPartition, cacheListener)
	if err != nil {
		return
	}
	ecDb := partition.disk.ecDb
	partition.originTinyExtentDeleteMap = make(map[uint64][]*proto.TinyExtentHole, 0)
	partition.extentStore.SetEcDb(ecDb)
	partition.ecNode = disk.space.ecNode
	disk.AttachEcPartition(partition)
	ep = partition

	go partition.repairAndTinyDelScheduler()
	return
}

func (ep *EcPartition) loadPersistMap() (err error) {
	if err = ep.loadPartitionPersistMap(ecstorage.OriginExtentSizeMapTable); err != nil {
		return
	}

	err = ep.loadPartitionPersistMap(ecstorage.OriginTinyDeleteMapTable)
	return
}

func (ep *EcPartition) loadPartitionPersistMap(mapTable uint64) (err error) {
	var (
		extentSize uint64
	)
	store := ep.extentStore
	err = store.PersistMapRange(mapTable, func(extentId uint64, data []byte) (err error) {
		if mapTable == ecstorage.OriginExtentSizeMapTable {
			err = json.Unmarshal(data, &extentSize)
			if err != nil {
				log.LogWarn(err)
				return
			}
			ep.originExtentSizeMap.Store(extentId, extentSize)
			err = store.SetOriginExtentSize(extentId, extentSize)
			if err == ecstorage.ExtentNotFoundError {
				err = nil
			}
		} else {
			holes := make([]*proto.TinyExtentHole, 0)
			if err = json.Unmarshal(data, &holes); err != nil {
				return
			}
			log.LogDebugf("loadPartitionPersistMap partitionId(%v) extentId(%v) holes(%v)", ep.PartitionID, extentId, len(holes))
			ep.originTinyExtentDeleteMapLock.Lock()
			ep.originTinyExtentDeleteMap[extentId] = holes
			ep.originTinyExtentDeleteMapLock.Unlock()
		}
		return
	})
	return
}

func (ep *EcPartition) Stop() {
	if ep.stopC != nil {
		close(ep.stopC)
	}
	ep.extentStore.Close()
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

	ep, err = newEcPartition(metaData, disk)
	if err != nil {
		return
	}

	if err = ep.loadPersistMap(); err != nil {
		log.LogError(err)
		return
	}

	disk.space.AttachPartition(ep)
	disk.AddSize(uint64(ep.Size()))
	return
}

func (ep *EcPartition) updatePartitionLocal(data []byte) (err error) {
	request, err := getChangeEcPartitionMemberRequest(data)
	if err != nil {
		return err
	}

	newHosts := request.Hosts
	ep.replicasLock.Lock()
	ep.Hosts = newHosts
	ep.intervalToUpdateReplicas = time.Now().Unix()
	ep.replicasLock.Unlock()
	err = ep.PersistMetaData()
	return
}

func (ep *EcPartition) updatePartition(nodeAddr string, data []byte, e *EcNode, wg *sync.WaitGroup, successCount *int32) {
	defer wg.Done()
	if nodeAddr == e.localServerAddr {
		// change member local
		err := ep.updatePartitionLocal(data)
		if err != nil {
			log.LogErrorf("updatePartitionLocal fail, PartitionID(%v), err:%v", ep.PartitionID, err)
			return
		}

		atomic.AddInt32(successCount, 1)
	} else {
		// change member on follower
		err := updatePartitionFollower(ep, data, nodeAddr, e)
		if err != nil {
			log.LogErrorf("updatePartitionFollower fail, PartitionID(%v) err(%v)", ep.PartitionID, err)
			return
		}

		atomic.AddInt32(successCount, 1)
	}
}

func updatePartitionFollower(ep *EcPartition, data []byte, nodeAddr string, e *EcNode) error {
	request := repl.NewPacket(context.Background())
	request.ReqID = proto.GenerateRequestID()
	request.PartitionID = ep.PartitionID
	request.Opcode = proto.OpUpdateEcDataPartition
	request.Data = data
	request.Size = uint32(len(data))
	request.CRC = crc32.ChecksumIEEE(data)
	err := DoRequest(request, nodeAddr, proto.ReadDeadlineTime, e)
	if err != nil || request.ResultCode != proto.OpOk {
		return errors.NewErrorf("request OpUpdateEcDataPartition fail. node(%v) resultCode(%v) err:%v", nodeAddr, request.ResultCode, err)
	}

	return nil
}

func (ep *EcPartition) updatePartitionAllEcNode(e *EcNode, r *proto.ChangeEcPartitionMembersRequest, data []byte) error {
	successCount := int32(0)
	wg := sync.WaitGroup{}
	wg.Add(len(r.Hosts))
	for _, host := range r.Hosts {
		go ep.updatePartition(host, data, e, &wg, &successCount)
	}
	wg.Wait()

	if successCount != int32(len(r.Hosts)) {
		return errors.NewErrorf("updatePartitionAllEcNode fail, PartitionID[%v] successCount[%v]",
			ep.PartitionID, successCount)
	}

	return nil
}
func (ep *EcPartition) listExtentsLocal() (fInfoList []*ecstorage.ExtentInfo, holes *holesInfo, err error) {
	var (
		originTinyDeleteCount int64
		tinyDeleteCount       int64
	)
	store := ep.ExtentStore()
	holes = new(holesInfo)
	originTinyDeleteCount = int64(len(ep.originTinyExtentDeleteMap))
	fInfoList, tinyDeleteCount, err = store.GetAllWatermarks(ecstorage.EcExtentFilter())
	holes.TinyDeleteCount = tinyDeleteCount
	holes.OriginTinyDeleteCount = originTinyDeleteCount
	return
}

func (ep *EcPartition) getLocalTinyDeletingInfo(extentId uint64) (deletingInfo *proto.TinyDelInfo, err error) {
	err = ep.extentStore.TinyDelInfoExtentIdRange(extentId, ecstorage.TinyDeleting, func(offset, size uint64, hostIndex uint32) (cbErr error) {
		if deletingInfo != nil {
			cbErr = fmt.Errorf("extent(%v) had more than one deleting info", extentId)
			return
		}
		deletingInfo = new(proto.TinyDelInfo)
		deletingInfo.DeleteStatus = ecstorage.TinyDeleting
		deletingInfo.Offset = offset
		deletingInfo.Size = size
		deletingInfo.HostIndex = hostIndex
		return
	})
	return
}

func (ep *EcPartition) dailEcNode() (err error) {
	for _, host := range ep.Hosts {
		if host == ep.ecNode.localServerAddr {
			continue
		}
		request := repl.NewPacket(context.Background())
		request.ReqID = proto.GenerateRequestID()
		request.PartitionID = ep.PartitionID
		request.Opcode = proto.OpEcNodeDail
		err = DoRequest(request, host, proto.ReadDeadlineTime, ep.ecNode)
		if err != nil {
			err = errors.NewErrorf("request remoteNodeDail fail. node(%v) err:%v", host, err)
			break
		}

		if request.ResultCode != proto.OpOk {
			err = errors.NewErrorf("remoteNodeDail node(%v) resultCode(%v)", host, request.ResultCode)
			break
		}
	}
	return
}

func (ep *EcPartition) getRemoteTinyDeletingInfo(nodeAddr string, extentId uint64, e *EcNode) (deletingInfo *proto.TinyDelInfo, err error) {
	deletingInfo = new(proto.TinyDelInfo)
	request := repl.NewPacket(context.Background())
	request.ReqID = proto.GenerateRequestID()
	request.PartitionID = ep.PartitionID
	request.ExtentID = extentId
	request.Opcode = proto.OpEcGetTinyDeletingInfo
	err = DoRequest(request, nodeAddr, proto.ReadDeadlineTime, e)
	if err != nil {
		err = errors.NewErrorf("request getRemoteTinyDeletingInfo fail. node(%v) err:%v", nodeAddr, err)
		return
	}

	if request.ResultCode != proto.OpOk || len(request.Data) == 0 {
		if len(request.Data) > 0 {
			err = errors.NewErrorf("getRemoteTinyDeletingInfo node(%v) resultCode(%v) data:%v", nodeAddr, request.ResultCode, string(request.Data))
		} else {
			err = errors.NewErrorf("getRemoteTinyDeletingInfo node(%v) resultCode(%v)", nodeAddr, request.ResultCode)
		}
		return
	}
	err = json.Unmarshal(request.Data, deletingInfo)
	if err != nil {
		err = errors.NewErrorf("unmarshal ExtentInfo fail. data:%v err:%v", string(request.Data), err)
		return
	}
	return
}

func (ep *EcPartition) getExtentsFromRemoteNode(nodeAddr string, e *EcNode) (remoteExtentsInfo []*ecstorage.ExtentInfo, holes *holesInfo, err error) {
	holes = new(holesInfo)
	remoteExtentsInfo = make([]*ecstorage.ExtentInfo, 0)
	request := repl.NewPacket(context.Background())
	request.ReqID = proto.GenerateRequestID()
	request.PartitionID = ep.PartitionID
	request.Opcode = proto.OpGetAllWatermarks
	err = DoRequest(request, nodeAddr, proto.ReadDeadlineTime, e)
	if err != nil {
		err = errors.NewErrorf("request OpGetAllWatermarks fail. node(%v) err:%v", nodeAddr, err)
		return
	}

	if request.ResultCode != proto.OpOk || len(request.Data) == 0 {
		if len(request.Data) > 0 {
			err = errors.NewErrorf("OpGetAllWatermarks node(%v) resultCode(%v) data:%v", nodeAddr, request.ResultCode, string(request.Data))
		} else {
			err = errors.NewErrorf("OpGetAllWatermarks node(%v) resultCode(%v)", nodeAddr, request.ResultCode)
		}
		return
	}
	err = json.Unmarshal(request.Data, &remoteExtentsInfo)
	if err != nil {
		err = errors.NewErrorf("unmarshal ExtentInfo fail. data:%v err:%v", string(request.Data), err)
		return
	}
	err = json.Unmarshal(request.Arg, holes)
	if err != nil {
		err = errors.NewErrorf("unmarshal tinyDeleteFileSize fail. data:%v err:%v", string(request.Arg), err)
		return
	}
	return
}

func (ep *EcPartition) tinyExtentDelete(delNodeAddr string, extentId, offset, size uint64) (err error) {
	e := ep.ecNode
	request := repl.NewPacket(context.Background())
	request.ReqID = proto.GenerateRequestID()
	request.PartitionID = ep.PartitionID
	request.ExtentID = extentId
	request.Opcode = proto.OpEcTinyDelete
	request.ExtentType = proto.TinyExtentType
	ext := new(proto.ExtentKey)
	ext.ExtentId = extentId
	ext.PartitionId = ep.PartitionID
	ext.Size = uint32(size)
	ext.ExtentOffset = offset
	request.Data, err = json.Marshal(ext)
	if err != nil {
		return
	}
	request.CRC = crc32.ChecksumIEEE(request.Data)
	request.Size = uint32(len(request.Data))
	err = DoRequest(request, delNodeAddr, proto.ReadDeadlineTime, e)
	if err != nil {
		return errors.NewErrorf("request tinyExtentDelete fail. node(%v) err:%v", delNodeAddr, err)
	}

	if request.ResultCode != proto.OpOk {
		return errors.NewErrorf("request tinyExtentDelete fail. node(%v) resultCode:%v", delNodeAddr, request.ResultCode)
	}
	return
}

func (ep *EcPartition) getAllNeedHandleTinyDelInfo(needHandleExtentsInfo map[uint64]*proto.TinyDelInfo) (recordExtentsIdMap map[uint64]uint32, err error) {
	var deletingInfo *proto.TinyDelInfo
	recordExtentsIdMap = make(map[uint64]uint32)
	ep.extentStore.TinyDelInfoRange(ecstorage.TinyDeleting, func(extentId, offset, size uint64, deleteStatus, hostIndex uint32) (err error) {
		if _, ok := recordExtentsIdMap[extentId]; !ok {
			recordExtentsIdMap[extentId] = deleteStatus
		}
		return
	})

	ep.extentStore.TinyDelInfoRange(ecstorage.TinyDeleteMark, func(extentId, offset, size uint64, deleteStatus, hostIndex uint32) (err error) {
		if _, ok := recordExtentsIdMap[extentId]; !ok {
			recordExtentsIdMap[extentId] = deleteStatus
		}
		return
	})

	for extentId, _ := range recordExtentsIdMap {
		hosts := proto.GetEcHostsByExtentId(uint64(len(ep.Hosts)), extentId, ep.Hosts)
		deletingParityNum := uint32(0)
		for i := int(ep.EcDataNum); i < len(ep.Hosts); i++ {
			hostAddr := hosts[i]
			deletingInfo, err = ep.getRemoteTinyDeletingInfo(hostAddr, extentId, ep.ecNode)
			if err != nil {
				continue
			}
			if deletingInfo != nil && deletingInfo.DeleteStatus == ecstorage.TinyDeleting {
				deletingParityNum++
			}
		}
		if deletingParityNum == 0 {
			continue
		} else if deletingParityNum == ep.EcParityNum {
			deletingInfo.NeedUpdateParity = false
		} else {
			deletingInfo.NeedUpdateParity = true
		}
		needHandleExtentsInfo[extentId] = deletingInfo
	}
	return
}

func (ep *EcPartition) recordRemoteTinyDelStatus(remoteAddr string, extentId, offset, size uint64, deleteStatus, hostIndex uint32) (err error) {
	e := ep.ecNode
	request := repl.NewPacket(context.Background())
	request.ReqID = proto.GenerateRequestID()
	request.PartitionID = ep.PartitionID
	request.ExtentID = extentId
	request.Opcode = proto.OpEcRecordTinyDelInfo
	info := new(proto.TinyDelInfo)
	info.DeleteStatus = deleteStatus
	info.HostIndex = hostIndex
	info.Offset = offset
	info.Size = size
	request.Data, err = json.Marshal(info)
	if err != nil {
		return
	}
	request.CRC = crc32.ChecksumIEEE(request.Data)
	request.Size = uint32(len(request.Data))
	err = DoRequest(request, remoteAddr, proto.ReadDeadlineTime, e)
	if err != nil {
		return errors.NewErrorf("request remoteExtentMarkDelete fail. node(%v) err:%v", remoteAddr, err)
	}

	if request.ResultCode != proto.OpOk {
		return errors.NewErrorf("request remoteExtentMarkDelete fail. node(%v) resultCode:%v", remoteAddr, request.ResultCode)
	}
	return
}

func (ep *EcPartition) recordAllNodeDelStatus(extentId, offset, size uint64, deleteStatus, hostIndex uint32) (err error) {
	for _, host := range ep.Hosts {
		if host == ep.Hosts[0] {
			continue
		}
		err = ep.recordRemoteTinyDelStatus(host, extentId, offset, size, deleteStatus, hostIndex)
		if err != nil {
			return
		}
	}
	err = ep.extentStore.EcRecordTinyDelete(extentId, offset, size, deleteStatus, hostIndex)
	return
}

func (ep *EcPartition) tinyExtentDelHandle(extentId, offset, size uint64, hostIndex uint32, needUpdateParity bool) (err error) {
	log.LogDebugf("tinyExtentDelHandle partitionId(%v) extentId(%v) offset(%v) size(%v) hostIndex(%v)",
		ep.PartitionID, extentId, offset, size, hostIndex)
	delNodeAddr := ep.Hosts[hostIndex]
	if needUpdateParity {
		if err = ep.updateEcParityData(hostIndex, extentId, offset, size); err != nil {
			return
		}
	}

	err = ep.tinyExtentDelete(delNodeAddr, extentId, offset, size)
	if err != nil {
		return
	}

	err = ep.recordAllNodeDelStatus(extentId, offset, size, ecstorage.TinyDeleted, hostIndex)
	return
}

func (ep *EcPartition) tinyAutoDeleteHandle() {
	if ep.ecNode.localServerAddr != ep.Hosts[0] {
		return
	}

	if err := ep.dailEcNode(); err != nil {
		return
	}

	store := ep.extentStore
	needHandleExtentsInfo := make(map[uint64]*proto.TinyDelInfo)
	recordExtentsIdMap, err := ep.getAllNeedHandleTinyDelInfo(needHandleExtentsInfo)
	if err != nil {
		log.LogErrorf("getAllTinyDeletingInfo err(%v)", err)
		return
	}

	for extentId, deletingInfo := range needHandleExtentsInfo {
		err = ep.tinyExtentDelHandle(extentId, deletingInfo.Offset, deletingInfo.Size, deletingInfo.HostIndex, deletingInfo.NeedUpdateParity)
		if err == nil {
			delete(needHandleExtentsInfo, extentId)
		} else {
			log.LogWarnf("tinyExtentDelHandle partition(%v) extent(%v) offset(%v) size(%v) err(%v)",
				ep.PartitionID, extentId, deletingInfo.Offset, deletingInfo.Size, err)
		}
	}
	handleCount := uint8(0)
	store.TinyDelInfoRange(ecstorage.TinyDeleteMark, func(extentId, offset, size uint64, deleteStatus, hostIndex uint32) (cbErr error) {
		_, exist := needHandleExtentsInfo[extentId]
		if exist { //is deleting, need handle first
			return
		}
		_, exist = recordExtentsIdMap[extentId]
		if !exist {
			log.LogWarnf("partition(%v) extentId(%v) not record, delay handle", ep.PartitionID, extentId)
			return
		}
		if handleCount > ep.ecNode.maxTinyDelCount {
			return
		}
		handleCount++
		log.LogDebugf("tinyDeleteHandle delNode(%v) partition(%v) extent(%v) offset(%v) size(%v) ",
			ep.Hosts[hostIndex], ep.PartitionID, extentId, offset, size)
		cbErr = ep.tinyExtentDelHandle(extentId, offset, size, hostIndex, true)
		if cbErr != nil {
			cbErr = errors.NewErrorf("tinyExtentDelHandle partition(%v) extent(%v) offset(%v) size(%v) err(%v)",
				ep.PartitionID, extentId, offset, size, cbErr)
			delInfo := &proto.TinyDelInfo{
				ExtentId:     extentId,
				Offset:       offset,
				Size:         size,
				DeleteStatus: deleteStatus,
				HostIndex:    hostIndex,
			}
			needHandleExtentsInfo[extentId] = delInfo
		}
		return
	})
}

func (ep *EcPartition) convertToExtentInfos(extentInfoList []*ecstorage.ExtentInfo) (extentsInfoMap map[uint64]*ecstorage.ExtentInfo) {
	extentsInfoMap = make(map[uint64]*ecstorage.ExtentInfo, 0)

	for _, file := range extentInfoList {
		extentId := file.FileID
		extentsInfoMap[extentId] = file
	}
	return
}

func (ep *EcPartition) getEcNodeExtentsInfo(i int, nodeAddr string, e *EcNode, extentsInfo []map[uint64]*ecstorage.ExtentInfo, extentsTinyDelRecordList []*holesInfo) error {
	if nodeAddr == e.localServerAddr {
		list, holes, err := ep.listExtentsLocal()
		if err != nil {
			log.LogErrorf("listExtentsLocal fail, PartitionID(%v), err:%v", ep.PartitionID, err)
			return err
		}
		log.LogDebugf("local partition(%v) OriginTinyDeleteCount(%v) TinyDeleteCount(%v) node(%v)", ep.PartitionID, holes.OriginTinyDeleteCount, holes.TinyDeleteCount, nodeAddr)
		extentsTinyDelRecordList[i] = holes
		extentsInfo[i] = ep.convertToExtentInfos(list)
	} else {
		list, holes, err := ep.getExtentsFromRemoteNode(nodeAddr, e)
		if err != nil {
			log.LogErrorf("getExtentsFromRemoteNode fail, PartitionID(%v), err:%v", ep.PartitionID, err)
			return err
		}
		log.LogDebugf("partition(%v) OriginTinyDeleteCount(%v) TinyDeleteCount(%v) node(%v)", ep.PartitionID, holes.OriginTinyDeleteCount, holes.TinyDeleteCount, nodeAddr)
		extentsTinyDelRecordList[i] = holes
		extentsInfo[i] = ep.convertToExtentInfos(list)
	}
	return nil
}

func (ep *EcPartition) getMaxTinyDelCount(extentsTinyDelRecordList []*holesInfo) (maxTinyDelCount int64, normalHost string) {
	for hostIndex, nodeTinyDelInfo := range extentsTinyDelRecordList {
		if nodeTinyDelInfo.TinyDeleteCount > maxTinyDelCount {
			maxTinyDelCount = nodeTinyDelInfo.TinyDeleteCount
			normalHost = ep.Hosts[hostIndex]
		}
	}
	return
}

func (ep *EcPartition) getMaxOriginTinyDelSize(extentsTinyDelRecordList []*holesInfo) (maxOriginTinyDelCount int64, normalHost string) {
	for hostIndex, nodeTinyDelInfo := range extentsTinyDelRecordList {
		if nodeTinyDelInfo.OriginTinyDeleteCount > maxOriginTinyDelCount {
			maxOriginTinyDelCount = nodeTinyDelInfo.OriginTinyDeleteCount
			normalHost = ep.Hosts[hostIndex]
		}
	}
	return
}

func (ep *EcPartition) getMaxExtentsInfo(extentsInfoNodeList []map[uint64]*ecstorage.ExtentInfo) (maxExtentsInfoMap map[uint64]*ecstorage.ExtentInfo) {
	maxExtentsInfoMap = make(map[uint64]*ecstorage.ExtentInfo)
	for _, nodeExtentsInfoMap := range extentsInfoNodeList {
		for extentId, nodeExtentInfo := range nodeExtentsInfoMap {
			if maxExtentInfo, ok := maxExtentsInfoMap[extentId]; ok {
				if nodeExtentInfo.OriginExtentSize > maxExtentInfo.OriginExtentSize {
					maxExtentInfo.OriginExtentSize = nodeExtentInfo.OriginExtentSize
				}
				if nodeExtentInfo.Size > maxExtentInfo.Size {
					maxExtentInfo.Size = nodeExtentInfo.Size
				}
				continue
			}
			maxExtentsInfoMap[extentId] = nodeExtentInfo
		}
	}
	return
}

func (ep *EcPartition) getNeedRepairOriginTinyDel(extentsTinyDelRecordList []*holesInfo) (needRepairOriginTinyDel *repairTinyDelInfo, err error) {
	needRepairOriginTinyDel = &repairTinyDelInfo{}
	maxOriginTinyDelCount, normalHost := ep.getMaxOriginTinyDelSize(extentsTinyDelRecordList)
	for hostIndex, nodeTinyFileSize := range extentsTinyDelRecordList {
		if nodeTinyFileSize.OriginTinyDeleteCount < maxOriginTinyDelCount {
			needRepairOriginTinyDel.needRepairHosts = append(needRepairOriginTinyDel.needRepairHosts, ep.Hosts[hostIndex])
		}
	}
	needRepairOriginTinyDel.maxTinyDelCount = maxOriginTinyDelCount
	needRepairOriginTinyDel.normalHost = normalHost
	return
}

func (ep *EcPartition) getNeedRepairTinyDel(extentsTinyDelRecordList []*holesInfo) (needRepairTinyDel *repairTinyDelInfo, err error) {
	needRepairTinyDel = &repairTinyDelInfo{}
	maxTinyDelCount, normalHost := ep.getMaxTinyDelCount(extentsTinyDelRecordList)
	for hostIndex, nodeTinyFileSize := range extentsTinyDelRecordList {
		if nodeTinyFileSize.TinyDeleteCount < maxTinyDelCount {
			needRepairTinyDel.needRepairHosts = append(needRepairTinyDel.needRepairHosts, ep.Hosts[hostIndex])
		}
	}
	needRepairTinyDel.maxTinyDelCount = maxTinyDelCount
	needRepairTinyDel.normalHost = normalHost
	return
}

func (ep *EcPartition) getNeedRepairExtents(e *EcNode) (needCreateExtents map[uint64]*createExtentInfo, needRepairExtents map[uint64]*repairExtentInfo, extentsTinyDelRecordList []*holesInfo, err error) {
	nodeNum := len(ep.Hosts)
	extentsInfoNodeList := make([]map[uint64]*ecstorage.ExtentInfo, nodeNum)
	extentsTinyDelRecordList = make([]*holesInfo, nodeNum)
	for index, host := range ep.Hosts {
		if err = ep.getEcNodeExtentsInfo(index, host, e, extentsInfoNodeList, extentsTinyDelRecordList); err != nil {
			return
		}
	}
	maxExtentsInfoMap := ep.getMaxExtentsInfo(extentsInfoNodeList)
	//k:extentId value: need repaired extent info
	needRepairExtents = make(map[uint64]*repairExtentInfo, 0)
	needCreateExtents = make(map[uint64]*createExtentInfo, 0)
	var stripeUnitFileSize uint64
	for extentId, maxExtentInfo := range maxExtentsInfoMap {
		for index, nodeExtentsInfoMap := range extentsInfoNodeList {
			stripeUnitFileSize, err = ep.getExtentStripeUnitFileSize(extentId, maxExtentInfo.OriginExtentSize, ep.Hosts[index])
			if err != nil {
				continue
			}

			nodeExtentInfo, ok := nodeExtentsInfoMap[extentId]
			if ok {
				if nodeExtentInfo.Size < stripeUnitFileSize { //data len err, need repair
					log.LogDebugf("getNeedRepairExtents nodeSize[%v] stripeUnitFileSize[%v] node[%v]", nodeExtentInfo.Size, stripeUnitFileSize, ep.Hosts[index])
					ep.fillNeedRepairExtents(nodeExtentInfo, stripeUnitFileSize, maxExtentInfo.OriginExtentSize, ep.Hosts[index], needRepairExtents)
				}
				continue
			}

			//data missing, need create extent and repair data
			ep.fillNeedCreateExtents(extentId, maxExtentInfo.OriginExtentSize, ep.Hosts[index], needCreateExtents)
			log.LogDebugf("getNeedRepairExtents missing extent[%v] node[%v]", extentId, ep.Hosts[index])
			extentInfo := &ecstorage.ExtentInfo{
				FileID:           extentId,
				Size:             0,
				OriginExtentSize: 0,
			}
			ep.fillNeedRepairExtents(extentInfo, stripeUnitFileSize, maxExtentInfo.OriginExtentSize, ep.Hosts[index], needRepairExtents)
		}
	}
	return
}

func (ep *EcPartition) Repair() {
	select {
	case ep.repairC <- struct{}{}:
	default:
	}
}

func (ep *EcPartition) changeMember(e *EcNode, r *proto.ChangeEcPartitionMembersRequest, data []byte) error {
	err := ep.updatePartitionAllEcNode(e, r, data)
	if err != nil { //wait updateEcHosts schedule
		return err
	}
	ep.Repair()
	return nil
}

func getChangeEcPartitionMemberRequest(data []byte) (*proto.ChangeEcPartitionMembersRequest, error) {
	request := &proto.ChangeEcPartitionMembersRequest{}
	task := proto.AdminTask{}
	task.Request = request
	err := json.Unmarshal(data, &task)
	if err != nil {
		return nil, err
	}

	return request, nil
}

// CreateEcPartition create ec partition and return its instance
func CreateEcPartition(epMetaData *EcPartitionMetaData, disk *Disk) (ep *EcPartition, err error) {
	ep, err = newEcPartition(epMetaData, disk)
	if err != nil {
		return
	}

	err = ep.PersistMetaData()
	if err != nil {
		return
	}

	disk.AddSize(uint64(ep.Size()))
	disk.computeUsage()
	return
}

func (ep *EcPartition) extentCreate(extentId, originExtentSize uint64) (err error) {
	// create ExtentFile
	err = ep.extentStore.EcCreate(extentId, originExtentSize, true)
	if err != nil {
		err = errors.NewErrorf("RepairWrite createExtent fail, err:%v", err)
		return
	}
	if err = ep.persistOriginExtentSize(extentId, originExtentSize); err != nil {
		log.LogErrorf("PersistOriginExtentSize failed err(%v)", err)
	}
	return
}

func (ep *EcPartition) remoteCreateExtent(extentId, originExtentSize uint64, nodeAddr string) (err error) {
	request := repl.NewPacket(context.Background())
	request.Opcode = proto.OpCreateExtent
	request.ExtentOffset = int64(originExtentSize)
	request.PartitionID = ep.PartitionID
	request.ExtentID = extentId
	request.ReqID = proto.GenerateRequestID()
	err = DoRequest(request, nodeAddr, proto.ReadDeadlineTime, ep.ecNode)
	// if err not nil, do nothing, only record log
	if err != nil {
		err = errors.NewErrorf("repairCreateExtent err(%v)", err)
	}
	if request.ResultCode != proto.OpOk {
		err = errors.NewErrorf("repairCreateExtent err(%v) resultCode(%v)", err, request.ResultCode)
	}
	return
}

func (ep *EcPartition) repairCreateExtent(extentId uint64, createInfo *createExtentInfo) (err error) {
	if len(createInfo.hosts) > int(ep.EcParityNum) {
		log.LogWarnf("need create extent hostsNum(%v) > EcParityNum", len(createInfo.hosts))
		return
	}
	for _, host := range createInfo.hosts {
		log.LogDebugf("partition[%v] repairCreateExtent[%v] originExtentSize(%v) nodeAddr(%v)", ep.PartitionID, extentId, createInfo.originExtentSize, host)
		if host == ep.ecNode.localServerAddr {
			err = ep.extentCreate(extentId, createInfo.originExtentSize)
		} else {
			err = ep.remoteCreateExtent(extentId, createInfo.originExtentSize, host)
		}
		if err != nil {
			return
		}
	}
	return
}

func (ep *EcPartition) repairExtentData(extentId uint64, repairInfo *repairExtentInfo, readFlag int) (err error) {
	log.LogDebugf("partition(%v) repairExtentId(%v) repairSize(%v) repairOffset(%v) repairFlag(%v)",
		ep.PartitionID, extentId, repairInfo.needRepairSize, repairInfo.repairStripeUnitOffset, readFlag)
	if len(repairInfo.needRepairHosts) > int(ep.EcParityNum) {
		err = errors.NewErrorf(" partition[%v] repairExtentData bad node[%v] > parityNum, can't repair extent[%v]", ep.PartitionID, len(repairInfo.needRepairHosts), extentId)
		return
	}
	stripeUnitSize := proto.CalStripeUnitSize(repairInfo.originExtentSize, ep.EcMaxUnitSize, uint64(ep.EcDataNum))
	ecStripe, _ := NewEcStripe(ep, stripeUnitSize, extentId)
	stripeUnitFileOffset := repairInfo.repairStripeUnitOffset
	repairSize := uint64(0)
	for {
		if repairSize >= repairInfo.needRepairSize {
			break
		}
		curRepairSize := unit.Min(int(repairInfo.needRepairSize-repairSize), unit.EcBlockSize)
		_, err = ecStripe.repairStripeData(stripeUnitFileOffset, uint64(curRepairSize), repairInfo, readFlag)
		if err != nil {
			return
		}
		stripeUnitFileOffset += uint64(curRepairSize)
		repairSize += uint64(curRepairSize)
	}
	return
}

func (ep *EcPartition) getExtentStripeUnitFileSize(extentId, originExtentSize uint64, nodeAddr string) (stripeUnitFileSize uint64, err error) {
	if originExtentSize == 0 {
		log.LogWarnf("getExtentStripeUnitFileSize originExtentSize==0 partition(%v) extent(%v)",
			ep.PartitionID, extentId)
		return
	}
	stripeUnitSize := proto.CalStripeUnitSize(originExtentSize, ep.EcMaxUnitSize, uint64(ep.EcDataNum))
	ecStripe, _ := NewEcStripe(ep, stripeUnitSize, extentId)
	stripeUnitFileSize, err = ecStripe.calcStripeUnitFileSize(originExtentSize, nodeAddr)
	if err != nil {
		return
	}
	return
}

func (ep *EcPartition) extentNeedScrub(ei *ecstorage.ExtentInfo) (stripeUnitFileSize uint64, needScrub bool) {
	needScrub = false
	stripeUnitFileSize, err := ep.getExtentStripeUnitFileSize(ei.FileID, ei.OriginExtentSize, ep.ecNode.localServerAddr)
	if err != nil {
		log.LogErrorf("partition(%v) extent(%v) extentNeedScrub err(%v)", ep.PartitionID, ei.FileID, err)
		return
	}
	if stripeUnitFileSize == 0 {
		log.LogDebugf("partition(%v) extent(%v) originExtentSize(%v) stripeUnitFileSize == 0, don't need scrub", ep.PartitionID, ei.FileID, ei.OriginExtentSize)
		return
	}
	if ecstorage.IsTinyExtent(ei.FileID) && time.Now().Unix()-ei.ModifyTime < ecstorage.UpdateCrcInterval {
		modifyTime := time.Unix(ei.ModifyTime, 0).Format("2006-01-02 15:04:05")
		log.LogDebugf("partition(%v) extent(%v) ModifyTime(%v), don't need scrub",
			ep.PartitionID, ei.FileID, modifyTime)
		return
	}
	if ei.Size < stripeUnitFileSize { //recovering or writing
		log.LogDebugf("partition(%v) extent(%v) extentSize(%v) stripeUnitFileSize(%v) is recovering or writing, don't need scrub",
			ep.PartitionID, ei.FileID, ei.Size, stripeUnitFileSize)
		return
	}
	needScrub = true
	return
}

func (ep *EcPartition) fillNeedCreateExtents(extentId, originExtentSize uint64, nodeAddr string, needCreateExtents map[uint64]*createExtentInfo) {
	if createInfo, ok := needCreateExtents[extentId]; ok {
		createInfo.hosts = append(createInfo.hosts, nodeAddr)
		return
	}
	var createInfo createExtentInfo
	createInfo.hosts = append(createInfo.hosts, nodeAddr)
	createInfo.originExtentSize = originExtentSize
	needCreateExtents[extentId] = &createInfo
	log.LogDebugf("fillCreateExtents extentId(%v) originExtentSize(%v)", extentId, originExtentSize)
}

func (ep *EcPartition) fillNeedRepairExtents(extentInfo *ecstorage.ExtentInfo, stripeUnitFileSize, originExtentSize uint64, nodeAddr string, needRepairExtents map[uint64]*repairExtentInfo) {
	repairSize := stripeUnitFileSize - extentInfo.Size
	if repairInfo, ok := needRepairExtents[extentInfo.FileID]; ok {
		ep.fillRepairExtentInfo(stripeUnitFileSize, extentInfo.Size, repairSize, originExtentSize, nodeAddr, repairInfo)
		log.LogDebugf("more fillNeedRepairExtents partition[%v] extent[%v] originExtentSize[%v] nodeAddr[%v]",
			ep.PartitionID, extentInfo.FileID, originExtentSize, nodeAddr)
		return
	}
	var repairInfo repairExtentInfo
	repairInfo.repairStripeUnitOffset = extentInfo.Size
	ep.fillRepairExtentInfo(stripeUnitFileSize, extentInfo.Size, repairSize, originExtentSize, nodeAddr, &repairInfo)
	needRepairExtents[extentInfo.FileID] = &repairInfo
	log.LogDebugf("first fillNeedRepairExtents partition[%v] extent[%v] originExtentSize[%v] nodeAddr[%v]",
		ep.PartitionID, extentInfo.FileID, originExtentSize, nodeAddr)
}

func (ep *EcPartition) fillRepairExtentInfo(stripeUnitFileSize, offset, size, originExtentSize uint64, nodeAddr string, repairInfo *repairExtentInfo) {
	repairNodeInfo := &repairHostInfo{
		nodeAddr:           nodeAddr,
		offset:             offset,
		stripeUnitFileSize: stripeUnitFileSize,
	}

	if size > repairInfo.needRepairSize {
		repairInfo.needRepairSize = size
	}

	if offset < repairInfo.repairStripeUnitOffset {
		repairInfo.repairStripeUnitOffset = offset
	}

	if originExtentSize > repairInfo.originExtentSize {
		repairInfo.originExtentSize = originExtentSize
	}

	repairInfo.needRepairHosts = append(repairInfo.needRepairHosts, repairNodeInfo)
	return
}

func (ep *EcPartition) repairWriteToExtent(p *repl.Packet, size int64, data []byte, crc uint32, writeType int) (err error) {
	store := ep.extentStore
	if p.Size <= unit.EcBlockSize {
		err = store.EcWrite(p.ExtentID, p.ExtentOffset, size, data, crc, writeType, p.IsSyncWrite())
		ep.checkIsDiskError(err)
	} else {
		writeSize := uint32(size)
		offset := 0
		for writeSize > 0 {
			if writeSize <= 0 {
				break
			}
			currSize := unit.Min(int(writeSize), unit.EcBlockSize)
			writeData := data[offset : offset+currSize]
			writeCrc := crc32.ChecksumIEEE(writeData)
			err = store.EcWrite(p.ExtentID, p.ExtentOffset+int64(offset), int64(currSize), writeData, writeCrc, writeType, p.IsSyncWrite())
			ep.checkIsDiskError(err)
			if err != nil {
				break
			}
			writeSize -= uint32(currSize)
			offset += currSize
		}
	}
	return err
}

func (ep *EcPartition) writeToExtent(p *repl.Packet, writeType int) (err error) {
	store := ep.extentStore
	if p.Size <= unit.EcBlockSize {
		err = store.EcWrite(p.ExtentID, p.ExtentOffset, int64(p.Size), p.Data, p.CRC, writeType, p.IsSyncWrite())
		ep.checkIsDiskError(err)
	} else {
		size := p.Size
		offset := 0
		for size > 0 {
			if size <= 0 {
				break
			}
			currSize := unit.Min(int(size), unit.EcBlockSize)
			data := p.Data[offset : offset+currSize]
			crc := crc32.ChecksumIEEE(data)
			err = store.EcWrite(p.ExtentID, p.ExtentOffset+int64(offset), int64(currSize), data, crc, writeType, p.IsSyncWrite())
			ep.checkIsDiskError(err)
			if err != nil {
				break
			}
			size -= uint32(currSize)
			offset += currSize
		}
	}

	return err
}

func (ep *EcPartition) repairTinyExtent(ei *ecstorage.ExtentInfo, offset, size, stripeUnitFileSize uint64, nodeAddr string, repairFlag int) (err error) {
	var repairInfo repairExtentInfo
	repairInfo.repairStripeUnitOffset = offset
	ep.fillRepairExtentInfo(stripeUnitFileSize, offset, size, ei.OriginExtentSize, nodeAddr, &repairInfo)
	err = ep.repairExtentData(ei.FileID, &repairInfo, repairFlag)
	if err != nil {
		log.LogWarnf("repairExtentData err[%v]", err)
	}
	return
}

func (ep *EcPartition) updateEcParityData(hostIndex uint32, extentId, offset, size uint64) (err error) {
	var (
		ei                 *ecstorage.ExtentInfo
		repairInfo         repairExtentInfo
		stripeUnitFileSize uint64
	)
	ei, err = ep.ExtentStore().EcWatermark(extentId)
	if err != nil {
		return
	}

	nodeNum := uint64(ep.EcParityNum + ep.EcDataNum)
	newHostsArr := proto.GetEcHostsByExtentId(nodeNum, extentId, ep.Hosts)
	repairInfo.repairStripeUnitOffset = offset
	repairInfo.tinyDelNodeIndex = hostIndex
	for i := uint64(1); i <= uint64(ep.EcParityNum); i++ {
		stripeUnitFileSize, err = ep.getExtentStripeUnitFileSize(extentId, ei.OriginExtentSize, newHostsArr[nodeNum-i])
		if err != nil {
			return
		}
		log.LogDebugf("updateParityData node(%v) partition(%v) extent(%v) offset(%v) size(%v)",
			newHostsArr[nodeNum-i], ep.PartitionID, extentId, offset, size)
		ep.fillRepairExtentInfo(stripeUnitFileSize, offset, size, ei.OriginExtentSize, newHostsArr[nodeNum-i], &repairInfo)
	}
	err = ep.repairExtentData(extentId, &repairInfo, proto.DeleteRepair)
	return
}

func (ep *EcPartition) checkCanRepairRead(hostIndex uint32, repairHosts []string) bool {
	for _, repairHost := range repairHosts {
		if repairHost == ep.Hosts[hostIndex] {
			return true
		}
	}
	return false
}

func (ep *EcPartition) checkDataCanRead(extentId, readOffset, readSize uint64) (err error) {
	if !proto.IsEcParityNode(ep.Hosts, ep.ecNode.localServerAddr, extentId, uint64(ep.EcDataNum), uint64(ep.EcParityNum)) {
		return
	}
	err = ep.extentStore.TinyDelInfoExtentIdRange(extentId, ecstorage.TinyDeleting, func(delOffset, delSize uint64, delHostIndex uint32) (cbErr error) {
		if readOffset+readSize <= delOffset || readOffset >= delOffset+delSize {
			return
		} else { //read size contain deleting size
			//repair host isn't deletingHost can't repair.
			//	if !ep.checkCanRepairRead(delHostIndex, repairHosts) {
			cbErr = fmt.Errorf("host(%v) is deleting partition(%v) extent(%v) offset(%v) size(%v) need wait delete done",
				ep.Hosts[delHostIndex], ep.PartitionID, extentId, delOffset, delSize)
			//	}
		}
		return
	})
	return
}

func (ep *EcPartition) delFailScrubExtents(extentId uint64) {
	ep.failScrubMapLock.Lock()
	defer ep.failScrubMapLock.Unlock()
	delete(ep.FailScrubExtents, extentId)
}

func (ep *EcPartition) putFailScrubExtents(extentId uint64) {
	ep.failScrubMapLock.Lock()
	defer ep.failScrubMapLock.Unlock()
	ep.FailScrubExtents[extentId] = extentId
}

func (ep *EcPartition) needFillFailScrubMap(err error, isScrubFailHandle bool) bool {
	if err == nil {
		return false
	}
	if !isScrubFailHandle && !IsDiskErr(err.Error()) && !ep.extentStore.IsExtentNotExistErr(err.Error()) {
		return true
	}
	return false
}

func (ep *EcPartition) checkBlockCrc(extentId uint64, wg *sync.WaitGroup, isScrubFailHandle bool) {
	var (
		err        error
		size       int
		crc        uint32
		repairRead bool
	)
	defer func() {
		if isScrubFailHandle && err == nil {
			ep.delFailScrubExtents(extentId)
		}
		wg.Done()
	}()
	ei, err := ep.extentStore.EcWatermark(extentId)
	if err != nil || ei != nil && ei.IsDeleted {
		log.LogWarnf("checkBlockCrc err(%v)", err)
		err = nil
		return
	}
	stripeUnitFileSize, needScrub := ep.extentNeedScrub(ei)
	if !needScrub {
		return
	}

	if proto.IsTinyExtent(extentId) {
		repairRead = true
	}
	//calc stripeUnitSize
	stripeUnitSize := proto.CalStripeUnitSize(ei.OriginExtentSize, ep.EcMaxUnitSize, uint64(ep.EcDataNum))

	ecStripe, err := NewEcStripe(ep, stripeUnitSize, ei.FileID)
	if ep.needFillFailScrubMap(err, isScrubFailHandle) {
		ep.putFailScrubExtents(extentId)
		return
	}

	ep.extentStore.PersistBlockCrcRange(extentId, func(blockNum uint64, blockCrc, blockSize uint32) (err error) {
		if blockCrc == 0 { //tiny blockCrc not update
			log.LogInfof("checkBlockCrc partition(%v) extentId(%v) block(%v) tiny crc not update",
				ep.PartitionID, extentId, blockNum)
			return
		}

		offset := int64(blockNum * unit.EcBlockSize)
		data := make([]byte, blockSize)
		size, crc, err = ep.extentStore.EcRead(ei.FileID, offset, int64(blockSize), data, repairRead)
		if !ep.checkIsEofError(err, ecStripe, ei.OriginExtentSize, uint64(offset), uint64(blockSize)) {
			err = nil
		}
		if err != nil && !IsDiskErr(err.Error()) && !ep.extentStore.IsExtentNotExistErr(err.Error()) || crc != blockCrc {
			log.LogErrorf("checkBlockCrc dataCrc[%v] blockCrc[%v] partition[%v] extent[%v] size[%v] blockSize[%v] offset[%v] blockNum[%v] err[%v]",
				crc, blockCrc, ep.PartitionID, ei.FileID, size, blockSize, offset, blockNum, err)
			msg := fmt.Sprintf("scrub find crc is not consist, partition(%v) extent(%v) block(%v)",
				ep.PartitionID, extentId, blockNum)
			exporter.Warning(msg)
			if ecstorage.IsTinyExtent(ei.FileID) {
				err = ep.repairTinyExtent(ei, uint64(offset), uint64(blockSize), stripeUnitFileSize, ep.ecNode.localServerAddr, proto.ScrubRepair)
				if ep.needFillFailScrubMap(err, isScrubFailHandle) {
					ep.putFailScrubExtents(extentId)
				}
			} else {
				ep.ExtentStore().EcMarkDelete(ei.FileID, ep.NodeIndex, 0, 0)
				err = fmt.Errorf("normal extent crc inconsist, markDelete extent")
				return
			}
		}
		return
	})
}

func (ep *EcPartition) startScrubExtent(scrubChan <-chan *scrubChanInfo, wg *sync.WaitGroup) {
	for {
		select {
		case scrubInfo, ok := <-scrubChan:
			if !ok {
				return
			}
			ep.checkBlockCrc(scrubInfo.extentId, wg, scrubInfo.isScrubFailHandle)
		}
	}
}

func (ep *EcPartition) scrubFailPartitionExtents(ecNode *EcNode) {
	var wg sync.WaitGroup
	scrubChan := make(chan *scrubChanInfo, ecNode.maxScrubExtents)
	defer close(scrubChan)
	for i := uint8(0); i < ecNode.maxScrubExtents; i++ {
		go ep.startScrubExtent(scrubChan, &wg)
	}
	concurrentExtents := uint8(0)
	for _, extentId := range ep.FailScrubExtents {
		if !ecNode.scrubEnable { //stop scrub
			return
		}

		wg.Add(1)
		scrubChan <- &scrubChanInfo{extentId, true}
		concurrentExtents++
		if concurrentExtents%ecNode.maxScrubExtents == 0 {
			wg.Wait()
			if err := ep.PersistMetaData(); err != nil {
				return
			}
		}
	}
	wg.Wait()
	ep.PersistMetaData()
	log.LogDebugf("end node[%v] scrubFailPartitionExtents PartitionID(%v) MaxScrubDoneId(%v) ",
		ecNode.localServerAddr, ep.PartitionID, ep.MaxScrubDoneId)
	return
}

func (ep *EcPartition) scrubPartitionData(ecNode *EcNode) (err error) {
	log.LogDebugf("start node[%v] scrubPartitionData PartitionID(%v) extent(%v)",
		ecNode.localServerAddr, ep.PartitionID, ep.MaxScrubDoneId)

	var (
		wg          sync.WaitGroup
		sortExtents []uint64
	)
	sortExtents = make([]uint64, 0)
	err = ep.ExtentStore().PersistMapRange(ecstorage.OriginExtentSizeMapTable, func(extentId uint64, data []byte) (err error) {
		sortExtents = append(sortExtents, extentId)
		return
	})
	if err != nil {
		return
	}
	if ep.ScrubStartTime == 0 {
		ep.ScrubStartTime = time.Now().Unix()
	}
	scrubChan := make(chan *scrubChanInfo, ecNode.maxScrubExtents)
	defer close(scrubChan)
	for i := uint8(0); i < ecNode.maxScrubExtents; i++ {
		go ep.startScrubExtent(scrubChan, &wg)
	}

	concurrentExtents := uint8(0)
	for _, id := range sortExtents {
		if !ecNode.scrubEnable { //stop scrub
			return
		}

		if id <= ep.MaxScrubDoneId {
			continue
		}

		wg.Add(1)
		scrubChan <- &scrubChanInfo{id, false}

		concurrentExtents++
		ep.MaxScrubDoneId = id
		if concurrentExtents%ecNode.maxScrubExtents == 0 {
			wg.Wait()
			if err = ep.PersistMetaData(); err != nil {
				return
			}
		}
	}
	wg.Wait()
	err = ep.PersistMetaData()
	log.LogDebugf("end node[%v] scrubPartitionData PartitionID(%v) MaxScrubDoneId(%v) ",
		ecNode.localServerAddr, ep.PartitionID, ep.MaxScrubDoneId)
	return
}

// Compare the fetched replica with the local one.
func (ep *EcPartition) compareEcReplicas(v1, v2 []string) (equals bool) {
	equals = true
	if len(v1) == len(v2) {
		for i := 0; i < len(v1); i++ {
			if v1[i] != v2[i] {
				equals = false
				return
			}
		}
		equals = true
		return
	}
	equals = false
	return
}

func (ep *EcPartition) fetchEcReplicasFromMaster() (replicas []string, err error) {
	var ecdp *proto.EcPartitionInfo
	if ecdp, err = MasterClient.AdminAPI().GetEcPartition(ep.VolumeID, ep.PartitionID); err != nil {
		return
	}
	for _, host := range ecdp.Hosts {
		replicas = append(replicas, host)
	}
	ep.ecMigrateStatus = ecdp.EcMigrateStatus
	return
}

func (ep *EcPartition) updateEcInfo() (err error) {
	if time.Now().Unix()-ep.intervalToUpdateReplicas <= defaultEcUpdateReplicaInterval {
		return
	}
	replicas, err := ep.fetchEcReplicasFromMaster()
	if err != nil {
		log.LogWarnf("get partition[%v] volume[%v] replicas err[%v]", ep.PartitionID, ep.VolumeID, err)
		return
	}
	ep.replicasLock.Lock()
	defer ep.replicasLock.Unlock()
	if !ep.compareEcReplicas(ep.Hosts, replicas) {
		log.LogDebugf("action[updateEcHosts] partition(%v) replicas changed from (%v) to (%v).",
			ep.PartitionID, ep.Hosts, replicas)
		if len(replicas) == int(ep.EcDataNum+ep.EcParityNum) { //ecHost must equal ep.EcDataNum + ep.EcParityNum
			ep.Hosts = replicas
		}
	}
	ep.intervalToUpdateReplicas = time.Now().Unix()
	log.LogDebugf("action[updateEcHosts] partition(%v) replicas ", ep.PartitionID)
	return
}

func (ep *EcPartition) repairAndTinyDelScheduler() {
	repairTimer := time.NewTimer(time.Minute * defaultRepairInterval)
	tinyAutoDeleteTimer := time.NewTimer(time.Minute * 0)
	for {
		select {
		case <-ep.repairC:
			repairTimer.Stop()
			ep.repairPartitionData(false)
			repairTimer.Reset(time.Minute * defaultRepairInterval)
		case <-repairTimer.C:
			ep.statusUpdate()
			ep.repairPartitionData(true)
			repairTimer.Reset(time.Minute * defaultRepairInterval)
		case <-tinyAutoDeleteTimer.C:
			ep.tinyAutoDeleteHandle()
			tinyAutoDeleteTimer.Reset(time.Minute * defaultAutoDeleteTinyInterval)
		case <-ep.stopC:
			repairTimer.Stop()
			return
		}
	}
}

func (ep *EcPartition) repairOriginTinyDelInfo(readHost string) (err error) {
	var (
		conn *net.TCPConn
	)
	defer func() {
		if err != nil {
			log.LogErrorf("repairOriginTinyDelFile err(%v)", err)
		}
	}()
	p := new(repl.Packet)
	p.Opcode = proto.OpEcOriginTinyDelInfoRead
	p.PartitionID = ep.PartitionID
	p.Magic = proto.ProtoMagic
	p.ReqID = proto.GenerateRequestID()
	p.ExtentOffset = 0
	p.SetCtx(context.Background())
	if conn, err = gConnPool.GetConnect(readHost); err != nil {
		return
	}
	defer gConnPool.PutConnect(conn, true)
	if err = p.WriteToConn(conn, proto.WriteDeadlineTime); err != nil {
		return
	}
	if err = p.ReadFromConn(conn, proto.ReadDeadlineTime); err != nil {
		return
	}
	originTinyExtentDeleteMap := make(map[uint64][]*proto.TinyExtentHole, 0)
	log.LogDebugf("repairOriginTinyDel partition(%v) dataLen(%v) readHost(%v) size(%v)", ep.PartitionID, len(p.Data), readHost, p.Size)
	if err = json.Unmarshal(p.Data, &originTinyExtentDeleteMap); err != nil {
		log.LogErrorf("partition(%v) newEcPartition Unmarshal(%v) err(%v)", ep.PartitionID, OriginTinyExtentDeleteFileName, err)
		return
	}
	for extentId, holes := range originTinyExtentDeleteMap {
		log.LogDebugf("repairOriginTinyDel partition(%v) extent(%v) holes(%v)", ep.PartitionID, extentId, holes)
		err = ep.persistOriginTinyDelInfo(extentId, holes)
		if err != nil {
			return
		}
	}
	return
}

func (ep *EcPartition) repairTinyDelete(readHost string, maxTinyDelCount int64) (err error) {
	var (
		conn *net.TCPConn
	)
	defer func() {
		if err != nil {
			log.LogErrorf("repairTinyDel err(%v)", err)
		}
	}()
	packet := repl.NewPacketToReadEcTinyDeleteRecord(context.Background(), ep.PartitionID, 0)
	if conn, err = gConnPool.GetConnect(readHost); err != nil {
		return
	}
	defer gConnPool.PutConnect(conn, true)
	if err = packet.WriteToConn(conn, proto.WriteDeadlineTime); err != nil {
		return
	}
	if err = packet.ReadFromConn(conn, proto.ReadDeadlineTime); err != nil {
		return
	}
	if packet.IsErrPacket() {
		logContent := fmt.Sprintf("action[doStreamFixTinyDeleteRecord] %v.",
			packet.LogMessage(packet.GetOpMsg(), conn.RemoteAddr().String(), time.Now().Unix(), fmt.Errorf(string(packet.Data[:packet.Size]))))
		err = fmt.Errorf(logContent)
		return
	}
	if packet.CRC != crc32.ChecksumIEEE(packet.Data[:packet.Size]) {
		err = fmt.Errorf("crc not match")
		return
	}
	tinyDeleteRecord := make([]*ecstorage.EcTinyDeleteRecord, 0)
	err = json.Unmarshal(packet.Data, &tinyDeleteRecord)
	if err != nil {
		return
	}
	log.LogDebugf(" MaxTinyDelCount(%v) packetSize(%v) host(%v) repairDelCount(%v)",
		maxTinyDelCount, packet.Size, readHost, len(tinyDeleteRecord))
	deleteStatus := ecstorage.TinyDeleteMark
	for _, recordInfo := range tinyDeleteRecord {
		log.LogDebugf("handleRepairTinyDel Delete hostIndex(%v) PartitionID(%v)_Extent(%v)_Offset(%v)_Size(%v)",
			recordInfo.HostIndex, ep.PartitionID, recordInfo.ExtentID, recordInfo.Offset, recordInfo.Size)
		if recordInfo.DeleteStatus != ecstorage.TinyDeleting {
			deleteStatus = recordInfo.DeleteStatus
		}
		err = ep.ExtentStore().EcRecordTinyDelete(recordInfo.ExtentID, recordInfo.Offset, recordInfo.Size, deleteStatus, recordInfo.HostIndex)
		if err != nil {
			log.LogDebugf("handleRepairTinyDel Delete hostIndex(%v) PartitionID(%v)_Extent(%v)_Offset(%v)_Size(%v) err(%v)",
				recordInfo.HostIndex, ep.PartitionID, recordInfo.ExtentID, recordInfo.Offset, recordInfo.Size, err)
			return
		}
	}

	return
}

func (ep *EcPartition) notifyRepairTinyDelInfo(needRepairTinyDelInfo *repairTinyDelInfo, IsOriginTinyDeleteInfoRepair bool) (err error) {
	for _, host := range needRepairTinyDelInfo.needRepairHosts {
		if host == ep.Hosts[0] && !IsOriginTinyDeleteInfoRepair {
			err = ep.repairTinyDelete(needRepairTinyDelInfo.normalHost, needRepairTinyDelInfo.maxTinyDelCount)
			return
		}
		repairData := &tinyFileRepairData{
			MaxTinyDelCount: needRepairTinyDelInfo.maxTinyDelCount,
			NormalHost:      needRepairTinyDelInfo.normalHost,
		}
		var ctx context.Context
		request := repl.NewPacket(ctx)
		request.PartitionID = ep.PartitionID
		request.Data, err = json.Marshal(repairData)
		request.Size = uint32(len(request.Data))
		if err != nil {
			log.LogErrorf("extentData marshal error[%v]", err)
			return
		}
		request.CRC = crc32.ChecksumIEEE(request.Data)
		if IsOriginTinyDeleteInfoRepair {
			request.Opcode = proto.OpNotifyEcRepairOriginTinyDelInfo
		} else {
			request.Opcode = proto.OpNotifyEcRepairTinyDelInfo
		}
		log.LogDebugf("start repair partition(%v) opcode(%v) node(%v)", ep.PartitionID, request.Opcode, host)
		request.ReqID = proto.GenerateRequestID()
		err = DoRequest(request, host, proto.ReadDeadlineTime, ep.ecNode)
		// if err not nil, do nothing, only record log
		if err != nil {
			log.LogWarnf("notifyRepairTinyDelFile fail. PartitionID(%v) node(%v) resultCode(%v) err:%v",
				ep.PartitionID, host, request.ResultCode, err)
			return
		}
		if request.ResultCode != proto.OpOk {
			log.LogWarnf("notifyRepairTinyDelFile fail. PartitionID(%v) node(%v) resultCode(%v) err:%v",
				ep.PartitionID, host, request.ResultCode, err)
			err = errors.NewErrorf("notifyRepairTinyDelFile resultCode[%v]", request.ResultCode)
			return
		}
	}
	return
}

func (ep *EcPartition) checkOriginTinyDelInfoRepair(extentsTinyDelRecordList []*holesInfo) (err error) {
	var (
		needRepairOriginTinyDel *repairTinyDelInfo
	)
	needRepairOriginTinyDel, err = ep.getNeedRepairOriginTinyDel(extentsTinyDelRecordList)
	if err != nil {
		return
	}

	needRepairTinyDelHosts := len(needRepairOriginTinyDel.needRepairHosts)
	if needRepairTinyDelHosts == 0 || needRepairTinyDelHosts > int(ep.EcParityNum) {
		return
	}
	err = ep.notifyRepairTinyDelInfo(needRepairOriginTinyDel, true)
	return
}

func (ep *EcPartition) checkTinyDelInfoRepair(extentsTinyDelRecordList []*holesInfo) (err error) {
	var (
		needRepairTinyDel *repairTinyDelInfo
	)
	needRepairTinyDel, err = ep.getNeedRepairTinyDel(extentsTinyDelRecordList)
	if err != nil {
		return
	}

	needRepairTinyDelFileHosts := len(needRepairTinyDel.needRepairHosts)
	if needRepairTinyDelFileHosts == 0 || needRepairTinyDelFileHosts > int(ep.EcParityNum) {
		return
	}
	err = ep.notifyRepairTinyDelInfo(needRepairTinyDel, false)
	return
}

func (ep *EcPartition) repairPartitionData(fetchReplicas bool) {
	var (
		err                      error
		needRepairExtents        map[uint64]*repairExtentInfo
		needCreateExtents        map[uint64]*createExtentInfo
		extentsTinyDelRecordList []*holesInfo
	)
	if ep.partitionStatus == proto.Unavailable {
		log.LogErrorf("partition[%v] status Unavailable", ep.PartitionID)
		return
	}
	if fetchReplicas {
		if err = ep.updateEcInfo(); err != nil { //don't handle err wait next schedule
			log.LogDebugf("action[updateEcHosts] partition(%v) err(%v).", ep.PartitionID, err)
		}
		if !proto.IsEcFinished(ep.ecMigrateStatus) {
			return
		}
	}

	if ep.ecNode.localServerAddr != ep.Hosts[0] { //just host[0] handle repair
		return
	}

	needCreateExtents, needRepairExtents, extentsTinyDelRecordList, err = ep.getNeedRepairExtents(ep.ecNode)
	if err != nil {
		log.LogWarnf("getNeedRepairExtents partition(%v) err[%v], wait next schedule", ep.PartitionID, err)
		return
	}

	err = ep.checkOriginTinyDelInfoRepair(extentsTinyDelRecordList)
	if err != nil {
		log.LogWarnf("checkOriginTinyDelFileRepair partition(%v) err(%v)", ep.PartitionID, err)
		return
	}

	for extentId, createInfo := range needCreateExtents {
		err = ep.repairCreateExtent(extentId, createInfo)
		if err != nil {
			log.LogWarnf("partition[%v] repairCreateExtent[%v] err[%v], wait next schedule", ep.PartitionID, extentId, err)
			continue
		}
	}

	for extentId, repairInfo := range needRepairExtents {
		err = ep.repairExtentData(extentId, repairInfo, proto.DataRepair)
		if err != nil {
			log.LogWarnf("partition(%v) repairExtent(%v) err(%v), wait next schedule", ep.PartitionID, extentId, err)
		}
	}

	err = ep.checkTinyDelInfoRepair(extentsTinyDelRecordList)
	if err != nil {
		log.LogWarnf("checkTinyDelInfoRepair err(%v)", err)
	}

}

func (ep *EcPartition) EvictExpiredFileDescriptor() {
	ep.extentStore.EvictExpiredCache()
}

func (ep *EcPartition) ForceEvictFileDescriptor(ratio ecstorage.Ratio) {
	ep.extentStore.ForceEvictCache(ratio)
}

func (ep *EcPartition) getTinyExtentOffset(extentId, oriOffset uint64) (offset uint64) {
	ep.originTinyExtentDeleteMapLock.RLock()
	defer ep.originTinyExtentDeleteMapLock.RUnlock()
	offset = oriOffset
	if holes, exist := ep.originTinyExtentDeleteMap[extentId]; exist {
		for _, hole := range holes {
			if hole.Offset > oriOffset {
				break
			}
			offset = oriOffset - hole.PreAllSize
		}
	}
	return
}

func (ep *EcPartition) deleteOriginExtentSize(extentId uint64) (err error) {
	store := ep.ExtentStore()
	ep.originExtentSizeMap.Delete(extentId)
	err = store.DeleteMapInfo(ecstorage.OriginExtentSizeMapTable, extentId)
	return
}

func (ep *EcPartition) persistOriginExtentSize(extentId, extentSize uint64) (err error) {
	store := ep.ExtentStore()
	data, err := json.Marshal(extentSize)
	if err != nil {
		return
	}
	ep.originExtentSizeMap.Store(extentId, extentSize)
	err = store.PersistMapInfo(ecstorage.OriginExtentSizeMapTable, extentId, data)
	return
}

func (ep *EcPartition) persistOriginTinyDelInfo(extentId uint64, holes []*proto.TinyExtentHole) (err error) {
	store := ep.ExtentStore()
	data, err := json.Marshal(holes)
	if err != nil {
		return
	}
	ep.originTinyExtentDeleteMapLock.Lock()
	ep.originTinyExtentDeleteMap[extentId] = holes
	ep.originTinyExtentDeleteMapLock.Unlock()
	err = store.PersistMapInfo(ecstorage.OriginTinyDeleteMapTable, extentId, data)
	return
}

func (ep *EcPartition) GetOriginExtentSize(extentId uint64) (originExtentSize uint64, err error) {
	if value, ok := ep.originExtentSizeMap.Load(extentId); ok {
		originExtentSize = value.(uint64)
	}
	//calc stripeUnitSize
	if originExtentSize == 0 {
		err = errors.NewErrorf("originExtentSize is zero, partition[%v] extentId[%v]",
			ep.PartitionID, extentId)
		return
	}
	return
}
