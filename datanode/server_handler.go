// Copyright 2018 The CubeFS Authors.
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

package datanode

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/cubefs/cubefs/cmd/common"
	"github.com/cubefs/cubefs/datanode/storage"
	"github.com/cubefs/cubefs/depends/tiglabs/raft"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/log"
)

var parseArgs = common.ParseArguments

var AutoRepairStatus = true

func (s *DataNode) getDiskAPI(w http.ResponseWriter, r *http.Request) {
	disks := make([]interface{}, 0)
	for _, diskItem := range s.space.GetDisks() {
		disk := &struct {
			Path         string `json:"path"`
			Total        uint64 `json:"total"`
			Used         uint64 `json:"used"`
			Available    uint64 `json:"available"`
			Unallocated  uint64 `json:"unallocated"`
			Allocated    uint64 `json:"allocated"`
			Status       int    `json:"status"`
			RestSize     uint64 `json:"restSize"`
			DiskRdoSize  uint64 `json:"diskRdoSize"`
			Partitions   int    `json:"partitions"`
			Decommission bool   `json:"decommission"`
		}{
			Path:         diskItem.Path,
			Total:        diskItem.Total,
			Used:         diskItem.Used,
			Available:    diskItem.Available,
			Unallocated:  diskItem.Unallocated,
			Allocated:    diskItem.Allocated,
			Status:       diskItem.Status,
			RestSize:     diskItem.ReservedSpace,
			DiskRdoSize:  diskItem.DiskRdonlySpace,
			Partitions:   diskItem.PartitionCount(),
			Decommission: diskItem.GetDecommissionStatus(),
		}
		disks = append(disks, disk)
	}
	diskReport := &struct {
		Disks []interface{} `json:"disks"`
		Zone  string        `json:"zone"`
	}{
		Disks: disks,
		Zone:  s.zoneName,
	}
	s.buildSuccessResp(w, diskReport)
}

func (s *DataNode) getStatAPI(w http.ResponseWriter, r *http.Request) {
	response := &proto.DataNodeHeartbeatResponse{}
	forbiddenVols := make(map[string]struct{})
	volDpRepairBlockSizes := make(map[string]uint64)

	s.buildHeartBeatResponse(response, forbiddenVols, volDpRepairBlockSizes, "")

	s.buildSuccessResp(w, response)
}

func (s *DataNode) setAutoRepairStatus(w http.ResponseWriter, r *http.Request) {
	var autoRepair common.Bool
	if err := parseArgs(r, autoRepair.Key("autoRepair")); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	AutoRepairStatus = autoRepair.V
	s.buildSuccessResp(w, autoRepair.V)
}

func (s *DataNode) getRaftStatus(w http.ResponseWriter, r *http.Request) {
	var raftID common.Uint
	if err := parseArgs(r, raftID.Key("raftID")); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	raftStatus := s.raftStore.RaftStatus(raftID.V)
	s.buildSuccessResp(w, raftStatus)
}

func (s *DataNode) getPartitionsAPI(w http.ResponseWriter, r *http.Request) {
	partitions := make([]interface{}, 0)
	var lock sync.Mutex
	s.space.RangePartitions(func(dp *DataPartition, testID string) bool {
		partition := &struct {
			ID       uint64   `json:"id"`
			Size     int      `json:"size"`
			Used     int      `json:"used"`
			Status   int      `json:"status"`
			Path     string   `json:"path"`
			Replicas []string `json:"replicas"`
			Hosts    []string `json:"hosts"`
		}{
			ID:       dp.partitionID,
			Size:     dp.Size(),
			Used:     dp.Used(),
			Status:   dp.Status(),
			Path:     dp.Path(),
			Replicas: dp.Replicas(),
			Hosts:    dp.getConfigHosts(),
		}
		lock.Lock()
		partitions = append(partitions, partition)
		lock.Unlock()
		return true
	}, "")
	result := &struct {
		Partitions     []interface{} `json:"partitions"`
		PartitionCount int           `json:"partitionCount"`
	}{
		Partitions:     partitions,
		PartitionCount: len(partitions),
	}
	s.buildSuccessResp(w, result)
}

func (s *DataNode) getPartitionAPI(w http.ResponseWriter, r *http.Request) {
	var (
		pid                  common.Uint
		files                []*storage.ExtentInfo
		err                  error
		tinyDeleteRecordSize int64
		raftSt               *raft.Status
	)
	if err := parseArgs(r, pid.ID()); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	partition := s.space.Partition(pid.V)
	if partition == nil {
		s.buildFailureResp(w, http.StatusNotFound, "partition not exist")
		return
	}
	if files, tinyDeleteRecordSize, err = partition.ExtentStore().GetAllWatermarks(nil); err != nil {
		err = fmt.Errorf("get watermark fail: %v", err)
		s.buildFailureResp(w, http.StatusInternalServerError, err.Error())
		return
	}

	if !partition.IsDataPartitionLoadFin() {
		raftSt = &raft.Status{Stopped: true}
	} else {
		raftSt = partition.raftPartition.Status()
	}

	result := &struct {
		VolName              string                `json:"volName"`
		ID                   uint64                `json:"id"`
		Size                 int                   `json:"size"`
		Used                 int                   `json:"used"`
		Status               int                   `json:"status"`
		Path                 string                `json:"path"`
		Files                []*storage.ExtentInfo `json:"extents"`
		FileCount            int                   `json:"fileCount"`
		Replicas             []string              `json:"replicas"`
		TinyDeleteRecordSize int64                 `json:"tinyDeleteRecordSize"`
		RaftStatus           *raft.Status          `json:"raftStatus"`
	}{
		VolName:              partition.volumeID,
		ID:                   partition.partitionID,
		Size:                 partition.Size(),
		Used:                 partition.Used(),
		Status:               partition.Status(),
		Path:                 partition.Path(),
		Files:                files,
		FileCount:            len(files),
		Replicas:             partition.Replicas(),
		TinyDeleteRecordSize: tinyDeleteRecordSize,
		RaftStatus:           raftSt,
	}

	if partition.isNormalType() && partition.raftPartition != nil {
		result.RaftStatus = partition.raftPartition.Status()
	}

	s.buildSuccessResp(w, result)
}

func (s *DataNode) getExtentAPI(w http.ResponseWriter, r *http.Request) {
	var (
		pid        common.Uint
		eid        common.Int
		err        error
		extentInfo *storage.ExtentInfo
	)
	if err = parseArgs(r, pid.PartitionID(), eid.ExtentID()); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	partition := s.space.Partition(pid.V)
	if partition == nil {
		s.buildFailureResp(w, http.StatusNotFound, "partition not exist")
		return
	}
	if extentInfo, err = partition.ExtentStore().Watermark(uint64(eid.V)); err != nil {
		s.buildFailureResp(w, 500, err.Error())
		return
	}

	s.buildSuccessResp(w, extentInfo)
}

func (s *DataNode) getBlockCrcAPI(w http.ResponseWriter, r *http.Request) {
	var (
		pid    common.Uint
		eid    common.Int
		err    error
		blocks []*storage.BlockCrc
	)
	if err = parseArgs(r, pid.PartitionID(), eid.ExtentID()); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	partition := s.space.Partition(pid.V)
	if partition == nil {
		s.buildFailureResp(w, http.StatusNotFound, "partition not exist")
		return
	}
	if blocks, err = partition.ExtentStore().ScanBlocks(uint64(eid.V)); err != nil {
		s.buildFailureResp(w, 500, err.Error())
		return
	}
	s.buildSuccessResp(w, blocks)
}

func (s *DataNode) getTinyDeleted(w http.ResponseWriter, r *http.Request) {
	var (
		pid        common.Uint
		err        error
		extentInfo []storage.ExtentDeleted
	)
	if err = parseArgs(r, pid.ID()); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	partition := s.space.Partition(pid.V)
	if partition == nil {
		s.buildFailureResp(w, http.StatusNotFound, "partition not exist")
		return
	}
	if extentInfo, err = partition.ExtentStore().GetHasDeleteTinyRecords(); err != nil {
		s.buildFailureResp(w, 500, err.Error())
		return
	}
	s.buildSuccessResp(w, extentInfo)
}

func (s *DataNode) getNormalDeleted(w http.ResponseWriter, r *http.Request) {
	var (
		pid        common.Uint
		err        error
		extentInfo []storage.ExtentDeleted
	)
	if err = parseArgs(r, pid.ID()); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	partition := s.space.Partition(pid.V)
	if partition == nil {
		s.buildFailureResp(w, http.StatusNotFound, "partition not exist")
		return
	}
	if extentInfo, err = partition.ExtentStore().GetHasDeleteExtent(); err != nil {
		s.buildFailureResp(w, 500, err.Error())
		return
	}
	s.buildSuccessResp(w, extentInfo)
}

func (s *DataNode) setQosEnable() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		var enable common.Bool
		if err := parseArgs(r, enable.Enable()); err != nil {
			s.buildFailureResp(w, http.StatusBadRequest, err.Error())
			return
		}
		s.diskQosEnable = enable.V
		s.buildSuccessResp(w, "success")
	}
}

func (s *DataNode) setDiskQos(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	parser := func(key string) (val int, err error, has bool) {
		valStr := r.FormValue(key)
		if valStr == "" {
			return 0, nil, false
		}
		has = true
		val, err = strconv.Atoi(valStr)
		return
	}

	updated := false
	for key, pVal := range map[string]*int{
		ConfigDiskReadIocc:   &s.diskReadIocc,
		ConfigDiskReadIops:   &s.diskReadIops,
		ConfigDiskReadFlow:   &s.diskReadFlow,
		ConfigDiskWriteIocc:  &s.diskWriteIocc,
		ConfigDiskWriteIops:  &s.diskWriteIops,
		ConfigDiskWriteFlow:  &s.diskWriteFlow,
		ConfigDiskWQueFactor: &s.diskWQueFactor,
	} {
		val, err, has := parser(key)
		if err != nil {
			s.buildFailureResp(w, http.StatusBadRequest, err.Error())
			return
		}
		if has {
			updated = true
			*pVal = val
		}
	}

	if updated {
		s.updateQosLimit()
	}
	s.buildSuccessResp(w, "success")
}

func (s *DataNode) getDiskQos(w http.ResponseWriter, r *http.Request) {
	disks := make([]interface{}, 0)
	for _, diskItem := range s.space.GetDisks() {
		disk := &struct {
			Path  string             `json:"path"`
			Read  util.LimiterStatus `json:"read"`
			Write util.LimiterStatus `json:"write"`
		}{
			Path:  diskItem.Path,
			Read:  diskItem.limitRead.Status(),
			Write: diskItem.limitWrite.Status(),
		}
		disks = append(disks, disk)
	}
	diskStatus := &struct {
		Disks []interface{} `json:"disks"`
		Zone  string        `json:"zone"`
	}{
		Disks: disks,
		Zone:  s.zoneName,
	}
	s.buildSuccessResp(w, diskStatus)
}

func (s *DataNode) getSmuxPoolStat() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.enableSmuxConnPool {
			s.buildFailureResp(w, 500, "smux pool not supported")
			return
		}
		if s.smuxConnPool == nil {
			s.buildFailureResp(w, 500, "smux pool now is nil")
			return
		}
		stat := s.smuxConnPool.GetStat()
		s.buildSuccessResp(w, stat)
	}
}

func (s *DataNode) setMetricsDegrade(w http.ResponseWriter, r *http.Request) {
	key := "level"
	var level common.Int
	if err := parseArgs(r, level.Key(key).OmitEmpty().
		OnError(func() error {
			w.Write([]byte("Set metrics degrade level parse failed\n"))
			return nil
		}).
		OnValue(func() error {
			atomic.StoreInt64(&s.metricsDegrade, level.V)
			w.Write([]byte(fmt.Sprintf("Set metrics degrade level to %d successfully\n", level.V)))
			return nil
		})); err != nil {
		w.Write([]byte(err.Error()))
		return
	}
}

func (s *DataNode) getMetricsDegrade(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte(fmt.Sprintf("%v\n", atomic.LoadInt64(&s.metricsDegrade))))
}

func (s *DataNode) genClusterVersionFile(w http.ResponseWriter, r *http.Request) {
	paths := make([]string, 0)
	s.space.RangePartitions(func(partition *DataPartition, testID string) bool {
		paths = append(paths, partition.disk.Path)
		return true
	}, "")
	paths = append(paths, s.raftDir)

	for _, p := range paths {
		if _, err := os.Stat(path.Join(p, config.ClusterVersionFile)); err == nil || os.IsExist(err) {
			s.buildFailureResp(w, http.StatusCreated, "cluster version file already exists in "+p)
			return
		}
	}
	for _, p := range paths {
		if err := config.CheckOrStoreClusterUuid(p, s.clusterUuid, true); err != nil {
			s.buildFailureResp(w, http.StatusInternalServerError, "Failed to create cluster version file in "+p)
			return
		}
	}
	s.buildSuccessResp(w, "Generate cluster version file success")
}

func (s *DataNode) buildSuccessResp(w http.ResponseWriter, data interface{}) {
	s.buildJSONResp(w, http.StatusOK, data, "")
}

func (s *DataNode) buildFailureResp(w http.ResponseWriter, code int, msg string) {
	s.buildJSONResp(w, code, nil, msg)
}

// Create response for the API request.
func (s *DataNode) buildJSONResp(w http.ResponseWriter, code int, data interface{}, msg string) {
	var (
		jsonBody []byte
		err      error
	)
	w.WriteHeader(code)
	w.Header().Set("Content-Type", "application/json")
	body := proto.HTTPReply{Code: int32(code), Msg: msg, Data: data}
	if jsonBody, err = json.Marshal(body); err != nil {
		return
	}
	w.Write(jsonBody)
}

type GcExtent struct {
	*storage.ExtentInfo
	GcStatus string `json:"gc_status"`
}

func (s *DataNode) getAllExtent(w http.ResponseWriter, r *http.Request) {
	var (
		partitionID uint64
		err         error
		extents     []*storage.ExtentInfo
	)
	if err = r.ParseForm(); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}

	beforeTime, err := strconv.ParseInt(r.FormValue("beforeTime"), 10, 64)
	if err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}

	if partitionID, err = strconv.ParseUint(r.FormValue("id"), 10, 64); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	partition := s.space.Partition(partitionID)
	if partition == nil {
		s.buildFailureResp(w, http.StatusNotFound, "partition not exist")
		return
	}

	store := partition.ExtentStore()
	extents, err = store.GetAllExtents(beforeTime)
	if err != nil {
		s.buildFailureResp(w, http.StatusInternalServerError, "get all extents failed")
		return
	}

	gcExtents := make([]*GcExtent, len(extents))
	for idx, e := range extents {
		gcExtents[idx] = &GcExtent{
			ExtentInfo: e,
			GcStatus:   store.GetGcFlag(e.FileID).String(),
		}
	}

	s.buildSuccessResp(w, gcExtents)
}

func (s *DataNode) setDiskBadAPI(w http.ResponseWriter, r *http.Request) {
	var (
		err      error
		diskPath common.String
		disk     *Disk
	)
	if err = parseArgs(r, diskPath.DiskPath()); err != nil {
		log.LogErrorf("[setDiskBadAPI] %v", err.Error())
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}

	if disk, err = s.space.GetDisk(diskPath.V); err != nil {
		err = fmt.Errorf("not exit such dissk, path: %v", diskPath.V)
		log.LogErrorf("[setDiskBadAPI] %v", err.Error())
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}

	if disk.Status == proto.Unavailable {
		msg := fmt.Sprintf("disk(%v) status was already unavailable, nothing to do", disk.Path)
		log.LogInfof("[setDiskBadAPI] %v", msg)
		s.buildSuccessResp(w, msg)
		return
	}

	log.LogWarnf("[setDiskBadAPI] set bad disk, path: %v", disk.Path)
	disk.doDiskError()

	s.buildSuccessResp(w, "OK")
}

func (s *DataNode) reloadDataPartition(w http.ResponseWriter, r *http.Request) {
	if !s.checkAllDiskLoaded() {
		s.buildFailureResp(w, http.StatusBadRequest, "please wait for disk loading")
		return
	}
	var pid common.Uint
	if err := parseArgs(r, pid.ID()); err != nil {
		err = fmt.Errorf("parse form fail: %v", err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	partitionID := pid.V

	partition := s.space.Partition(partitionID)
	if partition == nil {
		s.buildFailureResp(w, http.StatusNotFound, "partition not exist")
		return
	}
	err := partition.reload(s.space)
	if err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
	} else {
		s.buildSuccessResp(w, "success")
	}
}

// NOTE: use for test
func (s *DataNode) markDataPartitionBroken(w http.ResponseWriter, r *http.Request) {
	const (
		paramID     = "id"
		paramStatus = "status"
	)
	if err := r.ParseForm(); err != nil {
		err = fmt.Errorf("parse form fail: %v", err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	partitionID, err := strconv.ParseUint(r.FormValue(paramID), 10, 64)
	if err != nil {
		err = fmt.Errorf("parse param %v fail: %v", paramID, err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	status, err := strconv.ParseBool(r.FormValue(paramStatus))
	if err != nil {
		err = fmt.Errorf("parse param %v fail: %v", paramStatus, err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	partition := s.space.Partition(partitionID)
	if partition == nil {
		s.buildFailureResp(w, http.StatusBadRequest, "partition not found")
		return
	}
	if !status {
		partition.checkIsDiskError(syscall.EIO, WriteFlag|ReadFlag)
	} else {
		partition.StartRaft(true)
		partition.resetDiskErrCnt()
		partition.statusUpdate()
	}

	s.buildSuccessResp(w, "success")
}

func (s *DataNode) markDiskBroken(w http.ResponseWriter, r *http.Request) {
	const (
		paramDisk = "disk"
	)
	if err := r.ParseForm(); err != nil {
		err = fmt.Errorf("parse form fail: %v", err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	diskPath := r.FormValue(paramDisk)
	// store disk path and root of dp
	disk, err := s.space.GetDisk(diskPath)
	if err != nil {
		log.LogErrorf("[markDiskBroken] disk(%v) is not found err(%v).", diskPath, err)
		s.buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("disk %v is not found", diskPath))
		return
	}
	dps := disk.DataPartitionList()
	for _, dpId := range dps {
		partition := s.space.Partition(dpId)
		if partition == nil {
			log.LogErrorf("[markDiskBroken] dp(%v) not found", dpId)
			s.buildFailureResp(w, http.StatusBadRequest, "partition not found")
			return
		}
		partition.checkIsDiskError(syscall.EIO, WriteFlag|ReadFlag)
	}
	s.buildSuccessResp(w, "success")
}

func (s *DataNode) setDiskExtentReadLimitStatus(w http.ResponseWriter, r *http.Request) {
	const (
		paramStatus = "status"
	)
	if err := r.ParseForm(); err != nil {
		err = fmt.Errorf("parse form fail: %v", err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	status, err := strconv.ParseBool(r.FormValue(paramStatus))
	if err != nil {
		err = fmt.Errorf("parse param %v fail: %v", paramStatus, err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	for _, disk := range s.space.disks {
		disk.SetExtentRepairReadLimitStatus(status)
	}
	s.buildSuccessResp(w, "success")
}

type DiskExtentReadLimitInfo struct {
	DiskPath              string `json:"diskPath"`
	ExtentReadLimitStatus bool   `json:"extentReadLimitStatus"`
	Dp                    uint64 `json:"dp"`
}

type DiskExtentReadLimitStatusResponse struct {
	Infos []DiskExtentReadLimitInfo `json:"infos"`
}

func (s *DataNode) queryDiskExtentReadLimitStatus(w http.ResponseWriter, r *http.Request) {
	resp := &DiskExtentReadLimitStatusResponse{}
	for _, disk := range s.space.disks {
		status, dp := disk.QueryExtentRepairReadLimitStatus()
		resp.Infos = append(resp.Infos, DiskExtentReadLimitInfo{DiskPath: disk.Path, ExtentReadLimitStatus: status, Dp: dp})
	}
	s.buildSuccessResp(w, resp)
}

func (s *DataNode) detachDataPartition(w http.ResponseWriter, r *http.Request) {
	const (
		paramID = "id"
	)
	if err := r.ParseForm(); err != nil {
		err = fmt.Errorf("parse form fail: %v", err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	partitionID, err := strconv.ParseUint(r.FormValue(paramID), 10, 64)
	if err != nil {
		err = fmt.Errorf("parse param %v fail: %v", paramID, err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	partition := s.space.Partition(partitionID)
	if partition == nil {
		s.buildFailureResp(w, http.StatusBadRequest, "partition not exist")
		return
	}
	// store disk path and root of dp
	disk := partition.disk
	rootDir := partition.path
	log.LogDebugf("data partition disk %v rootDir %v", disk, rootDir)

	s.space.partitionMutex.Lock()
	delete(s.space.partitions, partitionID)
	s.space.partitionMutex.Unlock()
	partition.Stop()
	partition.Disk().DetachDataPartition(partition)

	log.LogDebugf("data partition %v is detached", partitionID)
	s.buildSuccessResp(w, "success")
}

func (s *DataNode) releaseDiskExtentReadLimitToken(w http.ResponseWriter, r *http.Request) {
	const (
		paramDisk = "disk"
	)
	if err := r.ParseForm(); err != nil {
		err = fmt.Errorf("parse form fail: %v", err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	diskPath := r.FormValue(paramDisk)
	// store disk path and root of dp
	disk, err := s.space.GetDisk(diskPath)
	if err != nil {
		log.LogErrorf("action[loadDataPartition] disk(%v) is not found err(%v).", diskPath, err)
		s.buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("disk %v is not found", diskPath))
		return
	}
	disk.ReleaseReadExtentToken()
	s.buildSuccessResp(w, "success")
}

func (s *DataNode) loadDataPartition(w http.ResponseWriter, r *http.Request) {
	const (
		paramID   = "id"
		paramDisk = "disk"
	)
	if err := r.ParseForm(); err != nil {
		err = fmt.Errorf("parse form fail: %v", err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	partitionID, err := strconv.ParseUint(r.FormValue(paramID), 10, 64)
	if err != nil {
		err = fmt.Errorf("parse param %v fail: %v", paramID, err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	partition := s.space.Partition(partitionID)
	if partition != nil {
		s.buildFailureResp(w, http.StatusBadRequest, "partition is already loaded")
		return
	}
	diskPath := r.FormValue(paramDisk)
	// store disk path and root of dp
	disk, err := s.space.GetDisk(diskPath)
	if err != nil {
		log.LogErrorf("action[loadDataPartition] disk(%v) is not found err(%v).", diskPath, err)
		s.buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("disk %v is not found", diskPath))
		return
	}

	fileInfoList, err := os.ReadDir(disk.Path)
	if err != nil {
		log.LogErrorf("action[loadDataPartition] read dir(%v) err(%v).", disk.Path, err)
		s.buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf(" read dir(%v) err(%v)", disk.Path, err))
		return
	}
	rootDir := ""
	for _, fileInfo := range fileInfoList {
		filename := fileInfo.Name()
		if !disk.isPartitionDir(filename) {
			continue
		}

		if id, _, err := unmarshalPartitionName(filename); err != nil {
			log.LogErrorf("action[RestorePartition] unmarshal partitionName(%v) from disk(%v) err(%v) ",
				filename, disk.Path, err.Error())
			continue
		} else {
			if id == partitionID {
				rootDir = filename
			}
		}
	}
	if rootDir == "" {
		log.LogErrorf("action[loadDataPartition] dp root not found in dir(%v) .", disk.Path)
		s.buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("dp root not found in dir(%v)", disk.Path))
		return
	}

	log.LogDebugf("data partition disk %v rootDir %v", disk, rootDir)

	_, err = LoadDataPartition(path.Join(diskPath, rootDir), disk)
	if err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
	} else {
		s.buildSuccessResp(w, "success")
	}
}

func (s *DataNode) getRaftPeers(w http.ResponseWriter, r *http.Request) {
	const (
		paramRaftID = "id"
	)
	if err := r.ParseForm(); err != nil {
		err = fmt.Errorf("parse form fail: %v", err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	raftID, err := strconv.ParseUint(r.FormValue(paramRaftID), 10, 64)
	if err != nil {
		err = fmt.Errorf("parse param %v fail: %v", paramRaftID, err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	raftPeers := s.raftStore.GetPeers(raftID)
	s.buildSuccessResp(w, raftPeers)
}
