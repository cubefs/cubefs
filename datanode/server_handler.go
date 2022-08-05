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

package datanode

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/chubaofs/chubaofs/util/log"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/chubaofs/chubaofs/repl"
	"github.com/chubaofs/chubaofs/util"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/storage"
	"github.com/tiglabs/raft"
)

var (
	AutoRepairStatus = true
)

func (s *DataNode) getDiskAPI(w http.ResponseWriter, r *http.Request) {
	disks := make([]interface{}, 0)
	for _, diskItem := range s.space.GetDisks() {
		disk := &struct {
			Path        string `json:"path"`
			Total       uint64 `json:"total"`
			Used        uint64 `json:"used"`
			Available   uint64 `json:"available"`
			Unallocated uint64 `json:"unallocated"`
			Allocated   uint64 `json:"allocated"`
			Status      int    `json:"status"`
			RestSize    uint64 `json:"restSize"`
			Partitions  int    `json:"partitions"`

			// Limits
			RepairTaskLimit              uint64 `json:"repair_task_limit"`
			ExecutingRepairTask          uint64 `json:"executing_repair_task"`
			FixTinyDeleteRecordLimit     uint64 `json:"fix_tiny_delete_record_limit"`
			ExecutingFixTinyDeleteRecord uint64 `json:"executing_fix_tiny_delete_record"`
		}{
			Path:        diskItem.Path,
			Total:       diskItem.Total,
			Used:        diskItem.Used,
			Available:   diskItem.Available,
			Unallocated: diskItem.Unallocated,
			Allocated:   diskItem.Allocated,
			Status:      diskItem.Status,
			RestSize:    diskItem.ReservedSpace,
			Partitions:  diskItem.PartitionCount(),

			FixTinyDeleteRecordLimit:     diskItem.fixTinyDeleteRecordLimit,
			ExecutingFixTinyDeleteRecord: diskItem.executingFixTinyDeleteRecord,
			RepairTaskLimit:              diskItem.repairTaskLimit,
			ExecutingRepairTask:          diskItem.executingRepairTask,
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
	s.buildHeartBeatResponse(response)

	s.buildSuccessResp(w, response)
}

func (s *DataNode) setAutoRepairStatus(w http.ResponseWriter, r *http.Request) {
	const (
		paramAutoRepair = "autoRepair"
	)
	if err := r.ParseForm(); err != nil {
		err = fmt.Errorf("parse form fail: %v", err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	autoRepair, err := strconv.ParseBool(r.FormValue(paramAutoRepair))
	if err != nil {
		err = fmt.Errorf("parse param %v fail: %v", paramAutoRepair, err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	AutoRepairStatus = autoRepair
	s.buildSuccessResp(w, autoRepair)
}

/*
 * release the space of mark delete data partitions of the whole node.
 */
func (s *DataNode) releasePartitions(w http.ResponseWriter, r *http.Request) {
	const (
		paramAuthKey = "key"
	)
	var (
		successVols []string
		failedVols  []string
		failedDisks []string
	)
	successVols = make([]string, 0)
	failedVols = make([]string, 0)
	failedDisks = make([]string, 0)
	if err := r.ParseForm(); err != nil {
		err = fmt.Errorf("parse form fail: %v", err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	key := r.FormValue(paramAuthKey)
	if !matchKey(key) {
		err := fmt.Errorf("auth key not match: %v", key)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}

	for _, d := range s.space.disks {
		fList, err := ioutil.ReadDir(d.Path)
		if err != nil {
			failedDisks = append(failedDisks, d.Path)
			continue
		}

		for _, fInfo := range fList {
			if !fInfo.IsDir() {
				continue
			}
			if !strings.HasPrefix(fInfo.Name(), "expired_") {
				continue
			}
			err = os.RemoveAll(d.Path + "/" + fInfo.Name())
			if err != nil {
				failedVols = append(failedVols, d.Path+":"+fInfo.Name())
				continue
			}
			successVols = append(successVols, d.Path+":"+fInfo.Name())
		}
	}
	s.buildSuccessResp(w, fmt.Sprintf("release partitions, success partitions: %v, failed partitions: %v, failed disks: %v", successVols, failedVols, failedDisks))
}

func (s *DataNode) getRaftStatus(w http.ResponseWriter, r *http.Request) {
	const (
		paramRaftID = "raftID"
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
	raftStatus := s.raftStore.RaftStatus(raftID)
	s.buildSuccessResp(w, raftStatus)
}

func (s *DataNode) getPartitionsAPI(w http.ResponseWriter, r *http.Request) {
	partitions := make([]interface{}, 0)
	s.space.RangePartitions(func(dp *DataPartition) bool {
		partition := &struct {
			ID       uint64   `json:"id"`
			Size     int      `json:"size"`
			Used     int      `json:"used"`
			Status   int      `json:"status"`
			Path     string   `json:"path"`
			Replicas []string `json:"replicas"`
		}{
			ID:       dp.partitionID,
			Size:     dp.Size(),
			Used:     dp.Used(),
			Status:   dp.Status(),
			Path:     dp.Path(),
			Replicas: dp.getReplicaClone(),
		}
		partitions = append(partitions, partition)
		return true
	})
	result := &struct {
		Partitions     []interface{} `json:"partitions"`
		PartitionCount int           `json:"partitionCount"`
	}{
		Partitions:     partitions,
		PartitionCount: len(partitions),
	}
	s.buildSuccessResp(w, result)
}

func (s *DataNode) getExtentMd5Sum(w http.ResponseWriter, r *http.Request) {
	const (
		paramPartitionID = "id"
		paramExtentID    = "extent"
		paramOffset      = "offset"
		paramSize        = "size"
	)
	var (
		err                                 error
		partitionID, extentID, offset, size uint64
		md5Sum                              string
	)
	if err = r.ParseForm(); err != nil {
		err = fmt.Errorf("parse form fail: %v", err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if partitionID, err = strconv.ParseUint(r.FormValue(paramPartitionID), 10, 64); err != nil {
		err = fmt.Errorf("parse param %v fail: %v", paramPartitionID, err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if extentID, err = strconv.ParseUint(r.FormValue(paramExtentID), 10, 64); err != nil {
		err = fmt.Errorf("parse param %v fail: %v", paramExtentID, err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if r.FormValue(paramOffset) != "" {
		if offset, err = strconv.ParseUint(r.FormValue(paramOffset), 10, 64); err != nil {
			err = fmt.Errorf("parse param %v fail: %v", paramOffset, err)
			s.buildFailureResp(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	if r.FormValue(paramSize) != "" {
		if size, err = strconv.ParseUint(r.FormValue(paramSize), 10, 64); err != nil {
			err = fmt.Errorf("parse param %v fail: %v", paramSize, err)
			s.buildFailureResp(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	partition := s.space.Partition(partitionID)
	if partition == nil {
		s.buildFailureResp(w, http.StatusNotFound, fmt.Sprintf("partition(%v) not exist", partitionID))
		return
	}
	exsit := partition.ExtentStore().HasExtent(extentID)
	if !exsit {
		s.buildFailureResp(w, http.StatusNotFound, fmt.Sprintf("partition(%v) extentID(%v) not exist", partitionID, extentID))
		return
	}
	md5Sum, err = partition.ExtentStore().ComputeMd5Sum(extentID, offset, size)
	if err != nil {
		s.buildFailureResp(w, http.StatusInternalServerError, fmt.Sprintf("partition(%v) extentID(%v) computeMD5 failed %v", partitionID, extentID, err))
		return
	}
	result := &struct {
		PartitionID uint64 `json:"PartitionID"`
		ExtentID    uint64 `json:"ExtentID"`
		Md5Sum      string `json:"md5"`
	}{
		PartitionID: partitionID,
		ExtentID:    extentID,
		Md5Sum:      md5Sum,
	}
	s.buildSuccessResp(w, result)
}

func (s *DataNode) getPartitionAPI(w http.ResponseWriter, r *http.Request) {
	const (
		paramPartitionID = "id"
	)
	var (
		partitionID          uint64
		files                []storage.ExtentInfoBlock
		err                  error
		tinyDeleteRecordSize int64
	)
	if err = r.ParseForm(); err != nil {
		err = fmt.Errorf("parse form fail: %v", err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if partitionID, err = strconv.ParseUint(r.FormValue(paramPartitionID), 10, 64); err != nil {
		err = fmt.Errorf("parse param %v fail: %v", paramPartitionID, err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	partition := s.space.Partition(partitionID)
	if partition == nil {
		s.buildFailureResp(w, http.StatusNotFound, "partition not exist")
		return
	}
	if files, tinyDeleteRecordSize, err = partition.ExtentStore().GetAllWatermarks(proto.AllExtentType, nil); err != nil {
		err = fmt.Errorf("get watermark fail: %v", err)
		s.buildFailureResp(w, http.StatusInternalServerError, err.Error())
		return
	}
	var raftStatus *raft.Status
	if partition.raftPartition != nil {
		raftStatus = partition.raftPartition.Status()
	}
	result := &struct {
		VolName              string                    `json:"volName"`
		ID                   uint64                    `json:"id"`
		Size                 int                       `json:"size"`
		Used                 int                       `json:"used"`
		Status               int                       `json:"status"`
		Path                 string                    `json:"path"`
		Files                []storage.ExtentInfoBlock `json:"extents"`
		FileCount            int                       `json:"fileCount"`
		Replicas             []string                  `json:"replicas"`
		Peers                []proto.Peer              `json:"peers"`
		Learners             []proto.Learner           `json:"learners"`
		TinyDeleteRecordSize int64                     `json:"tinyDeleteRecordSize"`
		RaftStatus           *raft.Status              `json:"raftStatus"`
		IsFinishLoad         bool                      `json:"isFinishLoad"`
	}{
		VolName:              partition.volumeID,
		ID:                   partition.partitionID,
		Size:                 partition.Size(),
		Used:                 partition.Used(),
		Status:               partition.Status(),
		Path:                 partition.Path(),
		Files:                files,
		FileCount:            len(files),
		Replicas:             partition.getReplicaClone(),
		TinyDeleteRecordSize: tinyDeleteRecordSize,
		RaftStatus:           raftStatus,
		Peers:                partition.config.Peers,
		Learners:             partition.config.Learners,
		IsFinishLoad:         partition.ExtentStore().IsFinishLoad(),
	}
	s.buildSuccessResp(w, result)
}

func (s *DataNode) getExtentAPI(w http.ResponseWriter, r *http.Request) {
	var (
		partitionID uint64
		extentID    int
		err         error
		extentInfo  *storage.ExtentInfoBlock
	)
	if err = r.ParseForm(); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if partitionID, err = strconv.ParseUint(r.FormValue("partitionID"), 10, 64); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if extentID, err = strconv.Atoi(r.FormValue("extentID")); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	partition := s.space.Partition(partitionID)
	if partition == nil {
		s.buildFailureResp(w, http.StatusNotFound, "partition not exist")
		return
	}
	if extentInfo, err = partition.ExtentStore().Watermark(uint64(extentID)); err != nil {
		s.buildFailureResp(w, 500, err.Error())
		return
	}

	s.buildSuccessResp(w, extentInfo)
	return
}

func (s *DataNode) getReplProtocalBufferDetail(w http.ResponseWriter, r *http.Request) {
	allReplDetail := repl.GetReplProtocolDetail()
	s.buildSuccessResp(w, allReplDetail)
	return
}

func (s *DataNode) getBlockCrcAPI(w http.ResponseWriter, r *http.Request) {
	var (
		partitionID uint64
		extentID    int
		err         error
		blocks      []*storage.BlockCrc
	)
	if err = r.ParseForm(); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if partitionID, err = strconv.ParseUint(r.FormValue("partitionID"), 10, 64); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if extentID, err = strconv.Atoi(r.FormValue("extentID")); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	partition := s.space.Partition(partitionID)
	if partition == nil {
		s.buildFailureResp(w, http.StatusNotFound, "partition not exist")
		return
	}
	if blocks, err = partition.ExtentStore().ScanBlocks(uint64(extentID)); err != nil {
		s.buildFailureResp(w, 500, err.Error())
		return
	}

	s.buildSuccessResp(w, blocks)
	return
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
	body := struct {
		Code int         `json:"code"`
		Data interface{} `json:"data"`
		Msg  string      `json:"msg"`
	}{
		Code: code,
		Data: data,
		Msg:  msg,
	}
	if jsonBody, err = json.Marshal(body); err != nil {
		return
	}
	w.Write(jsonBody)
}

func (s *DataNode) getStatInfo(w http.ResponseWriter, r *http.Request) {
	if s.processStatInfo == nil {
		s.buildFailureResp(w, http.StatusBadRequest, "data node is initializing")
		return
	}
	//get process stat info
	cpuUsageList, maxCPUUsage := s.processStatInfo.GetProcessCPUStatInfo()
	memoryUsedGBList, maxMemoryUsedGB, maxMemoryUsage := s.processStatInfo.GetProcessMemoryStatInfo()
	//get disk info
	disks := s.space.GetDisks()
	diskList := make([]interface{}, 0, len(disks))
	for _, disk := range disks {
		diskTotal, err := util.GetDiskTotal(disk.Path)
		if err != nil {
			diskTotal = disk.Total
		}
		diskInfo := &struct {
			Path          string  `json:"path"`
			TotalTB       float64 `json:"totalTB"`
			UsedGB        float64 `json:"usedGB"`
			UsedRatio     float64 `json:"usedRatio"`
			ReservedSpace uint    `json:"reservedSpaceGB"`
		}{
			Path:          disk.Path,
			TotalTB:       util.FixedPoint(float64(diskTotal)/util.TB, 1),
			UsedGB:        util.FixedPoint(float64(disk.Used)/util.GB, 1),
			UsedRatio:     util.FixedPoint(float64(disk.Used)/float64(diskTotal), 1),
			ReservedSpace: uint(disk.ReservedSpace / util.GB),
		}
		diskList = append(diskList, diskInfo)
	}
	result := &struct {
		Type           string        `json:"type"`
		Zone           string        `json:"zone"`
		Version        interface{}   `json:"versionInfo"`
		StartTime      string        `json:"startTime"`
		CPUUsageList   []float64     `json:"cpuUsageList"`
		MaxCPUUsage    float64       `json:"maxCPUUsage"`
		CPUCoreNumber  int           `json:"cpuCoreNumber"`
		MemoryUsedList []float64     `json:"memoryUsedGBList"`
		MaxMemoryUsed  float64       `json:"maxMemoryUsedGB"`
		MaxMemoryUsage float64       `json:"maxMemoryUsage"`
		DiskInfo       []interface{} `json:"diskInfo"`
	}{
		Type:           ModuleName,
		Zone:           s.zoneName,
		Version:        proto.MakeVersion("DataNode"),
		StartTime:      s.processStatInfo.ProcessStartTime,
		CPUUsageList:   cpuUsageList,
		MaxCPUUsage:    maxCPUUsage,
		CPUCoreNumber:  util.GetCPUCoreNumber(),
		MemoryUsedList: memoryUsedGBList,
		MaxMemoryUsed:  maxMemoryUsedGB,
		MaxMemoryUsage: maxMemoryUsage,
		DiskInfo:       diskList,
	}
	s.buildSuccessResp(w, result)
}

func matchKey(key string) bool {
	return key == generateAuthKey()
}

func generateAuthKey() string {
	date := time.Now().Format("2006-01-02 15")
	h := md5.New()
	h.Write([]byte(date))
	cipherStr := h.Sum(nil)
	return hex.EncodeToString(cipherStr)
}

func (s *DataNode) getTinyExtentHoleInfo(w http.ResponseWriter, r *http.Request) {
	var (
		partitionID uint64
		extentID    int
		err         error
	)
	if err = r.ParseForm(); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if partitionID, err = strconv.ParseUint(r.FormValue("partitionID"), 10, 64); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if extentID, err = strconv.Atoi(r.FormValue("extentID")); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	partition := s.space.Partition(partitionID)
	if partition == nil {
		s.buildFailureResp(w, http.StatusNotFound, "partition not exist")
		return
	}
	store := partition.ExtentStore()
	holes, extentAvaliSize, err := store.TinyExtentHolesAndAvaliSize(uint64(extentID), 0)
	if err != nil {
		s.buildFailureResp(w, http.StatusInternalServerError, err.Error())
		return
	}

	blks, _ := store.GetRealBlockCnt(uint64(extentID))
	result := &struct {
		Holes           []*proto.TinyExtentHole `json:"holes"`
		ExtentAvaliSize uint64                  `json:"extentAvaliSize"`
		ExtentBlocks    int64                   `json:"blockNum"`
	}{
		Holes:           holes,
		ExtentAvaliSize: extentAvaliSize,
		ExtentBlocks:    blks,
	}

	s.buildSuccessResp(w, result)
}

// 这个API用于回放指定Partition的TINYEXTENT_DELETE记录，为该Partition下的所有TINY EXTENT重新打洞
func (s *DataNode) playbackPartitionTinyDelete(w http.ResponseWriter, r *http.Request) {
	var (
		partitionID uint64
		err         error
	)
	if err = r.ParseForm(); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if partitionID, err = strconv.ParseUint(r.FormValue("partitionID"), 10, 64); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	partition := s.space.Partition(partitionID)
	if partition == nil {
		s.buildFailureResp(w, http.StatusNotFound, "partition not exist")
		return
	}
	store := partition.ExtentStore()
	if err = store.PlaybackTinyDelete(); err != nil {
		s.buildFailureResp(w, http.StatusInternalServerError, err.Error())
		return
	}
	s.buildSuccessResp(w, nil)
}

func (s *DataNode) stopPartitionById(partitionID uint64) (err error) {
	partition := s.space.Partition(partitionID)
	if partition == nil {
		err = fmt.Errorf("partition[%d] not exist", partitionID)
		return
	}
	partition.Disk().space.DetachDataPartition(partition.partitionID)
	partition.Disk().DetachDataPartition(partition)
	partition.Stop()
	return nil
}

func (s *DataNode) stopPartition(w http.ResponseWriter, r *http.Request) {
	var (
		partitionID uint64
		err         error
	)
	if err = r.ParseForm(); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if partitionID, err = strconv.ParseUint(r.FormValue("partitionID"), 10, 64); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}

	if err = s.stopPartitionById(partitionID); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}

	s.buildSuccessResp(w, nil)
}

func (s *DataNode) reloadPartitionByName(partitionPath, disk string) (err error) {
	var (
		d           *Disk
		partition   *DataPartition
		partitionID uint64
	)

	if d, err = s.space.GetDisk(disk); err != nil {
		return
	}

	if partitionID, _, err = unmarshalPartitionName(partitionPath); err != nil {
		err = fmt.Errorf("action[reloadPartition] unmarshal partitionName(%v) from disk(%v) err(%v) ",
			partitionPath, disk, err.Error())
		return
	}

	partition = s.space.Partition(partitionID)
	if partition != nil {
		err = fmt.Errorf("partition[%d] exist, can not reload", partitionID)
		return
	}

	if err = s.space.ReloadPartition(d, partitionID, partitionPath); err != nil {
		return
	}
	return
}

func (s *DataNode) reloadPartition(w http.ResponseWriter, r *http.Request) {
	var (
		partitionPath string
		disk          string
		err           error
	)
	if err = r.ParseForm(); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}

	if disk = r.FormValue("disk"); disk == "" {
		s.buildFailureResp(w, http.StatusBadRequest, "param disk is empty")
		return
	}

	if partitionPath = r.FormValue("partitionPath"); partitionPath == "" {
		s.buildFailureResp(w, http.StatusBadRequest, "param partitionPath is empty")
		return
	}

	if err = s.reloadPartitionByName(partitionPath, disk); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}

	s.buildSuccessResp(w, nil)
}

func (s *DataNode) moveExtentFileToBackup(pathStr string, partitionID, extentID uint64) (err error) {
	const (
		repairDirStr = "repair_extents_backup"
	)
	rootPath := pathStr[:strings.LastIndex(pathStr, "/")]
	extentFilePath := path.Join(pathStr, strconv.Itoa(int(extentID)))
	now := time.Now()
	repairDirPath := path.Join(rootPath, repairDirStr, fmt.Sprintf("%d-%d-%d", now.Year(), now.Month(), now.Day()))
	os.MkdirAll(repairDirPath, 0655)
	fileInfo, err := os.Stat(repairDirPath)
	if err != nil || !fileInfo.IsDir() {
		err = fmt.Errorf("path[%s] is not exist or is not dir", repairDirPath)
		return
	}
	repairBackupFilePath := path.Join(repairDirPath, fmt.Sprintf("%d_%d_%d", partitionID, extentID, now.Unix()))
	log.LogWarnf("rename file[%s-->%s]", extentFilePath, repairBackupFilePath)
	if err = os.Rename(extentFilePath, repairBackupFilePath); err != nil {
		return
	}

	file, err := os.OpenFile(extentFilePath, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0666)
	if err != nil {
		return
	}
	file.Close()
	return nil
}

func (s *DataNode) moveExtentFile(w http.ResponseWriter, r *http.Request) {
	const (
		paramPath        = "path"
		paramPartitionID = "partition"
		paramExtentID    = "extent"
	)
	var (
		err            error
		partitionID    uint64
		extentID       uint64
		pathStr        string
		partitionIDStr string
		extentIDStr    string
	)
	defer func() {
		if err != nil {
			s.buildFailureResp(w, http.StatusInternalServerError, err.Error())
			log.LogErrorf("move extent %s/%s failed:%s", pathStr, extentIDStr, err.Error())
		}
	}()

	pathStr = r.FormValue(paramPath)
	partitionIDStr = r.FormValue(paramPartitionID)
	extentIDStr = r.FormValue(paramExtentID)
	log.LogWarnf("move extent %s/%s begin", pathStr, extentIDStr)

	if len(partitionIDStr) == 0 || len(extentIDStr) == 0 || len(pathStr) == 0 {
		err = fmt.Errorf("need param [%s %s %s]", paramPath, paramPartitionID, paramExtentID)
		return
	}

	if partitionID, err = strconv.ParseUint(partitionIDStr, 10, 64); err != nil {
		err = fmt.Errorf("parse param %v fail: %v", paramPartitionID, err)
		return
	}
	if extentID, err = strconv.ParseUint(extentIDStr, 10, 64); err != nil {
		err = fmt.Errorf("parse param %v fail: %v", paramExtentID, err)
		return
	}

	if err = s.moveExtentFileToBackup(pathStr, partitionID, extentID); err != nil {
		return
	}

	log.LogWarnf("move extent %s/%s success", pathStr, extentIDStr)
	s.buildSuccessResp(w, nil)
	return
}

func (s *DataNode) moveExtentFileBatch(w http.ResponseWriter, r *http.Request) {
	const (
		paramPath        = "path"
		paramPartitionID = "partition"
		paramExtentID    = "extent"
	)
	var (
		err            error
		partitionID    uint64
		pathStr        string
		partitionIDStr string
		extentIDStr    string
		resultMap      map[uint64]string
	)
	defer func() {
		if err != nil {
			s.buildFailureResp(w, http.StatusInternalServerError, err.Error())
			log.LogWarnf("move extent batch partition:%s extents:%s failed:%s", pathStr, extentIDStr, err.Error())
		}
	}()

	resultMap = make(map[uint64]string)
	pathStr = r.FormValue(paramPath)
	partitionIDStr = r.FormValue(paramPartitionID)
	extentIDStr = r.FormValue(paramExtentID)
	extentIDArrayStr := strings.Split(extentIDStr, "-")
	log.LogWarnf("move extent batch partition:%s extents:%s begin", pathStr, extentIDStr)

	if len(partitionIDStr) == 0 || len(extentIDArrayStr) == 0 || len(pathStr) == 0 {
		err = fmt.Errorf("need param [%s %s %s]", paramPath, paramPartitionID, paramExtentID)
		return
	}

	if partitionID, err = strconv.ParseUint(partitionIDStr, 10, 64); err != nil {
		err = fmt.Errorf("parse param %v fail: %v", paramPartitionID, err)
		return
	}
	for _, idStr := range extentIDArrayStr {
		var ekId uint64
		if ekId, err = strconv.ParseUint(idStr, 10, 64); err != nil {
			err = fmt.Errorf("parse param %v fail: %v", paramExtentID, err)
			return
		}
		resultMap[ekId] = fmt.Sprintf("partition:%d extent:%d not start", partitionID, ekId)
	}

	for _, idStr := range extentIDArrayStr {
		ekId, _ := strconv.ParseUint(idStr, 10, 64)
		if err = s.moveExtentFileToBackup(pathStr, partitionID, ekId); err != nil {
			resultMap[ekId] = err.Error()
			log.LogErrorf("repair extent %s/%s failed", pathStr, idStr)
		} else {
			resultMap[ekId] = "OK"
			log.LogWarnf("repair extent %s/%s success", pathStr, idStr)
		}
	}
	err = nil
	log.LogWarnf("move extent batch partition:%s extents:%s success", pathStr, extentIDStr)
	s.buildSuccessResp(w, resultMap)
	return
}

func (s *DataNode) repairExtent(w http.ResponseWriter, r *http.Request) {
	const (
		paramPath        = "path"
		paramPartitionID = "partition"
		paramExtentID    = "extent"
	)
	var (
		err              error
		partitionID      uint64
		extentID         uint64
		hasStopPartition bool
		pathStr          string
		partitionIDStr   string
		extentIDStr      string
		lastSplitIndex   int
	)
	defer func() {
		if err != nil {
			s.buildFailureResp(w, http.StatusInternalServerError, err.Error())
			log.LogErrorf("repair extent batch partition: %s extents[%s] failed:%s", pathStr, extentIDStr, err.Error())
		}

		if hasStopPartition {
			s.reloadPartitionByName(pathStr[lastSplitIndex+1:], pathStr[:lastSplitIndex])
		}

	}()

	hasStopPartition = false
	pathStr = r.FormValue(paramPath)
	partitionIDStr = r.FormValue(paramPartitionID)
	extentIDStr = r.FormValue(paramExtentID)

	if len(partitionIDStr) == 0 || len(extentIDStr) == 0 || len(pathStr) == 0 {
		err = fmt.Errorf("need param [%s %s %s]", paramPath, paramPartitionID, paramExtentID)
		return
	}

	if partitionID, err = strconv.ParseUint(partitionIDStr, 10, 64); err != nil {
		err = fmt.Errorf("parse param %v fail: %v", paramPartitionID, err)
		return
	}
	if extentID, err = strconv.ParseUint(extentIDStr, 10, 64); err != nil {
		err = fmt.Errorf("parse param %v fail: %v", paramExtentID, err)
		return
	}
	lastSplitIndex = strings.LastIndex(pathStr, "/")
	log.LogWarnf("repair extent %s/%s begin", pathStr, extentIDStr)

	if err = s.stopPartitionById(partitionID); err != nil {
		return
	}
	hasStopPartition = true

	if err = s.moveExtentFileToBackup(pathStr, partitionID, extentID); err != nil {
		return
	}

	hasStopPartition = false
	if err = s.reloadPartitionByName(pathStr[lastSplitIndex+1:], pathStr[:lastSplitIndex]); err != nil {
		return
	}
	log.LogWarnf("repair extent %s/%s success", pathStr, extentIDStr)
	s.buildSuccessResp(w, nil)
	return
}

func (s *DataNode) repairExtentBatch(w http.ResponseWriter, r *http.Request) {
	const (
		paramPath        = "path"
		paramPartitionID = "partition"
		paramExtentID    = "extent"
	)
	var (
		err              error
		partitionID      uint64
		hasStopPartition bool
		pathStr          string
		partitionIDStr   string
		extentIDStr      string
		resultMap        map[uint64]string
		lastSplitIndex   int
	)
	defer func() {
		if err != nil {
			s.buildFailureResp(w, http.StatusInternalServerError, err.Error())
			log.LogErrorf("repair extent batch partition: %s extents[%s] failed:%s", pathStr, extentIDStr, err.Error())
		}

		if hasStopPartition {
			s.reloadPartitionByName(pathStr[lastSplitIndex+1:], pathStr[:lastSplitIndex])
		}
	}()

	resultMap = make(map[uint64]string)
	hasStopPartition = false
	pathStr = r.FormValue(paramPath)
	partitionIDStr = r.FormValue(paramPartitionID)
	extentIDStr = r.FormValue(paramExtentID)
	extentIDArrayStr := strings.Split(extentIDStr, "-")

	if len(partitionIDStr) == 0 || len(extentIDArrayStr) == 0 || len(pathStr) == 0 {
		err = fmt.Errorf("need param [%s %s %s]", paramPath, paramPartitionID, paramExtentID)
		return
	}

	if partitionID, err = strconv.ParseUint(partitionIDStr, 10, 64); err != nil {
		err = fmt.Errorf("parse param %v fail: %v", paramPartitionID, err)
		return
	}
	for _, idStr := range extentIDArrayStr {
		var ekId uint64
		if ekId, err = strconv.ParseUint(idStr, 10, 64); err != nil {
			err = fmt.Errorf("parse param %v fail: %v", paramExtentID, err)
			return
		}
		resultMap[ekId] = fmt.Sprintf("partition:%d extent:%d not start", partitionID, ekId)
	}
	lastSplitIndex = strings.LastIndex(pathStr, "/")
	log.LogWarnf("repair extent batch partition: %s extents[%s] begin", pathStr, extentIDStr)

	if err = s.stopPartitionById(partitionID); err != nil {
		return
	}
	hasStopPartition = true

	for _, idStr := range extentIDArrayStr {
		ekId, _ := strconv.ParseUint(idStr, 10, 64)
		if err = s.moveExtentFileToBackup(pathStr, partitionID, ekId); err != nil {
			resultMap[ekId] = err.Error()
			log.LogErrorf("repair extent %s/%s failed:%s", pathStr, idStr, err.Error())
		} else {
			resultMap[ekId] = "OK"
			log.LogWarnf("repair extent %s/%s success", pathStr, idStr)
		}
	}
	err = nil
	hasStopPartition = false
	if err = s.reloadPartitionByName(pathStr[lastSplitIndex+1:], pathStr[:lastSplitIndex]); err != nil {
		return
	}
	log.LogWarnf("repair extent batch partition: %s extents[%s] success", pathStr, extentIDStr)
	s.buildSuccessResp(w, resultMap)
	return
}


func (s *DataNode) getExtentCrc(w http.ResponseWriter, r *http.Request) {
	var (
		partitionID uint64
		extentID    uint64
		err         error
	)
	if err = r.ParseForm(); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if partitionID, err = strconv.ParseUint(r.FormValue("partitionId"), 10, 64); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if extentID, err = strconv.ParseUint(r.FormValue("extentId"), 10, 64); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	log.LogDebugf("getExtentCrc partitionID(%v) extentID(%v)", partitionID, extentID)
	partition := s.space.Partition(partitionID)
	if partition == nil {
		s.buildFailureResp(w, http.StatusNotFound, "partition not exist")
		return
	}
	store := partition.ExtentStore()
	crc, err := store.GetExtentCrc(extentID)
	if err != nil {
		log.LogErrorf("GetExtentCrc err(%v)", err)
		s.buildFailureResp(w, http.StatusInternalServerError, err.Error())
		return
	}

	result := &struct {
		CRC uint32
	}{
		CRC: crc,
	}
	s.buildSuccessResp(w, result)
}
