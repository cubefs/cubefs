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
	"github.com/cubefs/cubefs/util/config"
	"net/http"
	"os"
	"path"
	"strconv"
	"sync/atomic"

	"github.com/cubefs/cubefs/depends/tiglabs/raft"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/storage"
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
			DiskRdoSize uint64 `json:"diskRdoSize"`
			Partitions  int    `json:"partitions"`
		}{
			Path:        diskItem.Path,
			Total:       diskItem.Total,
			Used:        diskItem.Used,
			Available:   diskItem.Available,
			Unallocated: diskItem.Unallocated,
			Allocated:   diskItem.Allocated,
			Status:      diskItem.Status,
			RestSize:    diskItem.ReservedSpace,
			DiskRdoSize: diskItem.DiskRdonlySpace,
			Partitions:  diskItem.PartitionCount(),
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
			Replicas: dp.Replicas(),
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

func (s *DataNode) getPartitionAPI(w http.ResponseWriter, r *http.Request) {
	const (
		paramPartitionID = "id"
	)
	var (
		partitionID          uint64
		files                []*storage.ExtentInfo
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
	if files, tinyDeleteRecordSize, err = partition.ExtentStore().GetAllWatermarks(nil); err != nil {
		err = fmt.Errorf("get watermark fail: %v", err)
		s.buildFailureResp(w, http.StatusInternalServerError, err.Error())
		return
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
	}

	if partition.isNormalType() {
		result.RaftStatus = partition.raftPartition.Status()
	}

	s.buildSuccessResp(w, result)
}

func (s *DataNode) getExtentAPI(w http.ResponseWriter, r *http.Request) {
	var (
		partitionID uint64
		extentID    int
		err         error
		extentInfo  *storage.ExtentInfo
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

func (s *DataNode) getTinyDeleted(w http.ResponseWriter, r *http.Request) {
	var (
		partitionID uint64
		err         error
		extentInfo  []storage.ExtentDeleted
	)
	if err = r.ParseForm(); err != nil {
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
	if extentInfo, err = partition.ExtentStore().GetHasDeleteTinyRecords(); err != nil {
		s.buildFailureResp(w, 500, err.Error())
		return
	}

	s.buildSuccessResp(w, extentInfo)
	return
}

func (s *DataNode) getNormalDeleted(w http.ResponseWriter, r *http.Request) {
	var (
		partitionID uint64
		err         error
		extentInfo  []storage.ExtentDeleted
	)
	if err = r.ParseForm(); err != nil {
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
	if extentInfo, err = partition.ExtentStore().GetHasDeleteExtent(); err != nil {
		s.buildFailureResp(w, 500, err.Error())
		return
	}

	s.buildSuccessResp(w, extentInfo)
	return
}

func (s *DataNode) setQosEnable() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		var (
			err    error
			enable bool
		)
		if err = r.ParseForm(); err != nil {
			s.buildFailureResp(w, http.StatusBadRequest, err.Error())
			return
		}
		if enable, err = strconv.ParseBool(r.FormValue("enable")); err != nil {
			s.buildFailureResp(w, http.StatusBadRequest, err.Error())
			return
		}
		s.diskQosEnable = enable
		s.buildSuccessResp(w, "success")
	}
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
		return
	}
}

func (s *DataNode) setMetricsDegrade(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	if level := r.FormValue("level"); level != "" {
		val, err := strconv.Atoi(level)
		if err != nil {
			w.Write([]byte("Set metrics degrade level failed\n"))
		} else {
			atomic.StoreInt64(&s.metricsDegrade, int64(val))
			w.Write([]byte(fmt.Sprintf("Set metrics degrade level to %v successfully\n", val)))
		}
	}
}

func (s *DataNode) getMetricsDegrade(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte(fmt.Sprintf("%v\n", atomic.LoadInt64(&s.metricsDegrade))))
}

func (s *DataNode) genClusterVersionFile(w http.ResponseWriter, r *http.Request) {
	paths := make([]string, 0)
	s.space.RangePartitions(func(partition *DataPartition) bool {
		paths = append(paths, partition.disk.Path)
		return true
	})
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
