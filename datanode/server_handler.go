// Copyright 2018 The Container File System Authors.
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
	"strconv"

	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/storage"
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
		}
		disks = append(disks, disk)
	}
	diskReport := &struct {
		Disks []interface{} `json:"disks"`
		Rack  string        `json:"rack"`
	}{
		Disks: disks,
		Rack:  s.rackName,
	}
	s.buildSuccessResp(w, diskReport)
}

func (s *DataNode) getStatAPI(w http.ResponseWriter, r *http.Request) {

	// TODO there should be a better way to initialize a heartbeat response
	response := &proto.DataNodeHeartbeatResponse{}
	s.buildHeartBeatResponse(response)

	s.buildSuccessResp(w, response)
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
			ID:       dp.ID(),
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
		partitionID uint64
		files       []*storage.ExtentInfo
		err         error
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
	if files, err = partition.ExtentStore().GetAllWatermarks(nil); err != nil {
		err = fmt.Errorf("get watermark fail: %v", err)
		s.buildFailureResp(w, http.StatusInternalServerError, err.Error())
		return
	}
	result := &struct {
		ID        uint64                `json:"id"`
		Size      int                   `json:"size"`
		Used      int                   `json:"used"`
		Status    int                   `json:"status"`
		Path      string                `json:"path"`
		Files     []*storage.ExtentInfo `json:"extents"`
		FileCount int                   `json:"fileCount"`
		Replicas  []string              `json:"replicas"`
	}{
		ID:        partition.ID(),
		Size:      partition.Size(),
		Used:      partition.Used(),
		Status:    partition.Status(),
		Path:      partition.Path(),
		Files:     files,
		FileCount: len(files),
		Replicas:  partition.Replicas(),
	}
	s.buildSuccessResp(w, result)
}

func (s *DataNode) getExtentAPI(w http.ResponseWriter, r *http.Request) {
	var (
		partitionID uint64
		extentID    int
		reload      int
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
	reload, _ = strconv.Atoi(r.FormValue("reload"))

	partition := s.space.Partition(partitionID)
	if partition == nil {
		s.buildFailureResp(w, http.StatusNotFound, "partition not exist")
		return
	}
	if extentInfo, err = partition.ExtentStore().Watermark(uint64(extentID), reload == 1); err != nil {
		s.buildFailureResp(w, 500, err.Error())
		return
	}

	s.buildSuccessResp(w, extentInfo)
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
