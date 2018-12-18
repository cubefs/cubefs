package datanode

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/storage"
)

func (s *DataNode) apiGetDisk(w http.ResponseWriter, r *http.Request) {
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
			RestSize:    diskItem.RestSize,
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
	s.buildApiSuccessResp(w, diskReport)
}

func (s *DataNode) apiGetStat(w http.ResponseWriter, r *http.Request) {
	response := &proto.DataNodeHeartBeatResponse{}
	s.fillHeartBeatResponse(response)
	s.buildApiSuccessResp(w, response)
}

func (s *DataNode) apiGetPartitions(w http.ResponseWriter, r *http.Request) {
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
			Replicas: dp.ReplicaHosts(),
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
	s.buildApiSuccessResp(w, result)
}

func (s *DataNode) apiGetPartition(w http.ResponseWriter, r *http.Request) {
	const (
		paramPartitionId = "id"
	)
	var (
		partitionId uint64
		files       []*storage.FileInfo
		err         error
	)
	if err = r.ParseForm(); err != nil {
		err = fmt.Errorf("parse form fail: %v", err)
		s.buildApiFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if partitionId, err = strconv.ParseUint(r.FormValue(paramPartitionId), 10, 64); err != nil {
		err = fmt.Errorf("parse param %v fail: %v", paramPartitionId, err)
		s.buildApiFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	partition := s.space.GetPartition(partitionId)
	if partition == nil {
		s.buildApiFailureResp(w, http.StatusNotFound, "partition not exist")
		return
	}
	if files, err = partition.GetStore().GetAllWatermark(nil); err != nil {
		err = fmt.Errorf("get watermark fail: %v", err)
		s.buildApiFailureResp(w, http.StatusInternalServerError, err.Error())
		return
	}
	result := &struct {
		ID        uint64              `json:"id"`
		Size      int                 `json:"size"`
		Used      int                 `json:"used"`
		Status    int                 `json:"status"`
		Path      string              `json:"path"`
		Files     []*storage.FileInfo `json:"extents"`
		FileCount int                 `json:"fileCount"`
		Replicas  []string            `json:"replicas"`
	}{
		ID:        partition.ID(),
		Size:      partition.Size(),
		Used:      partition.Used(),
		Status:    partition.Status(),
		Path:      partition.Path(),
		Files:     files,
		FileCount: len(files),
		Replicas:  partition.ReplicaHosts(),
	}
	s.buildApiSuccessResp(w, result)
}

func (s *DataNode) apiGetExtent(w http.ResponseWriter, r *http.Request) {
	var (
		partitionId uint64
		extentId    int
		reload      int
		err         error
		extentInfo  *storage.FileInfo
	)
	if err = r.ParseForm(); err != nil {
		s.buildApiFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if partitionId, err = strconv.ParseUint(r.FormValue("partitionId"), 10, 64); err != nil {
		s.buildApiFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if extentId, err = strconv.Atoi(r.FormValue("extentId")); err != nil {
		s.buildApiFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	reload, _ = strconv.Atoi(r.FormValue("reload"))

	partition := s.space.GetPartition(partitionId)
	if partition == nil {
		s.buildApiFailureResp(w, http.StatusNotFound, "partition not exist")
		return
	}
	if extentInfo, err = partition.GetStore().GetWatermark(uint64(extentId), reload == 1); err != nil {
		s.buildApiFailureResp(w, 500, err.Error())
		return
	}

	s.buildApiSuccessResp(w, extentInfo)
	return
}

func (s *DataNode) buildApiSuccessResp(w http.ResponseWriter, data interface{}) {
	s.buildApiJsonResp(w, http.StatusOK, data, "")
}

func (s *DataNode) buildApiFailureResp(w http.ResponseWriter, code int, msg string) {
	s.buildApiJsonResp(w, code, nil, msg)
}

func (s *DataNode) buildApiJsonResp(w http.ResponseWriter, code int, data interface{}, msg string) {
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
