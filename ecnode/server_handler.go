// Copyright 2020 The CubeFS Authors.
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
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/exporter"
	"github.com/chubaofs/chubaofs/util/unit"
	"hash/crc32"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/chubaofs/chubaofs/ecstorage"
	"github.com/chubaofs/chubaofs/util/log"
)

func (e *EcNode) getPartitionsAPI(w http.ResponseWriter, r *http.Request) {
	log.LogDebugf("action[getPartitionsAPI]")

	partitions := make([]interface{}, 0)
	e.space.RangePartitions(func(ep *EcPartition) bool {
		partitions = append(partitions, ep.EcPartitionMetaData)
		return true
	})

	result := &struct {
		Partitions     []interface{} `json:"partitions"`
		PartitionCount int           `json:"partitionCount"`
	}{
		Partitions:     partitions,
		PartitionCount: len(partitions),
	}
	e.buildSuccessResp(w, result)
	return
}

func (e *EcNode) getExtentAPI(w http.ResponseWriter, r *http.Request) {
	var (
		partitionID uint64
		extentID    int
		err         error
		extentInfo  *ecstorage.ExtentInfo
	)
	if err = r.ParseForm(); err != nil {
		e.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if partitionID, err = strconv.ParseUint(r.FormValue("partitionID"), 10, 64); err != nil {
		e.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if extentID, err = strconv.Atoi(r.FormValue("extentID")); err != nil {
		e.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	partition := e.space.Partition(partitionID)
	if partition == nil {
		e.buildFailureResp(w, http.StatusNotFound, "partition not exist")
		return
	}
	if extentInfo, err = partition.ExtentStore().EcWatermark(uint64(extentID)); err != nil {
		e.buildFailureResp(w, 500, err.Error())
		return
	}

	e.buildSuccessResp(w, extentInfo)
	return
}

func (e *EcNode) getStatAPI(w http.ResponseWriter, r *http.Request) {
	response := &proto.EcNodeHeartbeatResponse{}
	e.buildHeartbeatResponse(response)

	e.buildSuccessResp(w, response)
	return
}

func (e *EcNode) releasePartitions(w http.ResponseWriter, r *http.Request) {
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
		e.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	key := r.FormValue(paramAuthKey)
	if !matchKey(key) {
		err := fmt.Errorf("auth key not match: %v", key)
		e.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}

	for _, d := range e.space.disks {
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
	e.buildSuccessResp(w, fmt.Sprintf("release partitions, success partitions: %v, failed partitions: %v, failed disks: %v", successVols, failedVols, failedDisks))
}

func (e *EcNode) getAllExtents(w http.ResponseWriter, r *http.Request) {
	var (
		partitionID uint64
		err         error
	)
	if err = r.ParseForm(); err != nil {
		e.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if partitionID, err = strconv.ParseUint(r.FormValue("partitionID"), 10, 64); err != nil {
		e.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}

	partition := e.space.Partition(partitionID)
	if partition == nil {
		e.buildFailureResp(w, http.StatusNotFound, "partition not exist")
		return
	}
	extents, _, err := partition.extentStore.GetAllWatermarks(ecstorage.EcExtentFilter())
	if err != nil {
		e.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	e.buildSuccessResp(w, extents)
}

func (e *EcNode) getTinyDelInfo(w http.ResponseWriter, r *http.Request) {
	var (
		partitionID uint64
		extentID    uint64
		err         error
	)
	if err = r.ParseForm(); err != nil {
		e.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if partitionID, err = strconv.ParseUint(r.FormValue("partitionID"), 10, 64); err != nil {
		e.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}

	ep := e.space.Partition(partitionID)
	if ep == nil {
		e.buildFailureResp(w, http.StatusNotFound, "partition not exist")
		return
	}

	if extentID, err = strconv.ParseUint(r.FormValue("extentID"), 10, 64); err != nil {
		e.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}

	delInfos := make([]*proto.TinyDelInfo, 0)
	ep.extentStore.TinyDelInfoRange(ecstorage.BaseDeleteMark, func(extentId, offset, size uint64, deleteStatus, hostIndex uint32) (err error) {
		if extentID == 0 || extentId == extentID {
			delInfo := &proto.TinyDelInfo{
				ExtentId:     extentId,
				DeleteStatus: deleteStatus,
				Offset:       offset,
				Size:         size,
				HostIndex:    hostIndex,
			}
			delInfos = append(delInfos, delInfo)
		}
		return
	})

	e.buildSuccessResp(w, delInfos)
}

func (e *EcNode) checkExtentChunkData(w http.ResponseWriter, r *http.Request) {
	var (
		partitionID uint64
		extentID    uint64
		err         error
	)
	if err = r.ParseForm(); err != nil {
		e.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if partitionID, err = strconv.ParseUint(r.FormValue("partitionID"), 10, 64); err != nil {
		e.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}

	if extentID, err = strconv.ParseUint(r.FormValue("extentID"), 10, 64); err != nil {
		e.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}

	partition := e.space.Partition(partitionID)
	if partition == nil {
		e.buildFailureResp(w, http.StatusBadRequest, "partition not exist")
		return
	}
	err = checkExtentChunkData(partition, extentID)
	if err != nil {
		e.buildFailureResp(w, http.StatusNotFound, err.Error())
	}

	e.buildSuccessResp(w, "")
}

func (e *EcNode) getExtentHosts(w http.ResponseWriter, r *http.Request) {
	var (
		partitionID uint64
		extentID    int
		err         error
	)
	if err = r.ParseForm(); err != nil {
		e.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if partitionID, err = strconv.ParseUint(r.FormValue("partitionID"), 10, 64); err != nil {
		e.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if extentID, err = strconv.Atoi(r.FormValue("extentID")); err != nil {
		e.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	partition := e.space.Partition(partitionID)
	if partition == nil {
		e.buildFailureResp(w, http.StatusNotFound, "partition not exist")
		return
	}
	nodeNum := partition.EcParityNum + partition.EcDataNum
	hosts := proto.GetEcHostsByExtentId(uint64(nodeNum), uint64(extentID), partition.Hosts)
	resp := fmt.Sprintf("hosts:[%v]", hosts)
	e.buildSuccessResp(w, resp)
}

func checkExtentParityData(ecStripe *ecStripe, ep *EcPartition, stripeUnitOffset, needReadSize uint64, isTinyRead bool) (err error) {
	var (
		stripeData [][]byte
		parityData [][]byte
	)
	stripeData, err = ecStripe.readStripeData(stripeUnitOffset, needReadSize, proto.ScrubRepair)
	if err != nil {
		return err
	}
	parityData = make([][]byte, ep.EcParityNum)
	for i := 0; i < int(ep.EcParityNum); i++ {
		parityData[i] = make([]byte, needReadSize)
		copy(parityData[i], stripeData[int(ep.EcDataNum)+i])
		stripeData[int(ep.EcDataNum)+i] = nil
	}
	err = ecStripe.verifyAndReconstructData(stripeData, needReadSize)
	if err != nil {
		log.LogErrorf("verifyAndReconstructData err(%v)", err)
	}
	for i := 0; i < int(ep.EcParityNum); i++ {
		if crc32.ChecksumIEEE(parityData[i]) != crc32.ChecksumIEEE(stripeData[int(ep.EcDataNum)+i]) {
			err = errors.NewErrorf("partition(%v) extent(%v) stripe data not consistent", ep.PartitionID, ecStripe.extentID)
			exporter.Warning(err.Error())
			break
		}
	}
	return
}

func checkExtentChunkData(ep *EcPartition, extentId uint64) (err error) {
	extent, err := ep.extentStore.EcWatermark(extentId)
	if err != nil {
		return
	}
	stripeUnitSize := proto.CalStripeUnitSize(extent.OriginExtentSize, ep.EcMaxUnitSize, uint64(ep.EcDataNum))
	ecStripe, _ := NewEcStripe(ep, stripeUnitSize, extent.FileID)
	size := uint64(0)
	for {
		if size >= extent.Size {
			break
		}
		curSize := unit.Min(int(extent.Size), unit.EcBlockSize)
		err = checkExtentParityData(ecStripe, ep, size, uint64(curSize), ecstorage.IsTinyExtent(extent.FileID))
		if err != nil {
			log.LogErrorf("checkExtentChunkData extent(%v) err(%v)", extent.FileID, err)
			return
		}
		size += uint64(curSize)
	}
	log.LogDebugf("checkExtentChunkData partition(%v) extent(%v) ", ep.PartitionID, extent.FileID)
	return
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

func (e *EcNode) buildSuccessResp(w http.ResponseWriter, data interface{}) {
	err := Response(w, http.StatusOK, data, "")
	if err != nil {
		log.LogErrorf("response fail. error:%v, data:%v", err, data)
	}
}

func (e *EcNode) buildFailureResp(w http.ResponseWriter, code int, msg string) {
	err := Response(w, code, nil, msg)
	if err != nil {
		log.LogErrorf("response fail. error:%v, code:%v msg:%v", err, code, msg)
	}
}

func (e *EcNode) getExtentCrc(w http.ResponseWriter, r *http.Request) {
	var (
		partitionID    uint64
		extentID       uint64
		stripeCount    uint64
		crc            uint64
		stripeUnitSize uint64
		err            error
	)
	if err = r.ParseForm(); err != nil {
		e.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if partitionID, err = strconv.ParseUint(r.FormValue("partitionId"), 10, 64); err != nil {
		e.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}

	if extentID, err = strconv.ParseUint(r.FormValue("extentId"), 10, 64); err != nil {
		e.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}

	if stripeCount, err = strconv.ParseUint(r.FormValue("stripeCount"), 10, 64); err != nil {
		e.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}

	if crc, err = strconv.ParseUint(r.FormValue("crc"), 10, 64); err != nil {
		e.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}

	log.LogDebugf("getExtentCrc partitionID(%v) extentID(%v) stripeCount(%v) crc(%v)",
		partitionID, extentID, stripeCount, crc)
	partition := e.space.Partition(partitionID)
	if partition == nil {
		e.buildFailureResp(w, http.StatusBadRequest, "partition not exist")
		return
	}
	store := partition.ExtentStore()
	originExtentSize, err := partition.GetOriginExtentSize(extentID)
	if err != nil {
		log.LogErrorf("GetOriginExtentSize err(%v)", err)
		e.buildFailureResp(w, http.StatusInternalServerError, err.Error())
		return
	}
	stripeUnitSize = proto.CalStripeUnitSize(originExtentSize, partition.EcMaxUnitSize, uint64(partition.EcDataNum))
	stripeUnitFileSize, err := partition.getExtentStripeUnitFileSize(extentID, originExtentSize, e.localServerAddr)
	if err != nil {
		log.LogErrorf("getExtentStripeUnitFileSize err(%v)", err)
		e.buildFailureResp(w, http.StatusInternalServerError, err.Error())
		return
	}
	crcResp, err := store.GetExtentCrcResponse(extentID, stripeCount, stripeUnitSize, stripeUnitFileSize, uint32(crc))
	if err != nil {
		log.LogErrorf("GetExtentCrc err(%v)", err)
		e.buildFailureResp(w, http.StatusInternalServerError, err.Error())
		return
	}

	e.buildSuccessResp(w, crcResp)
}

func (e *EcNode) setMaxTinyDelCount(w http.ResponseWriter, r *http.Request) {
	var (
		maxTinyDelCount uint64
		err             error
	)
	if err = r.ParseForm(); err != nil {
		e.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if maxTinyDelCount, err = strconv.ParseUint(r.FormValue("maxTinyDelCount"), 10, 64); err != nil {
		e.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}

	log.LogDebugf("setMaxTinyDelCount maxTinyDelCount(%v)", maxTinyDelCount)
	e.maxTinyDelCount = uint8(maxTinyDelCount)
	e.buildSuccessResp(w, "")
}

func (e *EcNode) setEcPartitionSize(w http.ResponseWriter, r *http.Request) {
	var (
		size        uint64
		partitionId uint64
		err         error
	)
	if err = r.ParseForm(); err != nil {
		e.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}

	if partitionId, err = strconv.ParseUint(r.FormValue("partitionId"), 10, 64); err != nil {
		e.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}

	if size, err = strconv.ParseUint(r.FormValue("size"), 10, 64); err != nil {
		e.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}

	log.LogDebugf("setEcPartition(%v) Size(%v)", partitionId, size)
	ep := e.space.Partition(partitionId)
	if ep == nil {
		err = errors.NewErrorf("no this ec partition(%v)", partitionId)
		e.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}

	if size < ep.Used() {
		err = errors.NewErrorf("partition(%v) setSize < used", partitionId)
		e.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}

	ep.PartitionSize = size
	currentPath := path.Clean(ep.path)
	newPath := path.Join(path.Dir(currentPath), fmt.Sprintf(EcPartitionPrefix+"_%v_%v", partitionId, size))
	err = os.Rename(currentPath, newPath)
	if err != nil {
		e.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	ep.path = newPath
	err = ep.PersistMetaData()
	if err != nil {
		e.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	e.buildSuccessResp(w, "success")
}
