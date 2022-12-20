package http_client

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

type DataClient struct {
	sync.RWMutex
	useSSL bool
	host   string
}

// NewDataClient returns a new DataClient instance.
func NewDataClient(host string, useSSL bool) *DataClient {
	return &DataClient{host: host, useSSL: useSSL}
}

func (dc *DataClient) RequestHttp(method, path string, param map[string]string) (respData []byte, err error) {
	req := newAPIRequest(method, path)
	for k, v := range param {
		req.addParam(k, v)
	}
	return serveRequest(dc.useSSL, dc.host, req)
}

func (dc *DataClient) ComputeExtentMd5(partitionID, extentID, offset, size uint64) (md5 string, err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("action[ComputeExtentMd5],pid:%v, extent:%v, offset:%v, size:%v, err:%v", partitionID, extentID, offset, size, err)
		}
	}()
	var buf []byte
	params := make(map[string]string, 0)
	params["id"] = strconv.FormatUint(partitionID, 10)
	params["extent"] = strconv.FormatUint(extentID, 10)
	params["offset"] = strconv.FormatUint(offset, 10)
	params["size"] = strconv.FormatUint(size, 10)
	res := struct {
		PartitionID uint64 `json:"PartitionID"`
		ExtentID    uint64 `json:"ExtentID"`
		Md5Sum      string `json:"md5"`
	}{}
	buf, err = dc.RequestHttp(http.MethodGet, "/computeExtentMd5", params)
	if err = json.Unmarshal(buf, &res); err != nil {
		return
	}
	md5 = res.Md5Sum
	return
}

func (dc *DataClient) GetDisks() (diskInfo *proto.DataNodeDiskReport, err error) {
	var d []byte
	for i := 0; i < 3; i++ {
		d, err = dc.RequestHttp(http.MethodGet, "/disks", nil)
		if err == nil {
			break
		}
		time.Sleep(1 * time.Second)
	}
	if err != nil {
		return
	}
	diskInfo = new(proto.DataNodeDiskReport)
	if err = json.Unmarshal(d, diskInfo); err != nil {
		return
	}
	return
}

//DataNode api
func (dc *DataClient) GetPartitionsFromNode() (partitions *proto.DataPartitions, err error) {
	var d []byte
	for i := 0; i < 3; i++ {
		d, err = dc.RequestHttp(http.MethodGet, "/partitions", nil)
		if err == nil {
			break
		}
		time.Sleep(time.Second)
	}
	if err != nil {
		return
	}
	partitions = new(proto.DataPartitions)
	if err = json.Unmarshal(d, partitions); err != nil {
		return
	}
	return
}

func (dc *DataClient) GetPartitionFromNode(id uint64) (pInfo *proto.DNDataPartitionInfo, err error) {
	params := make(map[string]string)
	params["id"] = strconv.FormatUint(id, 10)
	var d []byte
	for i := 0; i < 3; i++ {
		d, err = dc.RequestHttp(http.MethodGet, "/partition", params)
		if err == nil {
			break
		}
		time.Sleep(time.Second)
	}
	if err != nil {
		return
	}
	pInfo = new(proto.DNDataPartitionInfo)
	pInfoOld := new(proto.DNDataPartitionInfoOldVersion)
	if err = json.Unmarshal(d, pInfoOld); err != nil {
		err = json.Unmarshal(d, pInfo)
		return
	}
	for _, ext := range pInfoOld.Files {
		extent := proto.ExtentInfoBlock{
			ext.FileID,
			ext.Size,
			uint64(ext.Crc),
			uint64(ext.ModifyTime),
		}
		pInfo.Files = append(pInfo.Files, extent)
	}
	pInfo.RaftStatus = pInfoOld.RaftStatus
	pInfo.Path = pInfoOld.Path
	pInfo.VolName = pInfoOld.VolName
	return
}

func (dc *DataClient) GetPartitionSimple(id uint64) (pInfo *proto.DNDataPartitionInfo, err error) {
	params := make(map[string]string)
	params["id"] = strconv.FormatUint(id, 10)
	var d []byte
	for i := 0; i < 3; i++ {
		d, err = dc.RequestHttp(http.MethodGet, "/partitionSimple", params)
		if err == nil {
			break
		}
		time.Sleep(time.Second)
	}
	if err != nil {
		return
	}
	pInfo = new(proto.DNDataPartitionInfo)
	if err = json.Unmarshal(d, pInfo); err != nil {
		return
	}
	return
}

func (dc *DataClient) GetExtentHoles(id uint64, eid uint64) (ehs *proto.DNTinyExtentInfo, err error) {
	params := make(map[string]string)
	params["partitionID"] = strconv.FormatUint(id, 10)
	params["extentID"] = strconv.FormatUint(eid, 10)
	var d []byte
	for i := 0; i < 3; i++ {
		d, err = dc.RequestHttp(http.MethodGet, "/tinyExtentHoleInfo", params)
		if err == nil {
			break
		}
		time.Sleep(time.Second)
	}
	if err != nil {
		return
	}
	ehs = new(proto.DNTinyExtentInfo)
	if err = json.Unmarshal(d, ehs); err != nil {
		return
	}
	return
}

func (dc *DataClient) StopPartition(pid uint64) (err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("action[StopPartition],pid:%v,err:%v", pid, err)
		}
		log.LogFlush()
	}()
	params := make(map[string]string)
	params["partitionID"] = fmt.Sprintf("%v", pid)
	_, err = dc.RequestHttp(http.MethodGet, "/stopPartition", params)
	log.LogInfof("action[StopPartition],pid:%v,:%v", pid, err)
	if err != nil {
		return
	}
	return
}

func (dc *DataClient) ReLoadPartition(partitionDirName, dirPath string) (err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("action[ReLoadPartition],pid:%v,err:%v", partitionDirName, err)
		}
		log.LogFlush()
	}()
	params := make(map[string]string, 0)
	params["partitionPath"] = partitionDirName
	params["disk"] = dirPath
	_, err = dc.RequestHttp(http.MethodGet, "/reloadPartition", params)
	log.LogInfof("action[ReLoadPartition],pid:%v,err:%v", partitionDirName, err)
	if err != nil {
		return
	}
	return
}

func (dc *DataClient) GetExtentBlockCrc(id uint64, eid uint64) (blocks []*proto.BlockCrc, err error) {
	var d []byte
	params := make(map[string]string)
	params["partitionID"] = strconv.FormatUint(id, 10)
	params["extentID"] = strconv.FormatUint(eid, 10)
	for i := 0; i < 3; i++ {
		d, err = dc.RequestHttp(http.MethodGet, "/block", params)
		if err == nil {
			break
		}
		if strings.Contains(err.Error(), "extent does not exist") {
			break
		}
		time.Sleep(1 * time.Second)
	}
	if err != nil {
		return
	}
	blocks = make([]*proto.BlockCrc, 0)
	if err = json.Unmarshal(d, &blocks); err != nil {
		return
	}
	return
}

func (dc *DataClient) GetExtentInfo(id uint64, eid uint64) (ehs *proto.ExtentInfoBlock, err error) {
	var d []byte
	params := make(map[string]string)
	params["partitionID"] = strconv.FormatUint(id, 10)
	params["extentID"] = strconv.FormatUint(eid, 10)
	for i := 0; i < 3; i++ {
		d, err = dc.RequestHttp(http.MethodGet, "/extent", params)
		if err == nil {
			break
		}
		if strings.Contains(err.Error(), "extent does not exist") || strings.Contains(err.Error(), "404 page") {
			break
		}
		time.Sleep(1 * time.Second)
	}
	if err != nil {
		return
	}
	ehs = new(proto.ExtentInfoBlock)
	ehsOld := new(proto.ExtentInfo)
	if err = json.Unmarshal(d, ehsOld); err != nil {
		err = json.Unmarshal(d, ehs)
		return
	}
	ehs[proto.ExtentInfoFileID] = ehsOld.FileID
	ehs[proto.ExtentInfoSize] = ehsOld.Size
	ehs[proto.ExtentInfoModifyTime] = uint64(ehsOld.ModifyTime)
	ehs[proto.ExtentInfoCrc] = uint64(ehsOld.Crc)
	return
}

func (dc *DataClient) RepairExtent(extent uint64, partitionPath string, partition uint64) (err error) {
	params := make(map[string]string)
	params["partition"] = strconv.FormatUint(partition, 10)
	params["path"] = partitionPath
	params["extent"] = strconv.FormatUint(extent, 10)
	for i := 0; i < 3; i++ {
		_, err = dc.RequestHttp(http.MethodGet, "/repairExtent", params)
		if err == nil {
			break
		}
		time.Sleep(time.Second)
	}
	if err != nil {
		return
	}
	return
}

//RepairExtentBatch
//extent: split by '-'
//path: /data6/datapartition_190_128849018880
func (dc *DataClient) RepairExtentBatch(extents, partitionPath string, partition uint64) (exts map[uint64]string, err error) {
	params := make(map[string]string)
	params["partition"] = strconv.FormatUint(partition, 10)
	params["path"] = partitionPath
	params["extent"] = extents
	d := make([]byte, 0)
	for i := 0; i < 3; i++ {
		d, err = dc.RequestHttp(http.MethodGet, "/repairExtentBatch", params)
		if err == nil {
			break
		}
		time.Sleep(time.Second)
	}
	if err != nil {
		return
	}
	exts = make(map[uint64]string, 0)
	if err = json.Unmarshal(d, &exts); err != nil {
		return
	}
	return
}

func (dc *DataClient) GetDatanodeStats() (stats *proto.DataNodeStats, err error) {
	var d []byte
	for i := 0; i < 3; i++ {
		d, err = dc.RequestHttp(http.MethodGet, "/stats", nil)
		if err == nil {
			break
		}
		time.Sleep(1 * time.Second)
	}
	if err != nil {
		return
	}
	stats = new(proto.DataNodeStats)
	if err = json.Unmarshal(d, stats); err != nil {
		return
	}
	return
}

func (dc *DataClient) GetRaftStatus(raftID uint64) (raftStatus *proto.Status, err error) {
	var d []byte
	params := make(map[string]string, 0)
	params["raftID"] = strconv.FormatUint(raftID, 10)
	for i := 0; i < 3; i++ {
		d, err = dc.RequestHttp(http.MethodGet, "/raftStatus", params)
		if err == nil {
			break
		}
		time.Sleep(1 * time.Second)
	}
	if err != nil {
		return
	}
	raftStatus = new(proto.Status)
	if err = json.Unmarshal(d, raftStatus); err != nil {
		return
	}
	return
}

func (dc *DataClient) SetAutoRepairStatus(autoRepair bool) (autoRepairResult bool, err error) {
	var d []byte
	params := make(map[string]string, 0)
	params["autoRepair"] = strconv.FormatBool(autoRepair)
	for i := 0; i < 3; i++ {
		d, err = dc.RequestHttp(http.MethodGet, "/setAutoRepairStatus", params)
		if err == nil {
			break
		}
		time.Sleep(1 * time.Second)
	}
	if err != nil {
		return
	}
	if err = json.Unmarshal(d, &autoRepairResult); err != nil {
		return
	}
	return
}

func (dc *DataClient) ReleasePartitions(key string) (data string, err error) {
	var d []byte
	params := make(map[string]string, 0)
	params["key"] = key
	for i := 0; i < 3; i++ {
		d, err = dc.RequestHttp(http.MethodGet, "/releasePartitions", params)
		if err == nil {
			break
		}
		time.Sleep(1 * time.Second)
	}
	if err != nil {
		return
	}
	if err = json.Unmarshal(d, &data); err != nil {
		return
	}
	return
}

func (dc *DataClient) GetStatInfo() (statInfo *proto.StatInfo, err error) {
	var d []byte
	for i := 0; i < 3; i++ {
		d, err = dc.RequestHttp(http.MethodGet, "/stat/info", nil)
		if err == nil {
			break
		}
		time.Sleep(1 * time.Second)
	}
	if err != nil {
		return
	}
	statInfo = new(proto.StatInfo)
	if err = json.Unmarshal(d, statInfo); err != nil {
		return
	}
	return
}

func (dc *DataClient) GetReplProtocolDetail() (details []*proto.ReplProtocalBufferDetail, err error) {
	var d []byte
	for i := 0; i < 3; i++ {
		d, err = dc.RequestHttp(http.MethodGet, "/getReplBufferDetail", nil)
		if err == nil {
			break
		}
		time.Sleep(1 * time.Second)
	}
	if err != nil {
		return
	}
	if err = json.Unmarshal(d, &details); err != nil {
		return
	}
	return
}

func (dc *DataClient) MoveExtentFile(extent uint64, partitionPath string, partition uint64) (err error) {
	params := make(map[string]string)
	params["partition"] = strconv.FormatUint(partition, 10)
	params["path"] = partitionPath
	params["extent"] = strconv.FormatUint(extent, 10)
	for i := 0; i < 3; i++ {
		_, err = dc.RequestHttp(http.MethodGet, "/moveExtent", params)
		if err == nil {
			break
		}
		time.Sleep(time.Second)
	}
	if err != nil {
		return
	}
	return
}

func (dc *DataClient) MoveExtentFileBatch(extents, partitionPath string, partition uint64) (resultMap map[uint64]string, err error) {
	params := make(map[string]string)
	params["partition"] = strconv.FormatUint(partition, 10)
	params["path"] = partitionPath
	params["extent"] = extents
	d := make([]byte, 0)
	for i := 0; i < 3; i++ {
		d, err = dc.RequestHttp(http.MethodGet, "/moveExtentBatch", params)
		if err == nil {
			break
		}
		time.Sleep(time.Second)
	}
	if err != nil {
		return
	}
	resultMap = make(map[uint64]string, 0)
	if err = json.Unmarshal(d, &resultMap); err != nil {
		return
	}
	return
}

func (dc *DataClient) GetExtentCrc(partition, extent uint64) (crc proto.ExtentCrc, err error) {
	params := make(map[string]string)
	params["partitionId"] = strconv.FormatUint(partition, 10)
	params["extentId"] = strconv.FormatUint(extent, 10)
	d := make([]byte, 0)
	for i := 0; i < 3; i++ {
		d, err = dc.RequestHttp(http.MethodGet, "/extentCrc", params)
		if err == nil {
			break
		}
		time.Sleep(time.Second)
	}
	if err != nil {
		return
	}
	if err = json.Unmarshal(d, &crc); err != nil {
		return
	}
	crc.ExtentId = extent
	return
}

func (dc *DataClient) PlaybackPartitionTinyDelete(partition uint64) (err error) {
	params := make(map[string]string, 0)
	params["partitionID"] = strconv.FormatUint(partition, 10)
	for i := 0; i < 3; i++ {
		_, err = dc.RequestHttp(http.MethodGet, "/playbackTinyExtentMarkDelete", params)
		if err == nil {
			break
		}
		time.Sleep(time.Second)
	}
	if err != nil {
		return
	}
	return
}
