package data

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/log"
)

const (
	requestTimeout = 30 * time.Second
)

type request struct {
	method string
	path   string
	params map[string]string
	header map[string]string
	body   []byte
}

func newAPIRequest(method string, path string) *request {
	return &request{
		method: method,
		path:   path,
		params: make(map[string]string),
		header: make(map[string]string),
	}
}

func (r *request) addParam(key, value string) {
	r.params[key] = value
}

func (r *request) addHeader(key, value string) {
	r.header[key] = value
}

func (r *request) addBody(body []byte) {
	r.body = body
}

type DataHttpClient struct {
	sync.RWMutex
	useSSL bool
	host   string
}

// NewMasterHelper returns a new MasterClient instance.
func NewDataHttpClient(host string, useSSL bool) *DataHttpClient {
	return &DataHttpClient{host: host, useSSL: useSSL}
}

func (dc *DataHttpClient) serveRequest(r *request) (respData []byte, err error) {
	var resp *http.Response
	var schema string
	if dc.useSSL {
		schema = "https"
	} else {
		schema = "http"
	}
	var url = fmt.Sprintf("%s://%s%s", schema, dc.host, r.path)
	resp, err = dc.httpRequest(r.method, url, r.params, r.header, r.body)
	log.LogDebugf("resp %v,err %v", resp, err)
	if err != nil {
		log.LogErrorf("serveRequest: send http request fail: method(%v) url(%v) err(%v)", r.method, url, err)
		return
	}
	stateCode := resp.StatusCode
	respData, err = ioutil.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		log.LogErrorf("serveRequest: read http response body fail: err(%v)", err)
		return
	}
	switch stateCode {
	case http.StatusOK:
		var body = &struct {
			Code int32           `json:"code"`
			Msg  string          `json:"msg"`
			Data json.RawMessage `json:"data"`
		}{}

		if err := json.Unmarshal(respData, body); err != nil {
			return nil, fmt.Errorf("unmarshal response body err:%v", err)

		}
		// o represent proto.ErrCodeSuccess
		if body.Code != 200 {
			return nil, proto.ParseErrorCode(body.Code)
		}
		return []byte(body.Data), nil
	case http.StatusBadRequest:
		var body = &struct {
			Code int32           `json:"code"`
			Msg  string          `json:"msg"`
			Data json.RawMessage `json:"data"`
		}{}

		if err = json.Unmarshal(respData, body); err != nil {
			return nil, fmt.Errorf("unmarshal response body err:%v", err)
		}
		return nil, fmt.Errorf("%s:%s", proto.ParseErrorCode(body.Code), body.Msg)
	case http.StatusNotFound:
		return nil, fmt.Errorf("404 page:%s not found", r.path)
	case http.StatusInternalServerError:
		var body = &struct {
			Code int32           `json:"code"`
			Msg  string          `json:"msg"`
			Data json.RawMessage `json:"data"`
		}{}
		if err = json.Unmarshal(respData, body); err != nil {
			return nil, fmt.Errorf("unmarshal response body err:%v", err)
		}
		return nil, fmt.Errorf("%v", body.Msg)
	default:
		errMsg := fmt.Sprintf("serveRequest: unknown status: host(%v) uri(%v) status(%v) body(%s).",
			resp.Request.URL.String(), dc.host, stateCode, strings.Replace(string(respData), "\n", "", -1))
		err = fmt.Errorf(errMsg)
		log.LogErrorf(errMsg)
	}
	return
}

func (dc *DataHttpClient) httpRequest(method, url string, param, header map[string]string, reqData []byte) (resp *http.Response, err error) {
	client := http.DefaultClient
	reader := bytes.NewReader(reqData)
	client.Timeout = requestTimeout
	var req *http.Request
	fullUrl := dc.mergeRequestUrl(url, param)
	log.LogDebugf("httpRequest: merge request url: method(%v) url(%v) bodyLength[%v].", method, fullUrl, len(reqData))
	if req, err = http.NewRequest(method, fullUrl, reader); err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Connection", "close")
	for k, v := range header {
		req.Header.Set(k, v)
	}
	resp, err = client.Do(req)
	return
}

func (dc *DataHttpClient) mergeRequestUrl(url string, params map[string]string) string {
	if params != nil && len(params) > 0 {
		buff := bytes.NewBuffer([]byte(url))
		isFirstParam := true
		for k, v := range params {
			if isFirstParam {
				buff.WriteString("?")
				isFirstParam = false
			} else {
				buff.WriteString("&")
			}
			buff.WriteString(k)
			buff.WriteString("=")
			buff.WriteString(v)
		}
		return buff.String()
	}
	return url
}

func (dc *DataHttpClient) RequestHttp(method, path string, param map[string]string) (respData []byte, err error) {
	req := newAPIRequest(method, path)
	for k, v := range param {
		req.addParam(k, v)
	}
	return dc.serveRequest(req)
}

func (dc *DataHttpClient) ComputeExtentMd5(partitionID, extentID, offset, size uint64) (md5 string, err error){
	defer func() {
		if err != nil {
			log.LogErrorf("action[ComputeExtentMd5],pid:%v, extent:%v, offset:%v, size:%v, err:%v", partitionID, extentID, offset, size, err)
		}
	}()
	var buf []byte
	req := newAPIRequest(http.MethodGet, "/computeExtentMd5")
	req.addParam("id", strconv.FormatUint(partitionID, 10))
	req.addParam("extent", strconv.FormatUint(extentID, 10))
	req.addParam("offset", strconv.FormatUint(offset, 10))
	req.addParam("size", strconv.FormatUint(size, 10))
	res := struct {
		PartitionID uint64 `json:"PartitionID"`
		ExtentID    uint64 `json:"ExtentID"`
		Md5Sum      string `json:"md5"`
	}{}
	buf, err = dc.serveRequest(req)
	if err = json.Unmarshal(buf, &res); err != nil {
		return
	}
	md5 = res.Md5Sum
	return
}

func (dc *DataHttpClient) GetDisks() (diskInfo *proto.DataNodeDiskReport, err error){
	var d []byte
	for i := 0; i < 3; i++ {
		req := newAPIRequest(http.MethodGet, "/disks")
		d, err = dc.serveRequest(req)
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
func (dc *DataHttpClient) GetPartitionsFromNode() (partitions *proto.DataPartitions, err error) {
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

func (dc *DataHttpClient) GetPartitionFromNode(id uint64) (pInfo *proto.DNDataPartitionInfo, err error) {
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

func (dc *DataHttpClient) GetExtentHoles(id uint64, eid uint64) (ehs *proto.DNTinyExtentInfo, err error) {
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

func (dc *DataHttpClient) StopPartition(pid uint64) (err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("action[StopPartition],pid:%v,err:%v", pid, err)
		}
		log.LogFlush()
	}()
	req := newAPIRequest(http.MethodGet, "/stopPartition")
	req.addParam("partitionID", fmt.Sprintf("%v", pid))
	_, err = dc.serveRequest(req)
	log.LogInfof("action[StopPartition],pid:%v,:%v", pid, err)
	if err != nil {
		return
	}
	return
}

func (dc *DataHttpClient) ReLoadPartition(partitionDirName, dirPath string) (err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("action[ReLoadPartition],pid:%v,err:%v", partitionDirName, err)
		}
		log.LogFlush()
	}()
	req := newAPIRequest(http.MethodGet, "/reloadPartition")
	req.addParam("partitionPath", partitionDirName)
	req.addParam("disk", dirPath)
	_, err = dc.serveRequest(req)
	log.LogInfof("action[ReLoadPartition],pid:%v,err:%v", partitionDirName, err)
	if err != nil {
		return
	}
	return
}

func (dc *DataHttpClient) GetExtentBlockCrc(id uint64, eid uint64) (blocks []*proto.BlockCrc, err error) {
	var d []byte
	for i := 0; i < 3; i++ {
		req := newAPIRequest(http.MethodGet, "/block")
		req.addParam("partitionID", strconv.FormatUint(id, 10))
		req.addParam("extentID", strconv.FormatUint(eid, 10))
		d, err = dc.serveRequest(req)
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

func (dc *DataHttpClient) GetExtentInfo(id uint64, eid uint64) (ehs *proto.ExtentInfoBlock, err error) {
	var d []byte
	for i := 0; i < 3; i++ {
		req := newAPIRequest(http.MethodGet, "/extent")
		req.addParam("partitionID", strconv.FormatUint(id, 10))
		req.addParam("extentID", strconv.FormatUint(eid, 10))
		d, err = dc.serveRequest(req)
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

func (dc *DataHttpClient) RepairExtent(extent uint64, partitionPath string, partition uint64) (err error) {
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
func (dc *DataHttpClient) RepairExtentBatch(extents, partitionPath string, partition uint64) (exts map[uint64]string, err error) {
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

//datanodeAgent api

func (dc *DataHttpClient) FetchExtentsCrc(partitionPath string) (extentsMap map[uint64]*proto.ExtentInfoBlock, err error) {
	d := make([]byte, 0)
	for i := 0; i < 3; i++ {
		req := newAPIRequest(http.MethodGet, "/fetchExtentsCrc")
		req.addParam("path", partitionPath)
		d, err = dc.serveRequest(req)
		if err == nil {
			break
		}
		time.Sleep(5 * time.Second)
	}
	if err != nil {
		return
	}
	extentsMap = make(map[uint64]*proto.ExtentInfoBlock, 0)
	err = json.Unmarshal(d, &extentsMap)
	return
}
