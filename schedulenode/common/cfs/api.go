package cfs

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/unit"
	"io/ioutil"
	"net/http"
)

const (
	ErrCodeSuccess   = 0
	MNErrCodeSuccess = 200
)

func DoRequestToMn(isReleaseCluster bool, reqUrl string) (data []byte, err error) {

	var resp *http.Response
	if resp, err = http.Get(reqUrl); err != nil {
		log.LogErrorf("action[doRequest] reqRUL[%v] err:%v\n", reqUrl, err)
		return
	}
	defer resp.Body.Close()
	if data, err = ioutil.ReadAll(resp.Body); err != nil {
		log.LogErrorf("action[doRequest] reqRUL[%v] err:%v\n", reqUrl, err)
		return
	}

	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("action[doRequest] reqRUL[%v],statusCode[%v],body[%v]", reqUrl, resp.StatusCode, string(data))
		return
	}
	if !isReleaseCluster {
		reply := HTTPReply{}
		if err = json.Unmarshal(data, &reply); err != nil {
			log.LogErrorf("action[doRequest] reqRUL[%v],body[%v] err:%v\n", reqUrl, string(data), err)
			return
		}
		if reply.Code != MNErrCodeSuccess {
			err = fmt.Errorf("%v", reply.Msg)
			log.LogErrorf("action[doRequest] reqRUL[%v] err:%v\n", reqUrl, err)
			return
		}
		data = reply.Data
	}
	return
}

func DoRequest(reqUrl string, isReleaseCluster bool) (data []byte, err error) {
	var resp *http.Response
	if resp, err = http.Get(reqUrl); err != nil {
		log.LogErrorf("action[doRequest] reqRUL[%v] err:%v\n", reqUrl, err)
		return
	}
	defer resp.Body.Close()

	if data, err = ioutil.ReadAll(resp.Body); err != nil {
		log.LogErrorf("action[doRequest] reqRUL[%v] err:%v\n", reqUrl, err)
		return
	}
	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("action[doRequest] reqRUL[%v],statusCode[%v],body[%v]", reqUrl, resp.StatusCode, string(data))
		return
	}
	if !isReleaseCluster {
		reply := HTTPReply{}
		if err = json.Unmarshal(data, &reply); err != nil {
			log.LogErrorf("action[doRequest] reqRUL[%v] err:%v\n", reqUrl, err)
			return
		}
		if reply.Code != ErrCodeSuccess {
			err = fmt.Errorf("%v", reply.Msg)
			log.LogErrorf("action[doRequest] reqRUL[%v] err:%v\n", reqUrl, err)
			return
		}
		data = reply.Data
	}
	return
}

func GetDataNode(host, addr string, isReleaseCluster bool) (nv *NodeView, err error) {
	reqURL := fmt.Sprintf("http://%v/dataNode/get?addr=%v", host, addr)
	data, err := DoRequest(reqURL, isReleaseCluster)
	if err != nil {
		return
	}
	nv = &NodeView{}
	if err = json.Unmarshal(data, nv); err != nil {
		return
	}
	return
}

func GetMetaNode(host, addr string, isReleaseCluster bool) (nv *NodeView, err error) {
	reqURL := fmt.Sprintf("http://%v/metaNode/get?addr=%v", host, addr)
	data, err := DoRequest(reqURL, isReleaseCluster)
	if err != nil {
		return
	}
	nv = &NodeView{}
	if err = json.Unmarshal(data, nv); err != nil {
		return
	}
	return
}

func GetDataPartition(host string, id uint64, isReleaseCluster bool) (partition *DataPartition, err error) {
	reqURL := fmt.Sprintf("http://%v/dataPartition/get?id=%v", host, id)
	data, err := DoRequest(reqURL, isReleaseCluster)
	if err != nil {
		return
	}
	partition = &DataPartition{}
	if err = json.Unmarshal(data, partition); err != nil {
		return
	}
	return
}

func GetMetaPartition(host string, id uint64, isReleaseCluster bool) (partition *MetaPartition, err error) {
	reqURL := fmt.Sprintf("http://%v/metaPartition/get?id=%v", host, id)
	data, err := DoRequest(reqURL, isReleaseCluster)
	if err != nil {
		return
	}
	partition = &MetaPartition{}
	if err = json.Unmarshal(data, partition); err != nil {
		return
	}
	return
}

func GetCluster(host string, isReleaseCluster bool) (cv *ClusterView, err error) {
	reqURL := fmt.Sprintf("http://%v/admin/getCluster", host)
	data, err := DoRequest(reqURL, isReleaseCluster)
	if err != nil {
		return
	}
	cv = &ClusterView{}
	if err = json.Unmarshal(data, cv); err != nil {
		return
	}
	if !isReleaseCluster {
		cv.DataNodeStat = cv.DataNodeStatInfo
		cv.MetaNodeStat = cv.MetaNodeStatInfo
		cv.VolStat = cv.VolStatInfo
		for _, vol := range cv.VolStat {
			vol.TotalGB = vol.TotalSize / unit.GB
			vol.UsedGB = vol.UsedSize / unit.GB
		}
	}
	log.LogInfof("action[getCluster],host[%v],len(VolStat)=%v,len(metaNodes)=%v,len(dataNodes)=%v",
		host, len(cv.VolStat), len(cv.MetaNodes), len(cv.DataNodes))
	return
}

func GetClusterStat(host string, isReleaseCluster bool) (csv *ClusterStatInfoView, err error) {
	reqURL := fmt.Sprintf("http://%v/cluster/stat", host)
	if isReleaseCluster {
		return nil, fmt.Errorf("API[%s] does not exist", reqURL)
	}
	data, err := DoRequest(reqURL, isReleaseCluster)
	if err != nil {
		return
	}
	csv = &ClusterStatInfoView{}
	if err = json.Unmarshal(data, csv); err != nil {
		return
	}
	zoneLength := len(csv.ZoneStatInfo)
	zoneNames := make([]string, zoneLength)
	for zoneName := range csv.ZoneStatInfo {
		zoneNames = append(zoneNames, zoneName)
	}
	log.LogInfof("action[getClusterStat],host[%v],len(zone)=%v,zoneNames=%v", host, zoneLength, zoneNames)
	return
}
