package sdk

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/http_client"
	sdk "github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/storage"
	"github.com/tiglabs/raft"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type DataPartition struct {
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
	TinyDeleteRecordSize int64                     `json:"tinyDeleteRecordSize"`
	RaftStatus           *raft.Status              `json:"raftStatus"`
}

func GetExtentsByDp(client *sdk.MasterClient, partitionId uint64, replicaAddr string) (re *DataPartition, err error) {
	if replicaAddr == "" {
		partition, err := client.AdminAPI().GetDataPartition("", partitionId)
		if err != nil {
			return nil, err
		}
		replicaAddr = partition.Hosts[0]
	}
	addressInfo := strings.Split(replicaAddr, ":")
	datanode := fmt.Sprintf("%s:%d", addressInfo[0], client.DataNodeProfPort)
	url := fmt.Sprintf("http://%s/partition?id=%d", datanode, partitionId)
	httpClient := http.Client{
		Timeout: 2 * time.Minute,
	}
	resp, err := httpClient.Get(url)
	if err != nil {
		return
	}
	respData, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}
	var data []byte
	if data, err = parseResp(respData); err != nil {
		return
	}
	re = &DataPartition{}
	if err = json.Unmarshal(data, &re); err != nil {
		return
	}
	if re == nil {
		err = fmt.Errorf("Get %s fails, data: %s ", url, string(data))
		return
	}
	fmt.Printf("getExtentsByDp, dp: %d, addr: %s, total: %d\n", partitionId, replicaAddr, re.FileCount)
	return
}

func GetDataPartitionInfo(nodeAddr string, prof uint16, dp uint64) (dn *proto.DNDataPartitionInfo, err error) {
	datanodeAddr := fmt.Sprintf("%s:%d", strings.Split(nodeAddr, ":")[0], prof)
	dataClient := http_client.NewDataClient(datanodeAddr, false)
	dn, err = dataClient.GetPartitionFromNode(dp)
	return
}

func RepairExtents(mc *sdk.MasterClient, host string, partitionID uint64, extentIDs []uint64) (err error) {
	var dp *proto.DataPartitionInfo
	if partitionID < 0 || len(extentIDs) == 0 {
		return
	}
	dp, err = mc.AdminAPI().GetDataPartition("", partitionID)
	if err != nil {
		return
	}
	var exist bool
	for _, h := range dp.Hosts {
		if h == host {
			exist = true
			break
		}
	}
	if !exist {
		err = fmt.Errorf("host[%v] not exist in hosts[%v]", host, dp.Hosts)
		return
	}
	dHost := fmt.Sprintf("%v:%v", strings.Split(host, ":")[0], mc.DataNodeProfPort)
	dataClient := http_client.NewDataClient(dHost, false)
	partition, err := dataClient.GetPartitionFromNode(partitionID)
	if err != nil {
		return
	}
	partitionPath := fmt.Sprintf("datapartition_%v_%v", partitionID, dp.Replicas[0].Total)
	var extMap map[uint64]string
	if len(extentIDs) == 1 {
		err = dataClient.RepairExtent(extentIDs[0], partition.Path, partitionID)
	} else {
		extentsStrs := make([]string, 0)
		for _, e := range extentIDs {
			extentsStrs = append(extentsStrs, strconv.FormatUint(e, 10))
		}
		extMap, err = dataClient.RepairExtentBatch(strings.Join(extentsStrs, "-"), partition.Path, partitionID)
	}
	if err != nil {
		if _, e := dataClient.GetPartitionFromNode(partitionID); e == nil {
			return
		}
		for i := 0; i < 3; i++ {
			if e := dataClient.ReLoadPartition(partitionPath, strings.Split(partition.Path, "/datapartition")[0]); e == nil {
				break
			}
		}
		return
	}
	if len(extMap) > 0 {
		fmt.Printf("repair result: %v\n", extMap)
	}
	return nil
}

func parseResp(resp []byte) (data []byte, err error) {
	var body = &struct {
		Code int32           `json:"code"`
		Msg  string          `json:"msg"`
		Data json.RawMessage `json:"data"`
	}{}
	if err = json.Unmarshal(resp, &body); err != nil {
		return
	}
	data = body.Data
	return
}
