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
