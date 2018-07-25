package blob

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/util/log"
)

const (
	GetClusterInfoURL    = "/admin/getIp"
	GetDataPartitionsURL = "/client/dataPartitions"

	RefreshVolViewInterval = 1 * time.Minute

	MinWritableDataPartitionNum = 1 //FIXME
)

type DataPartitionView struct {
	DataPartitions []*DataPartition
}

type ClusterInfo struct {
	Cluster string
}

func (client *BlobClient) GetClusterInfo() error {
	body, err := client.master.Request(http.MethodPost, GetClusterInfoURL, nil, nil)
	if err != nil {
		log.LogErrorf("GetClusterInfo: err(%v)", err)
		return err
	}

	info := new(ClusterInfo)
	if err = json.Unmarshal(body, info); err != nil {
		log.LogErrorf("GetClusterInfo: err(%v)", err)
		return err
	}
	log.LogInfof("GetClusterInfo: info(%v)", *info)
	client.cluster = info.Cluster
	return nil
}

func (client *BlobClient) GetDataPartitionView() (*DataPartitionView, error) {
	params := make(map[string]string)
	params["name"] = client.volname
	body, err := client.master.Request(http.MethodPost, GetDataPartitionsURL, params, nil)
	if err != nil {
		log.LogErrorf("GetDataPartitionView: master request err(%v)", err)
		return nil, err
	}

	view := new(DataPartitionView)
	if err = json.Unmarshal(body, view); err != nil {
		log.LogErrorf("GetDataPartitionView: Unmarshal err(%v) body(%v)", err, string(body))
		return nil, err
	}
	return view, nil
}

func (client *BlobClient) UpdateDataPartitions() error {
	view, err := client.GetDataPartitionView()
	if err != nil {
		return err
	}

	rwPartitions := make([]*DataPartition, 0)
	for _, dp := range view.DataPartitions {
		if dp.Status == proto.ReadWrite {
			rwPartitions = append(rwPartitions, dp)
		}
	}
	if len(rwPartitions) < MinWritableDataPartitionNum {
		err = fmt.Errorf("UpdateDataPartitions: RW partitions(%v) Minimum(%v)", len(rwPartitions), MinWritableDataPartitionNum)
		log.LogWarn(err)
		return err
	}

	for _, dp := range view.DataPartitions {
		client.partitions.Put(dp)
	}
	return nil
}

func (client *BlobClient) refresh() {
	t := time.NewTicker(RefreshVolViewInterval)
	for {
		select {
		case <-t.C:
			client.UpdateDataPartitions()
		}
	}
}
