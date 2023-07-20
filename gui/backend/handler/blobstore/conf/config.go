package conf

import (
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/gin-gonic/gin"

	"github.com/cubefs/cubefs/console/backend/helper/codes"
	"github.com/cubefs/cubefs/console/backend/helper/ginutils"
	"github.com/cubefs/cubefs/console/backend/service/blobstore/clustermgr/config"
	"github.com/cubefs/cubefs/console/backend/service/blobstore/clustermgr/service"
	"github.com/cubefs/cubefs/console/backend/service/blobstore/scheduler"
)

type ListOutput struct {
	Repair        []interface{} `json:"repair"`
	Drop          []interface{} `json:"drop"`
	Balance       []interface{} `json:"balance"`
	ManualMigrate []interface{} `json:"manual_migrate"`
	Inspect       []interface{} `json:"inspect"`
	ShardRepair   []*TinkerData `json:"shard_repair"`
	BlobDelete    []*TinkerData `json:"blob_delete"`
}

type TinkerData struct {
	Host        string      `json:"host"`
	HostName    string      `json:"host_name"`
	ShardRepair interface{} `json:"shard_repair"`
	BlobDelete  interface{} `json:"blob_delete"`
}

func List(c *gin.Context) {
	consulAddr, err := ginutils.GetConsulAddr(c)
	if err != nil {
		return
	}
	taskStat, err := scheduler.LeaderStats(c, consulAddr)
	if err != nil {
		log.Errorf("scheduler.LeaderStats failed.consul_addr:%s,err:%+v", consulAddr, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	output := ListOutput{
		Repair:        []interface{}{taskStat.DiskRepair},
		Drop:          []interface{}{taskStat.DiskDrop},
		Balance:       []interface{}{taskStat.Balance},
		ManualMigrate: []interface{}{taskStat.ManualMigrate},
		Inspect:       []interface{}{taskStat.VolumeInspect},
		ShardRepair:   make([]*TinkerData, 0),
		BlobDelete:    make([]*TinkerData, 0),
	}
	serviceNodes, err := service.List(c, consulAddr)
	if err != nil {
		log.Errorf("service.List failed.consul_addr:%s,err:%+v", consulAddr, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	for _, node := range serviceNodes {
		if node.Name != proto.ServiceNameScheduler {
			continue
		}
		stats, err := scheduler.Stats(c, node.Host)
		if err != nil {
			log.Errorf("scheduler.Stats failed.node_addr:%s,err:%+v", node.Host, err)
			continue
		}

		dataRepair := &TinkerData{Host: node.Host + "_down"}
		if stats.ShardRepair != nil {
			dataRepair = &TinkerData{Host: node.Host, ShardRepair: stats.ShardRepair}
		}
		output.ShardRepair = append(output.ShardRepair, dataRepair)

		dataDelete := &TinkerData{Host: node.Host + "_down"}
		if stats.BlobDelete != nil {
			dataDelete = &TinkerData{Host: node.Host, BlobDelete: stats.BlobDelete}
		}
		output.BlobDelete = append(output.BlobDelete, dataDelete)
	}

	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), output)
}

type SetInput struct {
	Key   string `json:"key" binding:"required"`
	Value string `json:"value" binding:"required"`
}

func Set(c *gin.Context) {
	input := &SetInput{}
	consulAddr, err := ginutils.CheckAndGetConsul(c, input)
	if err != nil {
		return
	}
	err = config.Set(c, consulAddr, &config.SetInput{Key: input.Key, Value: input.Value})
	if err != nil {
		log.Errorf("config.Set failed.consulAddr:%s,args:%+v,err:%+v", consulAddr, input, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), nil)
}
