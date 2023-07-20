package datapartition

import (
	"strconv"

	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/cubefs/cubefs/proto"
	"github.com/gin-gonic/gin"

	"github.com/cubefs/cubefs/console/backend/helper/codes"
	"github.com/cubefs/cubefs/console/backend/helper/ginutils"
	"github.com/cubefs/cubefs/console/backend/helper/node"
	"github.com/cubefs/cubefs/console/backend/service/datapartition"
)

type CreateInput struct {
	Name  string `json:"name" binding:"required"`
	Count int    `json:"count" binding:"required"`
}

func Create(c *gin.Context) {
	input := &CreateInput{}
	addr, err := ginutils.CheckAndGetMaster(c, input)
	if err != nil {
		return
	}
	err = datapartition.Create(c, addr, input.Name, input.Count)
	if err != nil {
		log.Errorf("datapartition.Create failed.args:%+v,addr:%s,err:%+v", input, addr, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), nil)
}

type LoadInput struct {
	Id string `form:"id"`
}

func Load(c *gin.Context) {
	input := &LoadInput{}
	addr, err := ginutils.CheckAndGetMaster(c, input)
	if err != nil {
		return
	}
	err = datapartition.Load(c, addr, input.Id)
	if err != nil {
		log.Errorf("datapartition.Load failed. args:%+v,addr:%s,err:%+v", input, addr, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), nil)
}

type ListInput struct {
	Id      string `form:"id"`
	VolName string `form:"vol_name"`
}

type ListOutput struct {
	PartitionID uint64
	Status      string
	ReplicaNum  uint8
	Hosts       []string
	Members     []string
	Leader      string
	VolName     string
	IsRecover   bool
	FileCount   uint32
	Peers       []proto.Peer         `json:"Peers,omitempty"`
	Zones       []string             `json:"Zones,omitempty"`
	Replicas    []*proto.DataReplica `json:"Replicas,omitempty"`
}

func List(c *gin.Context) {
	input := &ListInput{}
	addr, err := ginutils.CheckAndGetMaster(c, input)
	if err != nil {
		return
	}
	output := make([]ListOutput, 0)
	if input.VolName != "" {
		output, err = listByName(c, addr, input)
	} else if input.Id != "" {
		output, err = listById(c, addr, input)
	}
	if err != nil {
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), output)
}

func listByName(c *gin.Context, clusterAddr string, input *ListInput) ([]ListOutput, error) {
	output := make([]ListOutput, 0)
	dps, err := datapartition.GetByName(c, clusterAddr, input.VolName)
	if err != nil {
		log.Errorf("datapartition.GetByName failed.args:%+v,addr:%s,err;%+v", input, clusterAddr, err)
		return output, err
	}
	for _, dp := range dps {
		if input.Id != "" && input.Id != strconv.FormatUint(dp.PartitionID, 10) {
			continue
		}
		item := ListOutput{
			PartitionID: dp.PartitionID,
			Status:      node.FormatDiskAndPartitionStatus(dp.Status),
			ReplicaNum:  dp.ReplicaNum,
			Hosts:       dp.Hosts,
			Members:     dp.Hosts,
			Leader:      dp.LeaderAddr,
			VolName:     input.VolName,
			IsRecover:   dp.IsRecover,
		}
		output = append(output, item)
	}
	return output, nil
}

func listById(c *gin.Context, clusterAddr string, input *ListInput) ([]ListOutput, error) {
	dp, err := datapartition.GetById(c, clusterAddr, input.Id)
	if err != nil {
		log.Errorf("datapartition.GetById failed.args:%+v,addr:%s,err;%+v", input, clusterAddr, err)
		return []ListOutput{}, err
	}
	output := ListOutput{
		PartitionID: dp.PartitionID,
		Status:      node.FormatDiskAndPartitionStatus(dp.Status),
		ReplicaNum:  dp.ReplicaNum,
		Hosts:       dp.Hosts,
		Members:     dp.Hosts,
		Leader:      "",
		VolName:     dp.VolName,
		IsRecover:   dp.IsRecover,
		FileCount:   0,
		Peers:       dp.Peers,
		Zones:       dp.Zones,
		Replicas:    dp.Replicas,
	}
	for _, r := range dp.Replicas {
		if r.IsLeader {
			output.Leader = r.Addr
		}
		output.FileCount += r.FileCount
	}
	return []ListOutput{output}, nil
}

type DecommissionInput struct {
	Partitions []DecommissionPartitionItem `json:"partitions" binding:"required,gte=1,dive"`
}

type DecommissionPartitionItem struct {
	Id       int64  `json:"id" binding:"required"`
	NodeAddr string `json:"node_addr" binding:"required"`
}

func Decommission(c *gin.Context) {
	input := &DecommissionInput{}
	addr, err := ginutils.CheckAndGetMaster(c, input)
	if err != nil {
		return
	}
	var output interface{}
	for i := range input.Partitions {
		p := input.Partitions[i]
		output, err = datapartition.Decommission(c, addr, strconv.FormatInt(p.Id, 10), p.NodeAddr)
		if err != nil {
			log.Errorf("datapartition.Decommission failed.addr:%s,id:%d,node_addr:%s,err:%+v", addr, p.Id, p.NodeAddr, err)
			ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
			return
		}
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), output)
}

func Diagnosis(c *gin.Context) {
	addr, err := ginutils.GetClusterMaster(c)
	if err != nil {
		return
	}
	output, err := datapartition.Diagnosis(c, addr)
	if err != nil {
		log.Errorf("datapartition.Diagnosis failed.addr:%s,err:%+v", addr, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), output)
}
