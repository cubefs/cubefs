package metapartition

import (
	"strconv"

	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/cubefs/cubefs/proto"
	"github.com/gin-gonic/gin"

	"github.com/cubefs/cubefs/console/backend/helper/codes"
	"github.com/cubefs/cubefs/console/backend/helper/ginutils"
	"github.com/cubefs/cubefs/console/backend/helper/node"
	"github.com/cubefs/cubefs/console/backend/service/metapartition"
)

type CreateInput struct {
	Name  string `json:"name" binding:"required"`
	Start uint64 `json:"start" binding:"required"`
}

func Create(c *gin.Context) {
	input := &CreateInput{}
	addr, err := ginutils.CheckAndGetMaster(c, input)
	if err != nil {
		return
	}
	err = metapartition.Create(c, addr, input.Name, input.Start)
	if err != nil {
		log.Errorf("metapartition.Create failed.args:%+v,addr:%s,err:%+v", input, addr, err)
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
	err = metapartition.Load(c, addr, input.Id)
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
	Start       uint64
	End         string
	MaxInodeID  uint64
	InodeCount  uint64
	DentryCount uint64
	VolName     string
	Leader      string
	Status      string
	ReplicaNum  uint8 `json:"ReplicaNum,omitempty"`
	IsRecover   bool
	Members     []string
	Hosts       []string                           `json:"Hosts,omitempty"`
	Peers       []proto.Peer                       `json:"Peers,omitempty"`
	Zones       []string                           `json:"Zones,omitempty"`
	Response    []*proto.MetaPartitionLoadResponse `json:"Response,omitempty"`
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
	mps, err := metapartition.GetByName(c, clusterAddr, input.VolName)
	if err != nil {
		log.Errorf("metapartition.GetByName failed.args:%+v,addr:%s,err;%+v", input, clusterAddr, err)
		return output, err
	}
	for _, mp := range *mps {
		if input.Id != "" && input.Id != strconv.FormatUint(mp.PartitionID, 10) {
			continue
		}
		item := ListOutput{
			PartitionID: mp.PartitionID,
			Start:       mp.Start,
			End:         node.FormatUint64(mp.End), // ??why does "end" format to string but start keeps uint64
			MaxInodeID:  mp.MaxInodeID,
			InodeCount:  mp.InodeCount,
			DentryCount: mp.DentryCount,
			VolName:     input.VolName,
			Leader:      mp.LeaderAddr,
			Status:      node.FormatDiskAndPartitionStatus(mp.Status),
			IsRecover:   mp.IsRecover,
			Members:     mp.Members,
		}
		output = append(output, item)
	}
	return output, nil
}

func listById(c *gin.Context, clusterAddr string, input *ListInput) ([]ListOutput, error) {
	mp, err := metapartition.GetById(c, clusterAddr, input.Id)
	if err != nil {
		log.Errorf("metapartition.GetById failed.args:%+v,addr:%s,err;%+v", input, clusterAddr, err)
		return []ListOutput{}, err
	}
	output := ListOutput{
		PartitionID: mp.PartitionID,
		Start:       mp.Start,
		End:         node.FormatUint64(mp.End),
		MaxInodeID:  mp.MaxInodeID,
		InodeCount:  mp.InodeCount,
		DentryCount: mp.DentryCount,
		VolName:     mp.VolName,
		Status:      node.FormatDiskAndPartitionStatus(mp.Status),
		ReplicaNum:  mp.ReplicaNum,
		IsRecover:   mp.IsRecover,
		Members:     mp.Hosts,
		Hosts:       mp.Hosts,
		Peers:       mp.Peers,
		Zones:       mp.Zones,
		Response:    mp.LoadResponse,
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
		output, err = metapartition.Decommission(c, addr, strconv.FormatInt(p.Id, 10), p.NodeAddr)
		if err != nil {
			log.Errorf("metapartition.Decommission failed.addr:%s,id:%d,node_addr:%s,err:%+v", addr, p.Id, p.NodeAddr, err)
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
	output, err := metapartition.Diagnosis(c, addr)
	if err != nil {
		log.Errorf("metapartition.Diagnosis failed.addr:%s,err:%+v", addr, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), output)
}
