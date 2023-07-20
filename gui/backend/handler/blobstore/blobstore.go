package blobstore

import (
	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/gin-gonic/gin"

	"github.com/cubefs/cubefs/console/backend/handler/blobstore/common"
	"github.com/cubefs/cubefs/console/backend/helper/codes"
	"github.com/cubefs/cubefs/console/backend/helper/enums"
	"github.com/cubefs/cubefs/console/backend/helper/ginutils"
	"github.com/cubefs/cubefs/console/backend/model"
	cm "github.com/cubefs/cubefs/console/backend/service/blobstore/clustermgr"
	"github.com/cubefs/cubefs/console/backend/service/consul"
)

type OverviewOutput struct {
	clustermgr.StatInfo
	CodeModeInfo []common.CodeMode `json:"code_mode_info"`
}

func Overview(c *gin.Context) {
	consulAddr, err := ginutils.GetConsulAddr(c)
	if err != nil {
		return
	}
	statInfo, err := cm.Stat(c, consulAddr, true)
	if err != nil {
		log.Errorf("clustermgr.Stat failed.consulAddr:%s,err:%+v", consulAddr, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	if len(statInfo.RaftStatus.Peers) == 0 {
		statInfo, err = cm.Stat(c, "http://"+statInfo.LeaderHost, false)
		if err != nil {
			log.Errorf("clustermgr.Stat failed.consulAddr:%s,err:%+v", consulAddr, err)
			ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
			return
		}
	}
	modes, err := common.GetCodeMode(c, consulAddr)
	if err != nil {
		log.Errorf("common.GetCodeMode failed.consulAddr:%s,err:%+v", consulAddr, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), OverviewOutput{StatInfo: *statInfo, CodeModeInfo: modes})
}

type LeadershipTransferInput struct {
	PeerId uint64 `json:"peer_id" binding:"required"`
}

func LeadershipTransfer(c *gin.Context) {
	input := &LeadershipTransferInput{}
	consulAddr, err := ginutils.CheckAndGetConsul(c, input)
	if err != nil {
		return
	}
	output, err := cm.LeadershipTransfer(c, consulAddr, input.PeerId)
	if err != nil {
		log.Errorf("cm.LeadershipTransfer failed. consulAddr:%s,peer_id:%d,err:%+v", consulAddr, input.PeerId, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), output)
}

type MemberRemoveInput struct {
	PeerId uint64 `json:"peer_id" binding:"required"`
}

func MemberRemove(c *gin.Context) {
	input := &MemberRemoveInput{}
	consulAddr, err := ginutils.CheckAndGetConsul(c, input)
	if err != nil {
		return
	}
	output, err := cm.MemberRemove(c, consulAddr, input.PeerId)
	if err != nil {
		log.Errorf("cm.MemberRemove failed. consulAddr:%s,peer_id:%d,err:%+v", consulAddr, input.PeerId, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	ginutils.Success(c, output)
}

func ListClusters(c *gin.Context) {
	name := c.Param(ginutils.Cluster)
	cluster, err := new(model.Cluster).FindName(name)
	if err != nil {
		log.Errorf("cluster.FindName failed.name:%+v,err:%+v", name, err)
		ginutils.Send(c, codes.DatabaseError.Code(), err.Error(), nil)
		return
	}
	if cluster.VolType != enums.VolTypeLowFrequency || cluster.ConsulAddr == "" {
		log.Errorf("type error or no consul_addr.clusterModel:%+v", cluster)
		ginutils.Send(c, codes.NotFound.Code(), codes.NotFound.Msg(), nil)
		return
	}
	clusters, err := consul.GetRegionClusters(c, cluster.ConsulAddr)
	if err != nil {
		log.Errorf("get clusters failed.name:%+v,consul:%s,err:%+v", name, cluster.ConsulAddr, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	ginutils.Success(c, clusters)
}
