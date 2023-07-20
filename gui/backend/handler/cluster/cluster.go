package cluster

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/gin-gonic/gin"
	"github.com/globalsign/mgo/bson"
	"github.com/jinzhu/copier"
	"gorm.io/gorm"

	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/cubefs/cubefs/console/backend/helper"
	"github.com/cubefs/cubefs/console/backend/helper/codes"
	"github.com/cubefs/cubefs/console/backend/helper/enums"
	"github.com/cubefs/cubefs/console/backend/helper/ginutils"
	"github.com/cubefs/cubefs/console/backend/helper/types"
	"github.com/cubefs/cubefs/console/backend/model"
	"github.com/cubefs/cubefs/console/backend/service/cluster"
)

type CreateInput struct {
	Name       string         `json:"name" binding:"required"`
	MasterAddr types.StrSlice `json:"master_addr" binding:"required"`
	IDC        string         `json:"idc"`
	Cli        string         `json:"cli"`
	Domain     string         `json:"domain"`
	ConsulAddr string         `json:"consul_addr" binding:"omitempty,url"`
	Tag        string         `json:"tag"`
	S3Endpoint string         `json:"s3_endpoint" binding:"omitempty,url"`
	VolType    enums.VolType  `json:"vol_type"`
}

func (input *CreateInput) checkAddr(c *gin.Context) error {
	if input.VolType == enums.VolTypeLowFrequency {
		addrs := []string{input.ConsulAddr}
		addrs = append(addrs, input.MasterAddr...)
		return checkAddrIpPort(c, addrs)
	}
	return checkMasterAddr(c, input.MasterAddr)
}

func checkMasterAddr(c *gin.Context, masterAddr types.StrSlice) error {
	for _, v := range masterAddr {
		if err := checkIpPort(c, v); err != nil {
			return err
		}
		if _, err := cluster.Get(c, v); err != nil {
			log.Errorf("[%s] get cluster failed. err:%+v", v, err)
			ginutils.Send(c, codes.ThirdPartyError.Code(), fmt.Sprintf("ip: %s 無效", v), nil)
			return err
		}
	}
	return nil
}

func checkIpPort(c *gin.Context, addr string) error {
	idx := strings.Index(addr, "//")
	if idx > 0 {
		addr = addr[idx+2:]
	}
	if err := helper.CheckIpPort(addr); err != nil {
		log.Errorf("checkIpPort failed. addr:%s, err:%+v", addr, err)
		ginutils.Send(c, codes.InvalidArgs.Code(), err.Error(), nil)
		return err
	}
	return nil
}

func checkAddrIpPort(c *gin.Context, addrs []string) error {
	for _, addr := range addrs {
		if err := checkIpPort(c, addr); err != nil {
			return err
		}
	}
	return nil
}

func Create(c *gin.Context) {
	input := &CreateInput{}
	if !ginutils.Check(c, input) {
		return
	}
	if input.checkAddr(c) != nil {
		return
	}
	if err := checkNameExists(c, input.Name); err != nil {
		return
	}
	clusterInfo, err := cluster.Get(c, input.MasterAddr[0])
	if err != nil {
		ginutils.Send(c, codes.ThirdPartyError.Error(), err.Error(), nil)
		return
	}
	if err := checkTagExists(c, clusterInfo.Name); err != nil {
		return
	}
	input.Tag = clusterInfo.Name
	m, err := create(input)
	if err != nil {
		ginutils.Send(c, codes.DatabaseError.Code(), err.Error(), nil)
		return
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), m)
}

type UpdateInput struct {
	Id         int64          `json:"id" binding:"required"`
	MasterAddr types.StrSlice `json:"master_addr"`
	IDC        string         `json:"idc"`
	Cli        string         `json:"cli"`
	Domain     string         `json:"domain"`
	ConsulAddr string         `json:"consul_addr" binding:"omitempty,url"`
	Tag        string         `json:"tag"`
	S3Endpoint string         `json:"s3_endpoint" binding:"omitempty,url"`
}

func Update(c *gin.Context) {
	input := &UpdateInput{}
	if !ginutils.Check(c, input) {
		return
	}
	cm := new(model.Cluster)
	if err := cm.FindId(input.Id); err != nil {
		log.Errorf("cm.FindId. id:%s, error:%+v", input.Id, err)
		ginutils.Send(c, codes.DatabaseError.Code(), err.Error(), nil)
		return
	}
	set := bson.M{"update_time": time.Now()}
	if len(input.MasterAddr) > 0 {
		var err error
		if cm.VolType == enums.VolTypeLowFrequency {
			err = checkAddrIpPort(c, input.MasterAddr)
		} else {
			err = checkMasterAddr(c, input.MasterAddr)
		}
		if err != nil {
			return
		}
		set["master_addr"] = input.MasterAddr.String()
		clusterInfo, err := cluster.Get(c, input.MasterAddr[0])
		if err != nil {
			ginutils.Send(c, codes.ThirdPartyError.Error(), err.Error(), nil)
			return
		}
		clusterModel, err := new(model.Cluster).FindTag(clusterInfo.Name)
		if err != nil && err != gorm.ErrRecordNotFound {
			log.Errorf("by cluster by tag failed. err:%+v", err)
			ginutils.Send(c, codes.DatabaseError.Code(), err.Error(), nil)
			return
		}
		if clusterModel != nil && clusterModel.Name != cm.Name {
			err = errors.New("cluster tag exists")
			ginutils.Send(c, codes.InvalidArgs.Code(), err.Error(), nil)
			return
		}
		input.Tag = clusterInfo.Name
	}

	if cm.VolType == enums.VolTypeLowFrequency && input.ConsulAddr != "" {
		err := checkIpPort(c, input.ConsulAddr)
		if err != nil {
			return
		}
		set["consul_addr"] = input.ConsulAddr
	}

	handleStrBson(set, "idc", input.IDC)
	handleStrBson(set, "cli", input.Cli)
	handleStrBson(set, "domain", input.Domain)
	handleStrBson(set, "tag", input.Tag)
	handleStrBson(set, "s3_endpoint", input.S3Endpoint)

	if err := new(model.Cluster).Update(input.Id, set); err != nil {
		log.Errorf("update cluster failed. err:%+v", err)
		ginutils.Send(c, codes.DatabaseError.Code(), err.Error(), nil)
		return
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), nil)
}

func handleStrBson(set bson.M, key, val string) {
	if val == "" {
		return
	}
	set[key] = val
}

func checkNameExists(c *gin.Context, name string) error {
	cm, err := new(model.Cluster).FindName(name)
	if err != nil && err != gorm.ErrRecordNotFound {
		log.Errorf("find cluster by name failed. err:%+v", err)
		ginutils.Send(c, codes.DatabaseError.Code(), err.Error(), nil)
		return err
	}
	if cm != nil && cm.Id != 0 {
		err = errors.New("cluster name exists")
		ginutils.Send(c, codes.InvalidArgs.Code(), err.Error(), nil)
		return err
	}
	return nil
}

func checkTagExists(c *gin.Context, tag string) error {
	cm, err := new(model.Cluster).FindTag(tag)
	if err != nil && err != gorm.ErrRecordNotFound {
		log.Errorf("by cluster by tag failed. err:%+v", err)
		ginutils.Send(c, codes.DatabaseError.Code(), err.Error(), nil)
		return err
	}
	if cm != nil && cm.Id != 0 {
		err = errors.New("cluster tag exists")
		ginutils.Send(c, codes.InvalidArgs.Code(), err.Error(), nil)
		return err
	}
	return nil
}

func create(input *CreateInput) (*model.Cluster, error) {
	m := &model.Cluster{}
	if err := copier.Copy(m, input); err != nil {
		log.Errorf("copy Cluster failed. err:%+v", err)
		return m, err
	}
	if err := m.Create(); err != nil {
		log.Errorf("create cluster failed. err:%+v", err)
		return m, err
	}
	return m, nil
}

type ListInput struct {
	Page    int    `form:"page"`
	PerPage int    `form:"per_page"`
	Name    string `form:"name"`
	VolType *int   `form:"vol_type"`
}

func (input *ListInput) Check() error {
	if input.Page <= 0 {
		input.Page = 1
	}
	if input.PerPage <= 0 {
		input.PerPage = 15
	}
	return nil
}

type ListOutput struct {
	Page     int       `json:"page"`
	PerPage  int       `json:"per_page"`
	Count    int64     `json:"count"`
	Clusters []Cluster `json:"clusters"`
}

type Cluster struct {
	model.Cluster       `json:",inline"`
	OriginalName        string                   `json:"original_name"`
	LeaderAddr          string                   `json:"leader_addr"`
	MetaNodeSum         int                      `json:"meta_node_sum"`
	MetaTotal           string                   `json:"meta_total"`
	MetaUsed            string                   `json:"meta_used"`
	MetaUsedRatio       string                   `json:"meta_used_ratio"`
	DataNodeSum         int                      `json:"data_node_sum"`
	DataTotal           string                   `json:"data_total"`
	DataUsed            string                   `json:"data_used"`
	DataUsedRatio       string                   `json:"data_used_ratio"`
	MetaNodes           []proto.NodeView         `json:"meta_nodes,omitempty" `
	DataNodes           []proto.NodeView         `json:"data_nodes,omitempty"`
	BadPartitionIDs     []proto.BadPartitionView `json:"bad_partition_ids"`
	BadMetaPartitionIDs []proto.BadPartitionView `json:"bad_meta_partition_ids"`
}

func List(c *gin.Context) {
	input := &ListInput{}
	if !ginutils.Check(c, input) {
		return
	}
	param := model.FindClusterParam{}
	if err := copier.Copy(&param, input); err != nil {
		log.Errorf("copy param failed. err:%+v", err)
		ginutils.Send(c, codes.InvalidArgs.Code(), err.Error(), nil)
		return
	}
	clusters, count, err := new(model.Cluster).Find(param)
	if err != nil {
		log.Errorf("find clusters failed. input:%+v, err:%+v", input, err)
		ginutils.Send(c, codes.DatabaseError.Code(), err.Error(), nil)
		return
	}
	output := ListOutput{
		Page:     input.Page,
		PerPage:  input.PerPage,
		Count:    count,
		Clusters: make([]Cluster, 0),
	}
	if len(clusters) == 0 {
		ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), output)
		return
	}
	for _, m := range clusters {
		if len(m.MasterAddr) == 0 {
			output.Clusters = append(output.Clusters, Cluster{Cluster: m})
			continue
		}
		clusterInfo, err := cluster.Get(c, m.MasterAddr[0])
		if err != nil {
			log.Errorf("get addr(%v) cluster failed: %v", m.MasterAddr[0], err)
		}
		if clusterInfo == nil {
			output.Clusters = append(output.Clusters, Cluster{Cluster: m})
			continue
		}
		data := Cluster{
			Cluster:             m,
			OriginalName:        clusterInfo.Name,
			LeaderAddr:          clusterInfo.LeaderAddr,
			MetaNodeSum:         len(clusterInfo.MetaNodes),
			MetaTotal:           helper.GBByteConversion(clusterInfo.MetaNodeStatInfo.TotalGB),
			MetaUsed:            helper.GBByteConversion(clusterInfo.MetaNodeStatInfo.UsedGB),
			MetaUsedRatio:       helper.Percentage(clusterInfo.MetaNodeStatInfo.UsedGB, clusterInfo.MetaNodeStatInfo.TotalGB),
			DataNodeSum:         len(clusterInfo.DataNodes),
			DataTotal:           helper.GBByteConversion(clusterInfo.DataNodeStatInfo.TotalGB),
			DataUsed:            helper.GBByteConversion(clusterInfo.DataNodeStatInfo.UsedGB),
			DataUsedRatio:       helper.Percentage(clusterInfo.DataNodeStatInfo.UsedGB, clusterInfo.DataNodeStatInfo.TotalGB),
			MetaNodes:           clusterInfo.MetaNodes,
			DataNodes:           clusterInfo.DataNodes,
			BadPartitionIDs:     clusterInfo.BadPartitionIDs,
			BadMetaPartitionIDs: clusterInfo.BadMetaPartitionIDs,
		}
		output.Clusters = append(output.Clusters, data)
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), output)
}
