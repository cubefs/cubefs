package metanode

import (
	"errors"
	"github.com/cubefs/cubefs/console/backend/helper"
	"net/http"

	"github.com/cubefs/cubefs/proto"
	"github.com/gin-gonic/gin"

	"github.com/cubefs/cubefs/console/backend/helper/httputils"
)

type AddInput struct {
	Id       string `json:"id"`
	ZoneName string `json:"zone_name"`
	Addr     string `json:"addr"`
}

func Add(c *gin.Context, clusterAddr string, input *AddInput) (interface{}, error) {
	reqUrl := "http://" + clusterAddr + proto.AddMetaNode + "?" + helper.BuildUrlParams(input)
	resp, err := httputils.DoRequestNoCookie(c, reqUrl, http.MethodGet, nil, nil)
	if err != nil {
		return nil, err
	}
	output := httputils.Output{}
	_, err = httputils.HandleResponse(c, resp, err, &output)
	if err != nil {
		return nil, err
	}
	if output.Code != proto.ErrCodeSuccess {
		return nil, errors.New(output.Msg)
	}
	return output.Data, nil
}

func Get(c *gin.Context, clusterNode, addr string) (*proto.MetaNodeInfo, error) {
	reqUrl := "http://" + clusterNode + proto.GetMetaNode + "?addr=" + addr
	resp, err := httputils.DoRequestNoCookie(c, reqUrl, http.MethodGet, nil, nil)
	if err != nil {
		return nil, err
	}
	output := httputils.Output{Data: &proto.MetaNodeInfo{}}
	_, err = httputils.HandleResponse(c, resp, err, &output)
	if err != nil {
		return nil, err
	}
	if output.Code != proto.ErrCodeSuccess {
		return nil, errors.New(output.Msg)
	}
	return output.Data.(*proto.MetaNodeInfo), nil
}

func Decommission(c *gin.Context, clusterAddr, addr string) (interface{}, error) {
	reqUrl := "http://" + clusterAddr + proto.DecommissionMetaNode + "?addr=" + addr
	resp, err := httputils.DoRequestNoCookie(c, reqUrl, http.MethodGet, nil, nil)
	if err != nil {
		return nil, err
	}
	output := httputils.Output{}
	_, err = httputils.HandleResponse(c, resp, err, &output)
	if err != nil {
		return nil, err
	}
	if output.Code != proto.ErrCodeSuccess {
		return nil, errors.New(output.Msg)
	}
	return output.Data, nil
}

func Migrate(c *gin.Context, clusterAddr, srcAddr, targetAddr string) error {
	reqUrl := "http://" + clusterAddr + proto.MigrateMetaNode + "?srcAddr=" + srcAddr + "&targetAddr=" + targetAddr
	resp, err := httputils.DoRequestNoCookie(c, reqUrl, http.MethodGet, nil, nil)
	if err != nil {
		return err
	}
	output := httputils.Output{}
	_, err = httputils.HandleResponse(c, resp, err, &output)
	if err != nil {
		return err
	}
	if output.Code != proto.ErrCodeSuccess {
		return errors.New(output.Msg)
	}
	return nil
}

const (
	NodePort      = "17220"
	PathPartition = "/getPartitions"
)

type PartitionsData map[string]Partition

type Partition struct {
	PartitionId uint64      `json:"partition_id"`
	VolName     string      `json:"vol_name"`
	Start       uint64      `json:"start"`
	End         uint64      `json:"end"`
	Peers       interface{} `json:"peers"`
}

func GetPartitions(c *gin.Context, nodeAddr string) (*PartitionsData, error) {
	reqUrl := "http://" + nodeAddr + ":" + NodePort + PathPartition
	resp, err := httputils.DoRequestNoCookie(c, reqUrl, http.MethodGet, nil, nil)
	if err != nil {
		return nil, err
	}
	output := httputils.Output{Data: &PartitionsData{}}
	_, err = httputils.HandleResponse(c, resp, err, &output)
	if err != nil {
		return nil, err
	}
	if output.Code != http.StatusOK {
		return nil, errors.New(output.Msg)
	}
	data := output.Data.(*PartitionsData)
	if data == nil {
		return nil, nil
	}
	return data, nil
}
