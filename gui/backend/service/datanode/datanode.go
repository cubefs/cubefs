package datanode

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
	reqUrl := "http://" + clusterAddr + proto.AddDataNode + "?" + helper.BuildUrlParams(input)
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

func Get(c *gin.Context, clusterAddr, addr string) (*proto.DataNodeInfo, error) {
	reqUrl := "http://" + clusterAddr + proto.GetDataNode + "?addr=" + addr
	resp, err := httputils.DoRequestNoCookie(c, reqUrl, http.MethodGet, nil, nil)
	if err != nil {
		return nil, err
	}
	output := httputils.Output{Data: &proto.DataNodeInfo{}}
	_, err = httputils.HandleResponse(c, resp, err, &output)
	if err != nil {
		return nil, err
	}
	if output.Code != proto.ErrCodeSuccess {
		return nil, errors.New(output.Msg)
	}
	return output.Data.(*proto.DataNodeInfo), nil
}

func Decommission(c *gin.Context, clusterAddr, addr string) (interface{}, error) {
	reqUrl := "http://" + clusterAddr + proto.DecommissionDataNode + "?addr=" + addr
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
	reqUrl := "http://" + clusterAddr + proto.MigrateDataNode + "?srcAddr=" + srcAddr + "&targetAddr=" + targetAddr
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
	NodePort      = "17320"
	PathDisk      = "/disks"
	PathPartition = "/partitions"
)

type DisksData struct {
	Disks []DiskInfo `json:"disks"`
	Zone  string     `json:"zone"`
}

type DiskInfo struct {
	Path        string `json:"path"`
	Total       uint64 `json:"total"`
	Used        uint64 `json:"used"`
	Available   uint64 `json:"available"`
	Unallocated uint64 `json:"unallocated"`
	Allocated   uint64 `json:"allocated"`
	Status      int8   `json:"status"`
	RestSize    uint64 `json:"restSize"`
	DiskRdoSize uint64 `json:"diskRdoSize"`
	Partitions  int    `json:"partitions"`
}

func GetDisks(c *gin.Context, nodeAddr string) (*DisksData, error) {
	reqUrl := "http://" + nodeAddr + ":" + NodePort + PathDisk
	resp, err := httputils.DoRequestNoCookie(c, reqUrl, http.MethodGet, nil, nil)
	if err != nil {
		return nil, err
	}
	output := httputils.Output{Data: &DisksData{}}
	_, err = httputils.HandleResponse(c, resp, err, &output)
	if err != nil {
		return nil, err
	}
	if output.Code != http.StatusOK {
		return nil, errors.New(output.Msg)
	}
	return output.Data.(*DisksData), nil
}

type PartitionsData struct {
	Partitions []Partition `json:"partitions"`
}

type Partition struct {
	ID       uint64   `json:"id"`
	Size     uint64   `json:"size"`
	Used     uint64   `json:"used"`
	Status   int8     `json:"status"`
	Path     string   `json:"path"`
	Replicas []string `json:"replicas"`
}

func GetPartitions(c *gin.Context, nodeAddr string) ([]Partition, error) {
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
	return data.Partitions, nil
}
