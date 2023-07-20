package datapartition

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/cubefs/cubefs/proto"
	"github.com/gin-gonic/gin"

	"github.com/cubefs/cubefs/console/backend/helper/httputils"
)

func Create(c *gin.Context, clusterAddr string, name string, count int) error {
	reqUrl := "http://" + clusterAddr + proto.AdminCreateDataPartition + "?name=" + name + "&count=" + strconv.Itoa(count)
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
		return errors.New(output.Msg) //
	}
	return nil
}

func Load(c *gin.Context, clusterAddr, id string) error {
	reqUrl := "http://" + clusterAddr + proto.AdminLoadDataPartition + "?id=" + id
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
		return errors.New(output.Msg) //
	}
	return nil
}

func GetById(c *gin.Context, clusterAddr, id string) (*proto.DataPartitionInfo, error) {
	reqUrl := "http://" + clusterAddr + proto.AdminGetDataPartition + "?id=" + id
	resp, err := httputils.DoRequestNoCookie(c, reqUrl, http.MethodGet, nil, nil)
	if err != nil {
		return nil, err
	}
	output := httputils.Output{Data: &proto.DataPartitionInfo{}}
	_, err = httputils.HandleResponse(c, resp, err, &output)
	if err != nil {
		return nil, err
	}
	if output.Code != proto.ErrCodeSuccess {
		return nil, errors.New(output.Msg) //
	}
	return output.Data.(*proto.DataPartitionInfo), nil
}

func GetByName(c *gin.Context, clusterAddr, name string) ([]*proto.DataPartitionResponse, error) {
	reqUrl := "http://" + clusterAddr + proto.ClientDataPartitions + "?name=" + name
	resp, err := httputils.DoRequestNoCookie(c, reqUrl, http.MethodGet, nil, nil)
	if err != nil {
		return nil, err
	}
	output := httputils.Output{Data: &proto.DataPartitionsView{}}
	_, err = httputils.HandleResponse(c, resp, err, &output)
	if err != nil {
		return nil, err
	}
	if output.Code != proto.ErrCodeSuccess {
		return nil, errors.New(output.Msg)
	}
	data := output.Data.(*proto.DataPartitionsView)
	return data.DataPartitions, nil
}

func Decommission(c *gin.Context, clusterAddr, id, addr string) (interface{}, error) {
	reqUrl := "http://" + clusterAddr + proto.AdminDecommissionDataPartition + "?id=" + id + "&addr=" + addr
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
		return nil, errors.New(output.Msg) //
	}
	return output.Data, nil
}

func Diagnosis(c *gin.Context, clusterAddr string) (*proto.DataPartitionDiagnosis, error) {
	reqUrl := "http://" + clusterAddr + proto.AdminDiagnoseDataPartition
	resp, err := httputils.DoRequestNoCookie(c, reqUrl, http.MethodGet, nil, nil)
	if err != nil {
		return nil, err
	}
	output := httputils.Output{Data: &proto.DataPartitionDiagnosis{}}
	_, err = httputils.HandleResponse(c, resp, err, &output)
	if err != nil {
		return nil, err
	}
	if output.Code != proto.ErrCodeSuccess {
		return nil, errors.New(output.Msg) //
	}
	return output.Data.(*proto.DataPartitionDiagnosis), nil
}