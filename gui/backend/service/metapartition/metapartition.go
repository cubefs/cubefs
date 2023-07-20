package metapartition

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/cubefs/cubefs/proto"
	"github.com/gin-gonic/gin"

	"github.com/cubefs/cubefs/console/backend/helper/httputils"
)

func Create(c *gin.Context, clusterAddr string, name string, start uint64) error {
	reqUrl := "http://" + clusterAddr + proto.AdminCreateMetaPartition + "?name=" + name + "&start=" + strconv.FormatUint(start, 10)
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
	reqUrl := "http://" + clusterAddr + proto.AdminLoadMetaPartition + "?id=" + id
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

func GetById(c *gin.Context, clusterAddr, id string) (*proto.MetaPartitionInfo, error) {
	reqUrl := "http://" + clusterAddr + proto.ClientMetaPartition + "?id=" + id
	resp, err := httputils.DoRequestNoCookie(c, reqUrl, http.MethodGet, nil, nil)
	if err != nil {
		return nil, err
	}
	output := httputils.Output{Data: &proto.MetaPartitionInfo{}}
	_, err = httputils.HandleResponse(c, resp, err, &output)
	if err != nil {
		return nil, err
	}
	if output.Code != proto.ErrCodeSuccess {
		return nil, errors.New(output.Msg) //
	}
	return output.Data.(*proto.MetaPartitionInfo), nil
}

func GetByName(c *gin.Context, clusterAddr, name string) (*[]proto.MetaPartitionView, error) {
	reqUrl := "http://" + clusterAddr + proto.ClientMetaPartitions + "?name=" + name
	resp, err := httputils.DoRequestNoCookie(c, reqUrl, http.MethodGet, nil, nil)
	if err != nil {
		return nil, err
	}
	output := httputils.Output{Data: &[]proto.MetaPartitionView{}}
	_, err = httputils.HandleResponse(c, resp, err, &output)
	if err != nil {
		return nil, err
	}
	if output.Code != proto.ErrCodeSuccess {
		return nil, errors.New(output.Msg)
	}
	return output.Data.(*[]proto.MetaPartitionView), nil
}

func Decommission(c *gin.Context, clusterAddr, id, addr string) (interface{}, error) {
	reqUrl := "http://" + clusterAddr + proto.AdminDecommissionMetaPartition + "?id=" + id + "&addr=" + addr
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

func Diagnosis(c *gin.Context, clusterAddr string) (*proto.MetaPartitionDiagnosis, error) {
	reqUrl := "http://" + clusterAddr + proto.AdminDiagnoseMetaPartition
	resp, err := httputils.DoRequestNoCookie(c, reqUrl, http.MethodGet, nil, nil)
	if err != nil {
		return nil, err
	}
	output := httputils.Output{Data: &proto.MetaPartitionDiagnosis{}}
	_, err = httputils.HandleResponse(c, resp, err, &output)
	if err != nil {
		return nil, err
	}
	if output.Code != proto.ErrCodeSuccess {
		return nil, errors.New(output.Msg) //
	}
	return output.Data.(*proto.MetaPartitionDiagnosis), nil
}