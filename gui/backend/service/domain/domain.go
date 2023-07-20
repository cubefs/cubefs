package domain

import (
	"errors"
	"github.com/cubefs/cubefs/console/backend/helper/httputils"
	"github.com/cubefs/cubefs/proto"
	"github.com/gin-gonic/gin"
	"net/http"
)

type StatusOutput struct {
	DomainOn bool
}

func Status(c *gin.Context, clusterAddr string) (*StatusOutput, error) {
	reqUrl := "http://" + clusterAddr + proto.AdminGetIsDomainOn
	resp, err := httputils.DoRequestNoCookie(c, reqUrl, http.MethodGet, nil, nil)
	if err != nil {
		return nil, err
	}
	output := httputils.Output{Data: &StatusOutput{}}
	_, err = httputils.HandleResponse(c, resp, err, &output)
	if err != nil {
		return nil, err
	}
	if output.Code != proto.ErrCodeSuccess {
		return nil, errors.New(output.Msg)
	}
	return output.Data.(*StatusOutput), nil
}

func Info(c *gin.Context, clusterAddr string) (*proto.DomainNodeSetGrpInfoList, error) {
	reqUrl := "http://" + clusterAddr + proto.AdminGetAllNodeSetGrpInfo
	resp, err := httputils.DoRequestNoCookie(c, reqUrl, http.MethodGet, nil, nil)
	if err != nil {
		return nil, err
	}
	output := httputils.Output{Data: &proto.DomainNodeSetGrpInfoList{}}
	_, err = httputils.HandleResponse(c, resp, err, &output)
	if err != nil {
		return nil, err
	}
	if output.Code != proto.ErrCodeSuccess {
		return nil, errors.New(output.Msg)
	}
	return output.Data.(*proto.DomainNodeSetGrpInfoList), nil
}
