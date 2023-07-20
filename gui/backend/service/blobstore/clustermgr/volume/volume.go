package volume

import (
	"fmt"
	"net/http"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/gin-gonic/gin"

	"github.com/cubefs/cubefs/console/backend/helper"
	"github.com/cubefs/cubefs/console/backend/helper/httputils"
	"github.com/cubefs/cubefs/console/backend/service/blobstore/api"
)

func Get(c *gin.Context, consulAddr string, vid uint32) (*clustermgr.VolumeInfo, error) {
	reqUrl := api.PathVolumeGet + "?" + fmt.Sprintf("vid=%d", vid)
	resp, err := api.DoRequestBlobstore(c, consulAddr, reqUrl, http.MethodGet, nil, nil)
	if err != nil {
		return nil, err
	}
	output := &clustermgr.VolumeInfo{}
	_, err = httputils.HandleResponse(c, resp, err, output)
	if err != nil {
		return nil, err
	}
	return output, nil
}

type ListInput struct {
	Marker int `json:"marker"`
	Count  int `json:"count"`
}

func List(c *gin.Context, consulAddr string, input *ListInput) (*clustermgr.ListVolumes, error) {
	reqUrl := api.PathVolumeList + "?" + helper.BuildUrlParams(input)
	resp, err := api.DoRequestBlobstore(c, consulAddr, reqUrl, http.MethodGet, nil, nil)
	if err != nil {
		return nil, err
	}
	output := &clustermgr.ListVolumes{Volumes: make([]*clustermgr.VolumeInfo, 0)}
	_, err = httputils.HandleResponse(c, resp, err, output)
	if err != nil {
		return nil, err
	}
	return output, nil
}

func V2List(c *gin.Context, consulAddr string, status int) (*clustermgr.ListVolumes, error) {
	reqUrl := api.PathV2VolumeList + "?" + fmt.Sprintf("status=%d", status)
	resp, err := api.DoRequestBlobstore(c, consulAddr, reqUrl, http.MethodGet, nil, nil)
	if err != nil {
		return nil, err
	}
	output := &clustermgr.ListVolumes{Volumes: make([]*clustermgr.VolumeInfo, 0)}
	_, err = httputils.HandleResponse(c, resp, err, output)
	if err != nil {
		return nil, err
	}
	return output, nil
}

type AllocatedListInput struct {
	Host     string `json:"host"`
	CodeMode uint8  `json:"code_mode"`
}

func AllocatedList(c *gin.Context, consulAddr string, input *AllocatedListInput) ([]clustermgr.AllocVolumeInfo, error) {
	reqUrl := api.PathVolumeAllocatedList + "?" + helper.BuildUrlParams(input)
	resp, err := api.DoRequestBlobstore(c, consulAddr, reqUrl, http.MethodGet, nil, nil)
	if err != nil {
		return nil, err
	}
	output := &clustermgr.AllocatedVolumeInfos{AllocVolumeInfos: make([]clustermgr.AllocVolumeInfo, 0)}
	_, err = httputils.HandleResponse(c, resp, err, output)
	if err != nil {
		return nil, err
	}
	return output.AllocVolumeInfos, nil
}
