package disk

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/gin-gonic/gin"

	"github.com/cubefs/cubefs/console/backend/helper"
	"github.com/cubefs/cubefs/console/backend/helper/httputils"
	"github.com/cubefs/cubefs/console/backend/service/blobstore/api"
)

type ListInput struct {
	Idc    string `json:"idc"`
	Rack   string `json:"rack"`
	Host   string `json:"host"`
	Status uint8  `json:"status"`
	Marker uint32 `json:"marker"`
	Count  int    `json:"count"`
}

func List(c *gin.Context, consulAddr string, input *ListInput) (*clustermgr.ListDiskRet, error) {
	reqUrl := api.PathDiskList + "?" + helper.BuildUrlParams(input)
	resp, err := api.DoRequestBlobstore(c, consulAddr, reqUrl, http.MethodGet, nil, nil)
	if err != nil {
		return nil, err
	}
	output := &clustermgr.ListDiskRet{Disks: make([]*blobnode.DiskInfo, 0)}
	_, err = httputils.HandleResponse(c, resp, err, output)
	if err != nil {
		return nil, err
	}
	return output, nil
}

func Info(c *gin.Context, consulAddr string, diskId uint32) (*blobnode.DiskInfo, error) {
	reqUrl := api.PathDiskInfo + "?" + fmt.Sprintf("disk_id=%d", diskId)
	resp, err := api.DoRequestBlobstore(c, consulAddr, reqUrl, http.MethodGet, nil, nil)
	if err != nil {
		return nil, err
	}
	output := &blobnode.DiskInfo{}
	_, err = httputils.HandleResponse(c, resp, err, output)
	if err != nil {
		return nil, err
	}
	return output, nil
}

func DroppingList(c *gin.Context, consulAddr string) (*clustermgr.ListDiskRet, error) {
	reqUrl := api.PathDiskDroppingList
	resp, err := api.DoRequestBlobstore(c, consulAddr, reqUrl, http.MethodGet, nil, nil)
	if err != nil {
		return nil, err
	}
	output := &clustermgr.ListDiskRet{Disks: make([]*blobnode.DiskInfo, 0)}
	_, err = httputils.HandleResponse(c, resp, err, output)
	if err != nil {
		return nil, err
	}
	return output, nil
}

type SetInput struct {
	DiskId uint32           `json:"disk_id"`
	Status proto.DiskStatus `json:"status"`
}

func Set(c *gin.Context, consulAddr string, input *SetInput) error {
	reqUrl := api.PathDiskSet
	b, _ := json.Marshal(input)
	resp, err := api.DoRequestBlobstore(c, consulAddr, reqUrl, http.MethodPost, bytes.NewReader(b), nil)
	if err != nil {
		return err
	}
	var output interface{}
	_, err = httputils.HandleResponse(c, resp, err, &output)
	if err != nil {
		return err
	}
	return nil
}

type AccessInput struct {
	DiskId   uint32 `json:"disk_id"`
	Readonly bool   `json:"readonly"`
}

func Access(c *gin.Context, consulAddr string, input *AccessInput) error {
	reqUrl := api.PathDiskAccess
	b, _ := json.Marshal(input)
	resp, err := api.DoRequestBlobstore(c, consulAddr, reqUrl, http.MethodPost, bytes.NewReader(b), nil)
	if err != nil {
		return err
	}
	var output interface{}
	_, err = httputils.HandleResponse(c, resp, err, &output)
	if err != nil {
		return err
	}
	return nil
}

func Drop(c *gin.Context, consulAddr string, diskId uint32) error {
	reqUrl := api.PathDiskDrop
	b, _ := json.Marshal(map[string]uint32{"disk_id": diskId})
	resp, err := api.DoRequestBlobstore(c, consulAddr, reqUrl, http.MethodPost, bytes.NewReader(b), nil)
	if err != nil {
		return err
	}
	var output interface{}
	_, err = httputils.HandleResponse(c, resp, err, &output)
	if err != nil {
		return err
	}
	return nil
}
