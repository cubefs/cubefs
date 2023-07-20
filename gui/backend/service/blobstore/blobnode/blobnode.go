package blobnode

import (
	"bytes"
	"encoding/json"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/cubefs/cubefs/console/backend/helper"
	"github.com/cubefs/cubefs/console/backend/helper/httputils"
	"github.com/cubefs/cubefs/console/backend/service/blobstore/api"
)

func DiskProbe(c *gin.Context, nodeAddr, path string) error {
	reqUrl := nodeAddr + api.PathDiskProbe
	b, _ := json.Marshal(map[string]string{"path": path})
	resp, err := httputils.DoRequestBlobstore(c, reqUrl, http.MethodPost, bytes.NewReader(b), nil)
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

type ConfigReloadInput struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func ConfigReload(c *gin.Context, nodeAddr string, input *ConfigReloadInput) error {
	reqUrl := nodeAddr + api.PathConfigReload + "?" + helper.BuildUrlParams(input)
	resp, err := httputils.DoRequestBlobstore(c, reqUrl, http.MethodPost, nil, nil)
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
