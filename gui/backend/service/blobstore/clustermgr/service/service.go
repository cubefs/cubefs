package service

import (
	"bytes"
	"encoding/json"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/gin-gonic/gin"
	"net/http"

	"github.com/cubefs/cubefs/console/backend/helper/httputils"
	"github.com/cubefs/cubefs/console/backend/service/blobstore/api"
)

func List(c *gin.Context, consulAddr string) ([]clustermgr.ServiceNode, error) {
	reqUrl := api.PathServiceList
	resp, err := api.DoRequestBlobstore(c, consulAddr, reqUrl, http.MethodGet, nil, nil)
	if err != nil {
		return nil, err
	}
	output := &clustermgr.ServiceInfo{Nodes: make([]clustermgr.ServiceNode, 0)}
	_, err = httputils.HandleResponse(c, resp, err, output)
	if err != nil {
		return nil, err
	}
	return output.Nodes, nil
}

func Get(c *gin.Context, consulAddr, name string) ([]clustermgr.ServiceNode, error) {
	reqUrl := api.PathServiceGet + "?name=" + name
	resp, err := api.DoRequestBlobstore(c, consulAddr, reqUrl, http.MethodGet, nil, nil)
	if err != nil {
		return nil, err
	}
	output := &clustermgr.ServiceInfo{Nodes: make([]clustermgr.ServiceNode, 0)}
	_, err = httputils.HandleResponse(c, resp, err, output)
	if err != nil {
		return nil, err
	}
	return output.Nodes, nil
}

type UnregisterInput struct {
	Name string `json:"name"`
	Host string `json:"host"`
}

func Unregister(c *gin.Context, consulAddr string, input *UnregisterInput) error {
	reqUrl := api.PathServiceList
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
