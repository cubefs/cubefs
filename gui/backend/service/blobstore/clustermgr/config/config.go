package config

import (
	"bytes"
	"encoding/json"
	"net/http"

	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/gin-gonic/gin"

	"github.com/cubefs/cubefs/console/backend/helper/httputils"
	"github.com/cubefs/cubefs/console/backend/service/blobstore/api"
)

func Get(c *gin.Context, consulAddr string, key string) ([]codemode.Policy, error) {
	reqUrl := api.PathConfigGet
	if key != "" {
		reqUrl += "?key=" + key
	}
	resp, err := api.DoRequestBlobstore(c, consulAddr, reqUrl, http.MethodGet, nil, nil)
	if err != nil {
		return nil, err
	}
	var output string
	_, err = httputils.HandleResponse(c, resp, err, &output)
	if err != nil {
		return nil, err
	}
	conf := make([]codemode.Policy, 0)
	err = json.Unmarshal([]byte(output), &conf)
	return conf, err
}

type SetInput struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func Set(c *gin.Context, consulAddr string, input *SetInput) error {
	reqUrl := api.PathConfigSet
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
