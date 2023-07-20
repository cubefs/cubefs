package scheduler

import (
	"errors"
	"io"
	"net/http"

	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/gin-gonic/gin"

	"github.com/cubefs/cubefs/console/backend/helper/httputils"
	"github.com/cubefs/cubefs/console/backend/service/blobstore/clustermgr/service"
)

func Request(c *gin.Context, method, consulAddr, path, query string, body io.Reader, header map[string]string) (*http.Response, error) {
	hosts, err := GetSchedulers(c, consulAddr)
	if err != nil {
		return nil, err
	}
	if len(hosts) == 0 {
		return nil, errors.New("scheduler host not found")
	}
	var resp *http.Response
	for _, host := range hosts {
		reqUrl := host + path
		if query != "" {
			reqUrl += "?" + query
		}
		resp, err = httputils.DoRequestBlobstore(c, reqUrl, method, body, header)
		if err == nil {
			return resp, nil
		}
	}
	return resp, err
}

func GetSchedulers(c *gin.Context, consulAddr string) ([]string, error) {
	hosts := make([]string, 0)
	nodes, err := service.List(c, consulAddr)
	if err != nil {
		return hosts, err
	}
	for _, node := range nodes {
		if node.Name == proto.ServiceNameScheduler {
			hosts = append(hosts, node.Host)
		}
	}
	return hosts, nil
}
