package api

import (
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/gin-gonic/gin"

	"github.com/cubefs/cubefs/console/backend/helper/httputils"
	"github.com/cubefs/cubefs/console/backend/service/consul"
)

func DoRequestBlobstore(c *gin.Context, consulAddr, path, method string, body io.Reader, extraHeaders map[string]string) (*http.Response, error) {
	id := c.Param("id")
	clusterId, err := strconv.Atoi(id)
	if err != nil {
		log.Errorf("parse id failed.id:%s,consul:%s,err:%+v", id, consulAddr, err)
		return nil, err
	}
	clusters, err := consul.GetRegionClustersMap(c, consulAddr)
	if err != nil {
		log.Errorf("consul.GetRegionClusters failed.consulAddr:%+s,err:+v", consulAddr, err)
		return nil, err
	}
	cluster, ok := clusters[clusterId]
	if !ok {
		log.Errorf("cluster_id not found. id:%s,clusters:%+v,consul:%s", id, clusters, consulAddr)
		return nil, fmt.Errorf("id:%s not found", id)
	}
	switch method {
	case http.MethodPost:
		if extraHeaders == nil {
			extraHeaders = map[string]string{}
		}
		extraHeaders["Content-Type"] = "application/json"
	}

	var resp *http.Response
	for _, node := range cluster.Nodes {
		resp, err = httputils.DoRequestNoCookie(c, node+path, method, body, extraHeaders)
		if err != nil {
			log.Errorf("do request failed.url:%s,err:%+v", node+path, err)
		} else {
			return resp, err
		}
	}
	return resp, err
}
