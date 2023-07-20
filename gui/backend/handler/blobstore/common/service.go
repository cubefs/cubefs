package common

import (
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/gin-gonic/gin"

	"github.com/cubefs/cubefs/console/backend/service/blobstore/clustermgr/service"
)

func GetProxyHost(c *gin.Context, consulAddr string) ([]string, error) {
	hosts := make([]string, 0)
	proxies, err := service.Get(c, consulAddr, proto.ServiceNameProxy)
	if err != nil {
		return hosts, err
	}
	for _, v := range proxies {
		hosts = append(hosts, v.Host)
	}
	return hosts, nil
}
