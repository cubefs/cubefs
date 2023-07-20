package domain

import (
	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/gin-gonic/gin"

	"github.com/cubefs/cubefs/console/backend/helper/codes"
	"github.com/cubefs/cubefs/console/backend/helper/ginutils"
	"github.com/cubefs/cubefs/console/backend/service/domain"
)

func Status(c *gin.Context) {
	addr, err := ginutils.GetClusterMaster(c)
	if err != nil {
		return
	}
	out, err := domain.Status(c, addr)
	if err != nil {
		log.Errorf("domain.Status failed. addr:%+v,err:%+v", addr, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), out)
}

func Info(c *gin.Context) {
	addr, err := ginutils.GetClusterMaster(c)
	if err != nil {
		return
	}
	out, err := domain.Info(c, addr)
	if err != nil {
		log.Errorf("domain.Info failed. addr:%+v,err:%+v", addr, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), out)
}
