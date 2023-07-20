package service

import (
	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/gin-gonic/gin"

	"github.com/cubefs/cubefs/console/backend/helper/codes"
	"github.com/cubefs/cubefs/console/backend/helper/ginutils"
	"github.com/cubefs/cubefs/console/backend/service/blobstore/clustermgr/service"
)

func List(c *gin.Context) {
	consulAddr, err := ginutils.GetConsulAddr(c)
	if err != nil {
		return
	}
	serviceNodes, err := service.List(c, consulAddr)
	if err != nil {
		log.Errorf("service.List failed.consulAddr:%s,err:%+v", consulAddr, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), serviceNodes)
}

type GetInput struct {
	Name string `form:"name" binding:"required"`
}

func Get(c *gin.Context) {
	input := &GetInput{}
	consulAddr, err := ginutils.CheckAndGetConsul(c, input)
	if err != nil {
		return
	}
	serviceNodes, err := service.Get(c, consulAddr, input.Name)
	if err != nil {
		log.Errorf("service.Get failed.consulAddr:%s,args:%+v,err:%+v", consulAddr, input, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), serviceNodes)
}

type OfflineInput struct {
	Name string `json:"name"`
	Host string `json:"host"`
}

func Offline(c *gin.Context) {
	input := &OfflineInput{}
	consulAddr, err := ginutils.CheckAndGetConsul(c, input)
	if err != nil {
		return
	}
	err = service.Unregister(c, consulAddr, &service.UnregisterInput{Name: input.Name, Host: input.Host})
	if err != nil {
		log.Errorf("service.Unregister failed.consulAddr:%s,args:%+v,err:%+v", consulAddr, input, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), nil)
}
