package router

import (
	"github.com/gin-gonic/gin"

	"github.com/cubefs/cubefs/console/backend/config"
	"github.com/cubefs/cubefs/console/backend/handler/oplog"
)

type opLogRouter struct{}

func (o *opLogRouter) Register(engine *gin.Engine) {
	group := engine.Group(config.Conf.Prefix.Api + "/console")

	optypes := group.Group("/optypes")
	{
		optypes.POST("/create", oplog.CreateOpType)
		optypes.PUT("/update", oplog.UpdateOpType)
		optypes.GET("/list", oplog.ListOpType)
	}

	oplogs := group.Group("/oplogs")
	{
		oplogs.GET("/list", oplog.List)
	}
}
