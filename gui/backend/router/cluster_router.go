package router

import (
	"github.com/cubefs/cubefs/console/backend/config"
	"github.com/cubefs/cubefs/console/backend/handler/cluster"
	"github.com/gin-gonic/gin"
)

type clusterRouter struct{}

func (c *clusterRouter) Register(engine *gin.Engine) {
	group := engine.Group(config.Conf.Prefix.Api + "/console")

	clusters := group.Group("/clusters")
	{
		clusters.POST("/create", cluster.Create)
		clusters.PUT("/update", cluster.Update)
		clusters.GET("/list", cluster.List)
	}
}