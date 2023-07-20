package router

import (
	"github.com/cubefs/cubefs/console/backend/config"
	"github.com/cubefs/cubefs/console/backend/handler/blobstore"
	"github.com/cubefs/cubefs/console/backend/handler/blobstore/conf"
	"github.com/cubefs/cubefs/console/backend/handler/blobstore/disk"
	"github.com/cubefs/cubefs/console/backend/handler/blobstore/node"
	"github.com/cubefs/cubefs/console/backend/handler/blobstore/service"
	"github.com/cubefs/cubefs/console/backend/handler/blobstore/volume"
	"github.com/gin-gonic/gin"
)

type blobRouter struct{}

func (b *blobRouter) Register(engine *gin.Engine) {
	group := engine.Group(config.Conf.Prefix.Api + "/console/blobstore")

	region := group.Group("/:cluster")
	{
		region.GET("/clusters/list", blobstore.ListClusters)
	}

	group = group.Group("/:cluster/:id")
	{
		// overview
		group.GET("/stat", blobstore.Overview)

		// raft
		group.POST("/leadership/transfer", blobstore.LeadershipTransfer)
		group.POST("/member/remove", blobstore.MemberRemove)
	}

	nodes := group.Group("/nodes")
	{
		nodes.GET("/list", node.List)
		nodes.POST("/access", node.Access)
		nodes.POST("/drop", node.Drop)
		nodes.POST("/offline", node.Offline)
		nodes.POST("/config/reload", node.ConfigReload)
		nodes.GET("/config/info", node.ConfigInfo)
		nodes.GET("/config/failures", node.ConfigFailures)
	}

	volumes := group.Group("/volumes")
	{
		volumes.GET("/list", volume.List)
		volumes.GET("/writing/list", volume.WritingList)
		volumes.GET("/v2/list", volume.V2List)
		volumes.GET("/allocated/list", volume.AllocatedList)
		volumes.GET("/get", volume.Get)
	}

	disks := group.Group("/disks")
	{
		disks.GET("/list", disk.List)
		disks.GET("/info", disk.Info)
		disks.GET("/dropping/list", disk.DroppingList)
		disks.POST("/access", disk.Access)
		disks.POST("/set", disk.SetBroken)
		disks.POST("/drop", disk.Drop)
		disks.POST("/probe", disk.Probe)
		disks.GET("/stats/migrating", disk.StatMigrating)
	}

	configs := group.Group("/config")
	{
		configs.GET("/list", conf.List)
		configs.POST("/set", conf.Set)
	}

	services := group.Group("/services")
	{
		services.GET("/list", service.List)
		services.GET("/get", service.Get)
		services.POST("/offline", service.Offline)
	}
}
