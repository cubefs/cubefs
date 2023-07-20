package router

import (
	"github.com/gin-gonic/gin"

	"github.com/cubefs/cubefs/console/backend/config"
	"github.com/cubefs/cubefs/console/backend/handler/datanode"
	"github.com/cubefs/cubefs/console/backend/handler/datapartition"
	"github.com/cubefs/cubefs/console/backend/handler/disk"
	"github.com/cubefs/cubefs/console/backend/handler/domain"
	"github.com/cubefs/cubefs/console/backend/handler/metanode"
	"github.com/cubefs/cubefs/console/backend/handler/metapartition"
	"github.com/cubefs/cubefs/console/backend/handler/s3"
	"github.com/cubefs/cubefs/console/backend/handler/user"
	"github.com/cubefs/cubefs/console/backend/handler/vol"
)

type cfsRouter struct{}

func (c *cfsRouter) Register(engine *gin.Engine) {
	group := engine.Group(config.Conf.Prefix.Api + "/console/cfs/:cluster")

	users := group.Group("/users")
	{
		users.POST("/create", user.Create)
		users.GET("/list", user.List)
		users.GET("/names", user.ListNames)
		users.POST("/policies", user.UpdatePolicy)
		users.GET("/vols/list", user.ListVols)
	}

	vols := group.Group("/vols")
	{
		vols.POST("/create", vol.Create)
		vols.GET("/list", vol.List)
		vols.GET("/info", vol.Info)
		vols.PUT("/update", vol.Update)
		vols.PUT("/expand", vol.Expand)
		vols.PUT("/shrink", vol.Shrink)
	}

	domains := group.Group("/domains")
	{
		domains.GET("/status", domain.Status)
		domains.GET("/info", domain.Info)
	}

	dataNodes := group.Group("/dataNode")
	{
		dataNodes.POST("/add", datanode.Add)
		dataNodes.GET("/list", datanode.List)
		dataNodes.GET("/partitions", datanode.Partitions)
		dataNodes.POST("/decommission", datanode.Decommission)
		dataNodes.POST("/migrate", datanode.Migrate)
	}

	metaNodes := group.Group("/metaNode")
	{
		metaNodes.POST("/add", metanode.Add)
		metaNodes.GET("/list", metanode.List)
		metaNodes.GET("/partitions", metanode.Partitions)
		metaNodes.POST("/decommission", metanode.Decommission)
		metaNodes.POST("/migrate", metanode.Migrate)
	}

	dataPartition := group.Group("/dataPartition")
	{
		dataPartition.POST("/create", datapartition.Create)
		dataPartition.GET("/load", datapartition.Load)
		dataPartition.GET("/list", datapartition.List)
		dataPartition.POST("/decommission", datapartition.Decommission)
		dataPartition.GET("/diagnosis", datapartition.Diagnosis)
	}

	metaPartition := group.Group("/metaPartition")
	{
		metaPartition.POST("/create", metapartition.Create)
		metaPartition.GET("/load", metapartition.Load)
		metaPartition.GET("/list", metapartition.List)
		metaPartition.POST("/decommission", metapartition.Decommission)
		metaPartition.GET("/diagnosis", metapartition.Diagnosis)
	}

	disks := group.Group("/disks")
	{
		disks.GET("/list", disk.List)
		disks.POST("/decommission", disk.Decommission)
	}

	s3R := group.Group("/s3")
	{
		s3R.GET("/vols/cors/get", s3.GetVolCors)
		s3R.PUT("/vols/cors/set", s3.PutVolCors)
		s3R.DELETE("/vols/cors/delete", s3.DeleteVolCors)
		s3R.GET("/files/list", s3.ListFile)
		s3R.GET("/files/download/signedUrl", s3.DownloadSignedUrl)
		s3R.GET("/files/upload/signedUrl", s3.UploadSignedUrl)
		s3R.GET("/files/upload/multipart/signedUrl", s3.MultiUploadSignedUrl)
		s3R.POST("/files/upload/multipart/complete", s3.MultiUploadComplete)
		s3R.POST("/dirs/create", s3.MakeDir)
	}
}
