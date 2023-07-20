package router

import (
	"github.com/gin-gonic/gin"

	"github.com/cubefs/cubefs/console/backend/config"
	"github.com/cubefs/cubefs/console/backend/helper/defaults"
)

type staticRouter struct{}

func (s *staticRouter) Register(e *gin.Engine) {
	conf := config.Conf.Server.StaticResource
	if !conf.Enable {
		return
	}
	relative := defaults.Str(conf.RelativePath, "/portal")
	root := defaults.Str(conf.RootPath, "../frontend/dist")
	e.LoadHTMLGlob(root + "/*.html")
	e.Static(relative, root)
}
