package router

import (
	"github.com/gin-gonic/gin"
)

type IRouter interface {
	Register(engine *gin.Engine)
}

func Register(engine *gin.Engine) {
	new(staticRouter).Register(engine)
	new(clusterRouter).Register(engine)
	new(blobRouter).Register(engine)
	new(cfsRouter).Register(engine)
	new(authRouter).Register(engine)
	new(opLogRouter).Register(engine)
}
