package middleware

import (
	"github.com/cubefs/cubefs/console/backend/config"
	"github.com/gin-gonic/gin"
	"net/http"
)

func Default() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()
		conf := config.Conf.Server.StaticResource
		if !conf.Enable {
			return
		}
		if c.Request.Method == http.MethodGet && c.Writer.Status() == http.StatusNotFound {
			c.HTML(http.StatusOK, "index.html", nil)
		}
	}
}
