package middleware

import (
	"time"

	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/gin-gonic/gin"
)

func Logger() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		log.Infof(
			"[REQ_BEG] %s %s %15s",
			c.Request.Method,
			c.Request.URL.Path,
			c.ClientIP(),
		)

		c.Next()
		log.Infof(
			"[REQ_END] %v, size: %d",
			time.Now().Sub(start),
			c.Writer.Size(),
		)
	}
}
