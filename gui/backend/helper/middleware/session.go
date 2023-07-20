package middleware

import (
	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/gin-contrib/sessions"
	"github.com/gin-gonic/gin"

	"github.com/cubefs/cubefs/console/backend/helper/enums"
)

func Session() gin.HandlerFunc  {
	return func(c *gin.Context) {
		// check whether the user session is valid
		session := sessions.Default(c)
		sessionId, err := c.Cookie("sessionId")
		if err != nil {
			log.Errorf("c.Cookie err: %+v", err)
			c.Next()
			return
		}
		sessionData := session.Get(sessionId)
		if sessionData == nil {
			log.Error("sessionData is nil")
			c.Next()
			return
		}
		c.Set(enums.LoginUser, sessionData)
		c.Next()
	}
}
