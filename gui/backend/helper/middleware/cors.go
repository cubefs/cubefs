package middleware

import (
	"github.com/gin-gonic/gin"
	"net/http"
	"net/textproto"
	"strings"
)

var allowHeaders = []string{
	textproto.CanonicalMIMEHeaderKey("Content-Type"),
	textproto.CanonicalMIMEHeaderKey("Content-Length"),
	textproto.CanonicalMIMEHeaderKey("Date"),
	textproto.CanonicalMIMEHeaderKey("X-Request-With"),
	textproto.CanonicalMIMEHeaderKey("TraceId"),
	textproto.CanonicalMIMEHeaderKey("Accept-Language"),
	textproto.CanonicalMIMEHeaderKey("Accept-Encoding"),
}

func Cors() gin.HandlerFunc {
	return func(c *gin.Context) {
		method := c.Request.Method
		origin := c.Request.Header.Get(textproto.CanonicalMIMEHeaderKey("Origin"))
		if origin != "" {
			c.Header("Access-Control-Allow-Origin", origin)
			c.Header("Access-Control-Allow-Methods", "POST,GET,OPTIONS,PUT,DELETE,UPDATE,HEAD")
			c.Header("Access-Control-Allow-Headers", strings.Join(allowHeaders,","))
			c.Header("Access-Control-Expose-Headers", "*")
			c.Header("Access-Control-Allow-Credentials", "true")
		}
		if method == "OPTIONS" {
			c.AbortWithStatus(http.StatusNoContent)
		}
		c.Next()
	}
}
