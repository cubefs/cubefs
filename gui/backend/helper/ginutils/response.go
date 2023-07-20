package ginutils

import (
	"github.com/cubefs/cubefs/console/backend/helper/codes"
	"net/http"

	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/gin-gonic/gin"
)

type Result struct {
	Code    int         `json:"code"`
	Message string      `json:"msg,omitempty"`
	Data    interface{} `json:"data,omitempty"`
}

func Send(ctx *gin.Context, code int, msg string, data interface{}) {
	log.Infof("[REQ_CODE] %d", code)
	sendResponse(ctx, code, msg, data)
}

func Success(ctx *gin.Context, data interface{}) {
	Send(ctx, codes.OK.Code(), codes.OK.Msg(), data)
}

func sendResponse(ctx *gin.Context, code int, msg string, data interface{}) {
	ctx.JSON(http.StatusOK, &Result{
		Code:    code,
		Message: msg,
		Data:    data,
	})
}
