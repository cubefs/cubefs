package oplog

import (
	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/gin-gonic/gin"

	"github.com/cubefs/cubefs/console/backend/helper/codes"
	"github.com/cubefs/cubefs/console/backend/helper/ginutils"
	"github.com/cubefs/cubefs/console/backend/model"
)

type ListOutPut struct {
	Page    int                  `json:"page"`
	PerPage int                  `json:"per_page"`
	Count   int64                `json:"count"`
	Data    []model.OperationLog `json:"data"`
}

func List(c *gin.Context) {
	input := &model.FindOpLogParam{}
	if !ginutils.Check(c, input) {
		return
	}
	oplogs, count, err := new(model.OperationLog).Find(input)
	if err != nil {
		log.Errorf("oplog.Find failed. input:%+v, err:%+v", input, err)
		ginutils.Send(c, codes.DatabaseError.Code(), err.Error(), nil)
		return
	}
	output := &ListOutPut{
		Page:    input.Page,
		PerPage: input.PerPage,
		Count:   count,
		Data:    oplogs,
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), output)
}
