package disk

import (
	"errors"
	"net/http"

	"github.com/cubefs/cubefs/proto"
	"github.com/gin-gonic/gin"

	"github.com/cubefs/cubefs/console/backend/helper/httputils"
)

func Decommission(c *gin.Context, clusterAddr, addr, disk string) error {
	reqUrl := "http://" + clusterAddr + proto.DecommissionDisk + "?addr=" + addr + "&disk=" + disk
	resp, err := httputils.DoRequestNoCookie(c, reqUrl, http.MethodGet, nil, nil)
	if err != nil {
		return err
	}
	output := httputils.Output{}
	_, err = httputils.HandleResponse(c, resp, err, &output)
	if err != nil {
		return err
	}
	if output.Code != proto.ErrCodeSuccess {
		return errors.New(output.Msg)
	}
	return nil
}
