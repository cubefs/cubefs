package proxy

import (
	"fmt"
	"net/http"

	"github.com/cubefs/cubefs/blobstore/api/proxy"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/gin-gonic/gin"

	"github.com/cubefs/cubefs/console/backend/helper/httputils"
	"github.com/cubefs/cubefs/console/backend/service/blobstore/api"
)

func GetVolumeList(c *gin.Context, proxyAddr string, code codemode.CodeMode) (*proxy.VolumeList, error) {
	reqUrl := proxyAddr + api.PathVolumeList + fmt.Sprintf("?code_mode=%d", uint8(code))
	resp, err := httputils.DoRequestBlobstore(c, reqUrl, http.MethodGet, nil, nil)
	if err != nil {
		return nil, err
	}
	output := &proxy.VolumeList{}
	_, err = httputils.HandleResponse(c, resp, err, output)
	if err != nil {
		return nil, err
	}
	return output, nil
}
