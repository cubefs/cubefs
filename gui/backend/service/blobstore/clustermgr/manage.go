package clustermgr

import (
	"bytes"
	"encoding/json"
	"net/http"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/gin-gonic/gin"

	"github.com/cubefs/cubefs/console/backend/helper/httputils"
	"github.com/cubefs/cubefs/console/backend/service/blobstore/api"
)

func Stat(c *gin.Context, addr string, isConsulAddr bool) (*clustermgr.StatInfo, error) {
	var (
		resp *http.Response
		err  error
	)
	if isConsulAddr {
		reqUrl := api.PathStat
		resp, err = api.DoRequestBlobstore(c, addr, reqUrl, http.MethodGet, nil, nil)
	} else {
		reqUrl := addr + api.PathStat
		resp, err = httputils.DoRequestBlobstore(c, reqUrl, http.MethodGet, nil, nil)
	}
	if err != nil {
		return nil, err
	}
	output := &clustermgr.StatInfo{}
	_, err = httputils.HandleResponse(c, resp, err, output)
	if err != nil {
		return nil, err
	}
	return output, nil
}

func LeadershipTransfer(c *gin.Context, consulAddr string, peerId uint64) (interface{}, error) {
	reqUrl := api.PathLeadershipTransfer
	b, _ := json.Marshal(map[string]uint64{"peer_id": peerId})
	resp, err := api.DoRequestBlobstore(c, consulAddr, reqUrl, http.MethodPost, bytes.NewReader(b), nil)
	if err != nil {
		return nil, err
	}
	var output interface{}
	_, err = httputils.HandleResponse(c, resp, err, &output)
	if err != nil {
		return nil, err
	}
	return output, nil
}

func MemberRemove(c *gin.Context, consulAddr string, peerId uint64) (interface{}, error) {
	reqUrl := api.PathMemberRemove
	b, _ := json.Marshal(map[string]uint64{"peer_id": peerId})
	resp, err := api.DoRequestBlobstore(c, consulAddr, reqUrl, http.MethodPost, bytes.NewReader(b), nil)
	if err != nil {
		return nil, err
	}
	var output interface{}
	_, err = httputils.HandleResponse(c, resp, err, &output)
	if err != nil {
		return nil, err
	}
	return output, nil
}
