package compact

import (
	"github.com/cubefs/cubefs/proto"
	"testing"
)

func TestGenStopCompactUrl(t *testing.T) {
	ipPort := "127.0.0.1:17321"
	cluster := "compact-cluster"
	volumeName := "cmpvol"
	url := genStopCompactUrl(ipPort, cluster, volumeName)
	expectUrl := "http://127.0.0.1:17321/compact/stop?cluster=compact-cluster&vol=cmpvol"
	if url != expectUrl {
		t.Fatalf("genStopCompactUrl failed, expect:%v, actual:%v", expectUrl, url)
	}
}

func TestDoGet(t *testing.T) {
	reqUrl := "http://192.168.0.11:17010/admin/getIp"
	var res *proto.QueryHTTPResult
	var err error
	if res, err = doGet(reqUrl); err != nil {
		t.Errorf("doGet failed, reqUrl:%v, err:%v", reqUrl, err)
	}
	if res.Code != proto.ErrCodeSuccess {
		t.Fatalf("doGet reqUrl:%v, res code expect:%v, actual:%v", reqUrl, proto.ErrCodeSuccess, res.Code)
	}
}
