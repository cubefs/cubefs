package scheduler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/chubaofs/chubaofs/proto"
)

type SchedulerAPI struct {
	sc *SchedulerClient
}

func (api *SchedulerAPI) ReportDpMetrics(version uint8, cluster, vol, ip string, timestamp int64, body []byte) (err error) {
	var request = newAPIRequest(http.MethodPost, proto.ReportUrl)
	request.addParam("version", strconv.Itoa(int(version)))
	request.addParam("cluster", cluster)
	request.addParam("vol", vol)
	request.addParam("ip", ip)
	request.addParam("time", strconv.FormatInt(timestamp, 10))
	request.addBody(body)
	var reply []byte
	if reply, err = api.sc.serveSchedulerRequest(request); err != nil {
		return
	}
	httpReply := &HTTPReply{}
	if err = json.Unmarshal(reply, httpReply); err != nil {
		return
	}
	if httpReply.Code != 0 {
		err = fmt.Errorf("reply code(%v) msg(%v)", httpReply.Code, httpReply.Msg)
	}
	return
}

func (api *SchedulerAPI) GetDpMetrics(version uint8, cluster, vol, ip string, timestamp int64) (reply []byte, err error) {
	var request = newAPIRequest(http.MethodPost, proto.FetchUrl)
	request.addParam("version", strconv.Itoa(int(version)))
	request.addParam("cluster", cluster)
	request.addParam("vol", vol)
	request.addParam("ip", ip)
	request.addParam("time", strconv.FormatInt(timestamp, 10))
	if reply, err = api.sc.serveSchedulerRequest(request); err != nil {
		return
	}
	return
}