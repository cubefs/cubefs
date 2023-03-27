package monitor

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strconv"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
)

func requestQuery(reqURL string) (reply *proto.QueryHTTPReply, err error) {
	log.LogInfof("requestQuery: url(%v)", reqURL)
	resp, err := http.Get(reqURL)
	if err != nil {
		return
	}
	body, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	if err != nil {
		return
	}
	if resp.StatusCode != http.StatusOK {
		err = errors.NewErrorf("monitor request: failed, response status code(%v) is not ok, url(%v)", resp.StatusCode, reqURL)
		return
	}
	reply = &proto.QueryHTTPReply{}
	if err = json.Unmarshal(body, reply); err != nil {
		return
	}
	return
}

func sendReply(w http.ResponseWriter, r *http.Request, httpReply *proto.HTTPReply) {
	log.LogInfof("URL[%v], remoteAddr[%v], response[%v]", r.URL, r.RemoteAddr, httpReply)
	reply, err := json.Marshal(httpReply)
	if err != nil {
		log.LogErrorf("fail to marshal http reply[%v]. URL[%v],remoteAddr[%v] err:[%v]", httpReply, r.URL, r.RemoteAddr, err)
		http.Error(w, "fail to marshal http reply", http.StatusBadRequest)
		return
	}
	w.Header().Set("content-type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(reply)))
	if _, err = w.Write(reply); err != nil {
		log.LogErrorf("fail to write http reply[%s] len[%d].URL[%v],remoteAddr[%v] err:[%v]", string(reply), len(reply), r.URL, r.RemoteAddr, err)
	}
	return
}
