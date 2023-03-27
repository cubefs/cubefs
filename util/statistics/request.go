package statistics

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
)

type HTTPReply struct {
	Code int32       `json:"code"`
	Msg  string      `json:"msg"`
	Data interface{} `json:"data"`
}

func (m *Statistics) sendToMonitor(data []byte) {
	url := fmt.Sprintf("http://%v%v", m.monitorAddr, MonitorCollect)
	reply, err := post(url, data)
	if err == nil && reply.Code == 0 {
		return
	}
	log.LogWarnf("sendToMonitor failed: reply[%v] err[%v] url[%v]", reply, err, url)
}

func post(reqURL string, data []byte) (reply *HTTPReply, err error) {

	var req *http.Request
	reader := bytes.NewReader(data)
	if req, err = http.NewRequest(http.MethodPost, reqURL, reader); err != nil {
		return
	}

	var resp *http.Response
	client := http.DefaultClient
	client.Timeout = 4 * time.Second
	if resp, err = client.Do(req); err != nil {
		return
	}

	body, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	if err != nil {
		return
	}
	if resp.StatusCode != http.StatusOK {
		err = errors.New(fmt.Sprintf("status code[%v]", resp.StatusCode))
		return
	}

	reply = &HTTPReply{}
	if err = json.Unmarshal(body, reply); err != nil {
		return
	}
	if reply.Code != 0 {
		err = errors.New(fmt.Sprintf("request failed, code[%v], msg[%v], data[%v]", reply.Code, reply.Msg, reply.Data))
		return
	}
	return
}
