package scheduler

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/chubaofs/chubaofs/util/log"
)

type SchedulerClient struct {
	schedulerDomain string
	enableHTTPS  	bool
	schedulerAPI 	*SchedulerAPI
}

func (sc *SchedulerClient) SchedulerAPI() *SchedulerAPI {
	return sc.schedulerAPI
}

func NewSchedulerClient(domain string, enableHTTPS bool) *SchedulerClient {
	var sc = &SchedulerClient{
		schedulerDomain: 	domain,
		enableHTTPS:  		enableHTTPS,
	}

	sc.schedulerAPI = &SchedulerAPI{sc: sc}
	return sc
}

func (sc *SchedulerClient) UpdateSchedulerDomain(domain string) {
	sc.schedulerDomain = domain
}

func (sc *SchedulerClient) httpRequest(method, url string, param, header map[string]string, reqData []byte) (resp *http.Response, err error) {
	client := http.DefaultClient
	client.Timeout = 5 * time.Second
	reader := bytes.NewReader(reqData)
	var req *http.Request
	fullUrl := sc.mergeRequestUrl(url, param)
	if req, err = http.NewRequest(method, fullUrl, reader); err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Connection", "close")
	for k, v := range header {
		req.Header.Set(k, v)
	}
	resp, err = client.Do(req)
	return
}

func (sc *SchedulerClient) mergeRequestUrl(url string, params map[string]string) string {
	if params != nil && len(params) > 0 {
		buff := bytes.NewBuffer([]byte(url))
		isFirstParam := true
		for k, v := range params {
			if isFirstParam {
				buff.WriteString("?")
				isFirstParam = false
			} else {
				buff.WriteString("&")
			}
			buff.WriteString(k)
			buff.WriteString("=")
			buff.WriteString(v)
		}
		return buff.String()
	}
	return url
}

func (sc *SchedulerClient) serveSchedulerRequest(r *request) (respData []byte, err error) {
	var (
		resp     *http.Response
		urlProto string
		url      string
	)
	requestAddr := sc.schedulerDomain
	if sc.enableHTTPS {
		urlProto = "https"
	} else {
		urlProto = "http"
	}

	url = fmt.Sprintf("%s://%s%s", urlProto, requestAddr, r.path)
	resp, err = sc.httpRequest(r.method, url, r.params, r.header, r.body)
	if err != nil {
		log.LogWarnf("serveRequest: send http request fail: method(%v) url(%v) err(%v)", r.method, url, err)
		return
	}
	respData, err = ioutil.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		log.LogWarnf("serveRequest: read http response body fail: err(%v)", err)
		return
	}
	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("send request err: status code[%v], req addr[%v], req path[%v]", resp.StatusCode, requestAddr, r.path)
		return
	}

	return
}