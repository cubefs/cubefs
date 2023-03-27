package hbase

import (
	"bytes"
	"github.com/cubefs/cubefs/util/log"
	"net/http"
	netUrl "net/url"
	"strconv"
	"time"
)

type HttpBaseClent struct {
	Timeout time.Duration
}

func NewHttpBaseClient(timeout time.Duration) *HttpBaseClent {
	return &HttpBaseClent{
		Timeout: timeout,
	}
}

func (hc *HttpBaseClent) HttpRequest(method, url string, param, header map[string]string, reqData []byte) (resp *http.Response, err error, timeout bool) {
	client := http.DefaultClient
	reader := bytes.NewReader(reqData)
	if header["isTimeOut"] != "" {
		var isTimeOut bool
		if isTimeOut, err = strconv.ParseBool(header["isTimeOut"]); err != nil {
			return
		}
		if isTimeOut {
			client.Timeout = hc.Timeout
		}
	} else {
		client.Timeout = hc.Timeout
	}
	var req *http.Request
	fullUrl := hc.mergeRequestUrl(url, param)
	log.LogDebugf("HttpRequest: merge request url: method(%v) url(%v) timeout(%v) bodyLength[%v].", method, fullUrl, client.Timeout, len(reqData))
	if req, err = http.NewRequest(method, fullUrl, reader); err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Connection", "close")
	for k, v := range header {
		req.Header.Set(k, v)
	}
	resp, err = client.Do(req)
	if err != nil {
		timeout = err.(*netUrl.Error).Timeout()
	}
	return
}

func (hc *HttpBaseClent) mergeRequestUrl(url string, params map[string]string) string {
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
