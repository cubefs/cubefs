package mdc

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

var (
	GetMethod    = "GET"
	PostMethod   = "POST"
	SendTypeForm = "form"
	SendTypeJson = "json"
)

type HttpSend struct {
	Link     string
	SendType string
	Header   map[string]string
	Body     map[string]interface{}
	timeout  time.Duration
	sync.RWMutex
}

func NewHttpSend(link string) *HttpSend {
	return &HttpSend{
		Link:     link,
		SendType: SendTypeForm,
		timeout:  300 * time.Second,
	}
}

func (h *HttpSend) SetTimeout(d time.Duration) {
	h.timeout = d
}

func (h *HttpSend) SetLink(link string) {
	h.Lock()
	defer h.Unlock()
	h.Link = link
}

func (h *HttpSend) SetBody(body map[string]interface{}) {
	h.Lock()
	defer h.Unlock()
	h.Body = body
}

func (h *HttpSend) SetJsonTypeBody(body map[string]interface{}) {
	h.Lock()
	defer h.Unlock()
	h.Body = body
	h.SendType = SendTypeJson
}

func (h *HttpSend) SetFormTypeBody(body map[string]interface{}) {
	h.Lock()
	defer h.Unlock()
	h.Body = body
	h.SendType = SendTypeForm
}

func (h *HttpSend) SetHeader(header map[string]string) {
	h.Lock()
	defer h.Unlock()
	h.Header = header
}

func (h *HttpSend) SetSendType(sendType string) {
	h.Lock()
	defer h.Unlock()
	h.SendType = sendType
}

func (h *HttpSend) Get() ([]byte, error) {
	return h.send(GetMethod)
}

func (h *HttpSend) Post() ([]byte, error) {
	return h.send(PostMethod)
}

func GetUrlBuild(link string, data map[string]string) string {
	u, _ := url.Parse(link)
	q := u.Query()
	for k, v := range data {
		q.Set(k, v)
	}
	u.RawQuery = q.Encode()
	return u.String()
}

func (h *HttpSend) send(method string) ([]byte, error) {
	var (
		req      *http.Request
		resp     *http.Response
		client   http.Client
		sendData string
		err      error
	)

	if len(h.Body) > 0 {
		if strings.ToLower(h.SendType) == SendTypeForm {
			sendBody := http.Request{}
			sendBody.ParseForm()
			for k, v := range h.Body {
				sendBody.Form.Add(k, v.(string))
			}
			sendData = sendBody.Form.Encode()
		} else {
			sendBody, jsonErr := json.Marshal(h.Body)
			if jsonErr != nil {
				return nil, jsonErr
			}
			sendData = string(sendBody)
		}
	}

	//忽略https的证书
	client.Transport = &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	req, err = http.NewRequest(method, h.Link, strings.NewReader(sendData))
	if err != nil {
		return nil, err
	}
	defer req.Body.Close()

	//设置默认header
	if len(h.Header) == 0 {
		//json
		if strings.ToLower(h.SendType) == SendTypeJson {
			h.Header = map[string]string{
				"Content-Type": "application/json; charset=utf-8",
			}
		} else { //form
			h.Header = map[string]string{
				"Content-Type": "application/x-www-form-urlencoded",
			}
		}
	}

	for k, v := range h.Header {
		if strings.ToLower(k) == "host" {
			req.Host = v
		} else {
			req.Header.Add(k, v)
		}
	}

	client.Timeout = h.timeout
	resp, err = client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, errors.New(fmt.Sprintf("error http code :%d", resp.StatusCode))
	}

	return ioutil.ReadAll(resp.Body)
}
