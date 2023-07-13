package common

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

var (
	//"http://api.storage.sre.jd.local/openApi/sendMail"
	Host = "api.storage.sre.jd.local"
	EmailPath = "/openApi/sendMail"
	DefaultRequestTimeOut = time.Second * 60
)

func SendEmail(subject, content string, mailTo []string) (err error) {
	if len(mailTo) == 0 {
		return
	}
	var (
		url    string
		body   []byte
		header map[string]string
	)
	req := &struct{
		Content string   `json:"content"`
		MailTo  []string `json:"mailTo"`
		Subject string   `json:"subject"`
	}{
		Content: content,
		MailTo:  mailTo,
		Subject: subject,
	}
	if body, err = json.Marshal(req); err != nil {
		return
	}

	header = map[string]string{
		"Content-Type": "application/json",
		"accept":       "application/json",
	}

	url = fmt.Sprintf("http://%s%s", Host, EmailPath)
	if _, err = httpRequest("POST", url, header, body); err != nil {
		err = fmt.Errorf("send email failed, request(%v) failed:%v", req, err)
		return
	}
	return
}

func httpRequest(method, url string, header map[string]string, body []byte) (resp * http.Response, err error) {
	client := http.DefaultClient
	client.Timeout = DefaultRequestTimeOut

	var req *http.Request
	if req, err = http.NewRequest(method, url, bytes.NewReader(body)); err != nil {
		return
	}

	for k, v := range header {
		req.Header.Set(k, v)
	}
	return client.Do(req)
}