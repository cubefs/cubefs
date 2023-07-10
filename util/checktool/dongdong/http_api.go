package dongdong

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/util/log"
	"io/ioutil"
	"net/http"
	"time"
)

const (
	tokenExpires = 30 * 60 // 30 min
)

type DDAPI struct {
	domain  string
	token   string
	key     string
	secret  string
	country string
}

func NewDDAPI(domain, key, secret, country string) (api *DDAPI) {
	api = new(DDAPI)
	api.key = key
	api.secret = secret
	api.domain = domain
	api.country = country
	api.refreshToken()
	return
}

func (d *DDAPI) refreshToken() {
	d.getToken()
	ticker := time.NewTicker(30 * time.Minute)
	go func() {
		for {
			select {
			case <-ticker.C:
				d.getToken()
			}
		}
	}()
}

func (d *DDAPI) getToken() {
	for {
		token, alive, err := postRefreshToken(DDDomain, d.key, d.secret)
		if err != nil {
			log.LogWarnf("[getToken], err: %v", err.Error())
			time.Sleep(5 * time.Second)
			continue
		}
		if alive < tokenExpires {
			time.Sleep(5 * time.Second)
			continue
		}
		d.token = token
		break
	}
}

func (d *DDAPI) SendMsgToERP(erp, msg string) (err error) {
	return postSendERPTextMsg(d.domain, d.token, msg, erp, d.country)
}

func (d *DDAPI) SendMsgToGroup(gid int, msg string) (err error) {
	return postSendGroupTextMsg(d.domain, d.token, msg, gid)
}

type RefreshTokenResp struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
	Data struct {
		Token string `json:"access_token"`
		Time  int    `json:"effective_time"`
	} `json:"Data"`
}

func postRefreshToken(domain, key, secret string) (token string, alive int, err error) {
	url := fmt.Sprintf("http://%v/open-apis/v1/auth/get_access_token", domain)
	client := &http.Client{Timeout: time.Second * 5}

	body := struct {
		Key    string `json:"app_key"`
		Secret string `json:"app_secret"`
	}{
		Key:    key,
		Secret: secret,
	}

	var (
		data []byte
		req  *http.Request
	)
	data, err = json.Marshal(body)
	if err != nil {
		return
	}
	buff := bytes.NewBuffer(data)
	req, err = http.NewRequest(http.MethodPost, url, buff)
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Requested-Id", fmt.Sprintf("%v", time.Now().Nanosecond()))
	var resp *http.Response
	resp, err = client.Do(req)
	if err != nil || resp.Body == nil {
		return
	}
	defer resp.Body.Close()
	if data, err = ioutil.ReadAll(resp.Body); err != nil {
		return
	}
	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("[postRefreshToken], statusCode[%v],Data[%v]", resp.StatusCode, string(data))
		return
	}
	var tokenBody RefreshTokenResp
	err = json.Unmarshal(data, &tokenBody)
	if err != nil {
		err = fmt.Errorf("action[postRefreshToken] err: %v", err.Error())
		return
	}
	token = tokenBody.Data.Token
	alive = tokenBody.Data.Time
	return
}

type ERPTextMsgBody struct {
	To struct {
		Pin string `json:"pin"`
		App string `json:"app"`
	} `json:"to"`
	Body struct {
		MsgType string `json:"type"`
		Content string `json:"content"`
	} `json:"body"`
}

func postSendERPTextMsg(domain, token, msg, erp, country string) (err error) {
	body := &ERPTextMsgBody{
		To: struct {
			Pin string `json:"pin"`
			App string `json:"app"`
		}{Pin: erp, App: country},
		Body: struct {
			MsgType string `json:"type"`
			Content string `json:"content"`
		}{MsgType: "text", Content: msg},
	}

	var data []byte
	data, err = json.Marshal(body)
	if err != nil {
		log.LogWarnf("[postSendERPTextMsg], err: %v", err.Error())
		err = nil
		return
	}
	return postSendMsg(domain, token, data)
}

type GroupTextMsgBody struct {
	Gid  int `json:"gid"`
	Body struct {
		MsgType string `json:"type"`
		Content string `json:"content"`
	} `json:"body"`
}

func postSendGroupTextMsg(domain, token, msg string, gid int) (err error) {
	body := &GroupTextMsgBody{
		Gid: gid,
		Body: struct {
			MsgType string `json:"type"`
			Content string `json:"content"`
		}{MsgType: "text", Content: msg},
	}
	var data []byte
	data, err = json.Marshal(body)
	if err != nil {
		log.LogWarnf("[postSendGroupTextMsg], err: %v", err.Error())
		err = nil
		return
	}
	return postSendMsg(domain, token, data)
}

func postSendMsg(domain, token string, data []byte) (err error) {
	url := fmt.Sprintf("http://%v/open-apis/v1/messages/robot", domain)
	client := &http.Client{Timeout: time.Second * 5}

	var req *http.Request
	buff := bytes.NewBuffer(data)
	req, err = http.NewRequest(http.MethodPost, url, buff)
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", token)
	req.Header.Set("X-Requested-Id", fmt.Sprintf("%v", time.Now().Nanosecond()))
	var resp *http.Response
	resp, err = client.Do(req)
	if err != nil || resp.Body == nil {
		return
	}
	defer resp.Body.Close()
	_, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		err = fmt.Errorf("[postSendMsg], err: %v", err.Error())
		return
	}
	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("[postSendMsg], httpCode: %v", resp.StatusCode)
		return
	}
	return
}
