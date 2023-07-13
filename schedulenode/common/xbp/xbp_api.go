package xbp

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"
)

const (
	Erp                    = "wjcc_chubaofsapply" //API用户 需要申请 + 添加到对应的流程中
	Sign                   = "3abeaf2c12"
	Domain                 = "xbp-api.jd.com" //正式环境域名
	OfflineTicketProcessId = 5009
)

const (
	TicketStatusReject     = -1 // 驳回
	TicketStatusInProgress = 0  // 正在进行中
	TicketStatusFinish     = 1  // 完结
	TicketStatusCancel     = 2  // 已撤销
)

type CreateTicketResp struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
	Data struct {
		TicketID int `json:"ticketId"`
	} `json:"data"`
}

type GetTicketsStatusResp struct {
	Code int            `json:"code"`
	Msg  string         `json:"msg"`
	Data []TicketStatus `json:"data"`
}

type TicketStatus struct {
	Status   int `json:"status"` // -1 已被驳回, 0 正在进行中, 1	已完结, 2 已撤销
	TicketID int `json:"ticketId"`
}

// 创建申请单
func CreateTicket(processId int, domain, username, apiSign, apiUser string, applicationInfo map[string]string) (ticketId int, err error) {
	data, err := postToXBP(domain, "/ticket/create", apiSign, apiUser, map[string]interface{}{
		"processId":       fmt.Sprintf("%v", processId),
		"username":        username,
		"applicationInfo": applicationInfo,
	})
	if err != nil {
		return 0, fmt.Errorf("action[CreateTicket] data:%s err:%v", string(data), err)
	}
	resp := CreateTicketResp{}
	if err = json.Unmarshal(data, &resp); err != nil {
		return 0, fmt.Errorf("action[CreateTicket] data:%s err:%v", string(data), err)
	}
	if resp.Code != 0 {
		return 0, fmt.Errorf("action[CreateTicket] data:%s err:%v", string(data), err)
	}
	return resp.Data.TicketID, nil
}

// 批量获取申请单状态
func GetTicketsStatus(domain, apiSign, apiUser string, tickets []int) (ticketStatus []TicketStatus, err error) {
	data, err := postToXBP(domain, "/ticket/status", apiSign, apiUser, map[string]interface{}{
		"ids": tickets,
	})
	if err != nil {
		return nil, fmt.Errorf("action[GetTicketsStatus] data:%s err:%v", string(data), err)
	}
	resp := GetTicketsStatusResp{}
	if err = json.Unmarshal(data, &resp); err != nil {
		return nil, fmt.Errorf("action[GetTicketsStatus] data:%s err:%v", string(data), err)
	}
	if resp.Code != 0 {
		return nil, fmt.Errorf("action[GetTicketsStatus] data:%s err:%v", string(data), err)
	}
	return resp.Data, nil
}

func postToXBP(domain, path, apiSign, apiUser string, body map[string]interface{}) (data []byte, err error) {
	url := fmt.Sprintf("http://%s/api/api%s", domain, path)
	client := &http.Client{Timeout: time.Second * 5}
	if data, err = json.Marshal(body); err != nil {
		return
	}
	buff := bytes.NewBuffer(data)
	req, err := http.NewRequest(http.MethodPost, url, buff)
	if err != nil {
		return
	}
	sess, ts := session(apiSign)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Xbp-Api-User", apiUser)
	req.Header.Set("Xbp-Api-Session", sess)
	req.Header.Set("Xbp-Api-Timestamp", ts)
	resp, err := client.Do(req)
	if err != nil || resp.Body == nil {
		return
	}
	defer resp.Body.Close()
	if data, err = ioutil.ReadAll(resp.Body); err != nil {
		return
	}
	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("action[postToXBP] statusCode[%v],data[%v]", resp.StatusCode, string(data))
		return
	}
	return
}

func session(apiSign string) (string, string) {
	ts := fmt.Sprintf("%v", time.Now().Unix()*1000)
	hash := md5.Sum([]byte(ts + apiSign))
	sess := hex.EncodeToString(hash[:])
	return sess, ts
}
