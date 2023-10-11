package ump

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/util/log"
)

type UMPClient struct {
	domain string
	token  string
}

type AlarmRecordRequest struct {
	AppName     string `json:"appName"`
	Platform    string `json:"platform"`
	Provider    string `json:"provider"`
	Page        int    `json:"page"`
	PageSize    int    `json:"pageSize"`
	Endpoint    string `json:"endpoint"`
	MonitorType string `json:"monitorType"`
	StartTime   int64  `json:"startTime"`
	EndTime     int64  `json:"endTime"`
}

type AlarmRecordResponse struct {
	PageNum      int      `json:"pageNum"`
	PageSize     int      `json:"pageSize"`
	Records      []Record `json:"records"`
	TotalPage    int      `json:"totalPage"`
	TotalRecords int      `json:"totalRecords"`
}

type Record struct {
	AlarmTime int64 `json:"alarmTime"`
	Contacts  map[string][]string
	Content   string `json:"content"`
	Level     int    `json:"level"`
	Provider  string `json:"provider"`
	SentTime  int64  `json:"sentTime"`
	Status    int    `json:"status"`
}

func NewUmpClient(token, domain string) (ump *UMPClient) {
	return &UMPClient{
		token:  token,
		domain: domain,
	}
}

func (ump *UMPClient) GetAlarmRecords(method, appName, platform, endpoint string, startTimeStamp, endTimeStamp int64) (alarmRecordResp *AlarmRecordResponse, err error) {
	request := &AlarmRecordRequest{
		AppName:     appName,
		Platform:    platform,
		Provider:    "ump",
		Page:        1,
		PageSize:    100,
		Endpoint:    endpoint,
		MonitorType: "CUSTOM",
		StartTime:   startTimeStamp,
		EndTime:     endTimeStamp,
	}
	requestBuf, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	headers := make(map[string]string)
	headers["token"] = ump.token
	headers["Content-Type"] = "application/json"
	url := fmt.Sprintf("http://%v%v", ump.domain, method)
	log.LogInfof("GetAlarmRecords, url:%v", url)
	log.LogDebugf("GetAlarmRecords, body: %v", string(requestBuf))
	result, err := doPost(url, requestBuf, headers)
	if err != nil {
		return nil, err
	}
	alarmRecordResp = &AlarmRecordResponse{}
	err = json.Unmarshal(result, alarmRecordResp)
	if err != nil {
		return
	}
	return
}
