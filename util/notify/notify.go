package notify

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/util/config"
	"io/ioutil"
	"net/http"
	"time"
)

const (
	NotifyAPIUrlToDongdongExt = "http://notifyservice.jd.local/api/v1/notify/timline/ext"
	NotifyAPIUrlToEmail       = "http://notifyservice.jd.local/api/v1/notify/mail"
	NotifyAPIUrlToCall        = "http://notifyservice.jd.local/api/v1/notify/call"
	NotifyAPIToken            = "bc01aad756ee48077dcc92a7ab637c5a"
	PrefixSubject             = "[警告]"
)

// DongdongExtRequestBody 咚咚报警请求数据
type DongdongExtRequestBody struct {
	Content  string   `json:"content"`
	URL      string   `json:"url"`
	Timlines []string `json:"timlines"`
	Subject  string   `json:"subject"`
	// AccountName string            `json:"accountName"`
	SendType string  `json:"sendType"`
	Labels   *Labels `json:"labels"`
}

// EmailRequestBody 邮件报警请求数据
type EmailRequestBody struct {
	Content         string           `json:"content"`
	Subject         string           `json:"subject"`
	To              []string         `json:"to"`
	Cc              []string         `json:"cc"`
	Bcc             []string         `json:"bcc"`
	IsHTML          bool             `json:"isHtml"`
	ReplyTo         string           `json:"replyTo"`
	Personal        string           `json:"personal"`
	Labels          *Labels          `json:"labels"`
	EmailSmtpConfig *EmailSmtpConfig `json:"EmailSmtpConfig"`
}

// EmailSmtpConfig SMTP配置
type EmailSmtpConfig struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Port     uint   `json:"port"`
	Host     string `json:"host"`
	Auth     bool   `json:"auth"`
	Starttls bool   `json:"starttls"`
}

// CallRequestBody 电话报警请求数据
type CallRequestBody struct {
	Content  []interface{} `json:"contacts"`
	Platform string        `json:"platform"`
	Node     string        `json:"detail"`
	Labels   *Labels       `json:"labels"`
}

// ResponseResult Response
type ResponseResult struct {
	Code    int         `json:"code"`
	Data    interface{} `json:"data"`
	Msg     string      `json:"msg"`
	Request interface{} `json:"request"`
}

// Member 成员信息
type Member struct {
	Name       string   `json:"name"`
	Erp        string   `json:"erp"`
	Email      string   `json:"email"`
	Phone      string   `json:"phone"`
	Role       string   `json:"role"`
	Scheduling []int    `json:"scheduling"`
	Systems    []string `json:"systems"`
}

type Labels struct {
	System    string `json:"system"`
	App       string `json:"app"`
	Module    string `json:"module"`
	Alarmtype string `json:"alarmtype"`
	Localip   string `json:"localip"`
	Remoteip  string `json:"remoteip"`
}

// Notify Notify信息
type Notify struct {
	urlToDongdong      string
	urlToEmail         string
	urlToCall          string
	token              string
	Subject            string                  `json:"subject"`
	Content            string                  `json:"content"`
	AlarmSync          bool                    `json:"alarmsync"`
	Labels             *Labels                 `json:"labels"`
	Mails              []string                `json:"mails"`
	Phones             []string                `json:"phones"`
	Erps               []string                `json:"erps"`
	DongdongExtRequest *DongdongExtRequestBody `json:"dongdongExtRequest"`
	EmailRequest       *EmailRequestBody       `json:"emailRequest"`
	CallRequest        *CallRequestBody        `json:"callRequest"`
	ResponseResult     *ResponseResult         `json:"responseResult"`
}

type AlarmErpsRules struct {
	Role       string `json:"role" gorm:"column:role"`
	Content    string `json:"content" gorm:"column:content"`
	Uname      string `json:"uname" gorm:"column:uname"`
	UnameCN    string `json:"uname_cn" gorm:"column:uname_cn"`
	Cname      string `json:"cname" gorm:"column:cname"`
	Trash      int    `json:"trash" gorm:"column:trash"`
	Department string `json:"extra_str1" gorm:"column:extra_str1"`
}

func newDongdongExtRequestBody() *DongdongExtRequestBody {
	rb := &DongdongExtRequestBody{
		Timlines: make([]string, 0),
		Labels:   &Labels{},
	}
	return rb
}

func newEmailRequestBody() *EmailRequestBody {
	rb := &EmailRequestBody{
		To:     make([]string, 0),
		Cc:     make([]string, 0),
		Bcc:    make([]string, 0),
		Labels: &Labels{},
	}
	return rb
}

func newCallRequestBody() *CallRequestBody {
	rb := &CallRequestBody{
		Content: make([]interface{}, 0),
		Labels:  &Labels{},
	}
	return rb
}

func NewNotify(conf *config.NotifyConfig) *Notify {
	nt := &Notify{
		urlToDongdong: conf.URLToDongDong,
		urlToEmail:    conf.URLToEmail,
		urlToCall:     conf.URLToCall,
		token:         conf.Token,
		Mails:         make([]string, 0),
		Phones:        make([]string, 0),
		Erps:          make([]string, 0),
	}
	nt.AlarmSync = true
	nt.Labels = &Labels{}
	nt.DongdongExtRequest = newDongdongExtRequestBody()
	nt.EmailRequest = newEmailRequestBody()
	nt.CallRequest = newCallRequestBody()
	nt.ResponseResult = &ResponseResult{}
	return nt
}

func (n *Notify) SetAlarmErps(names []string) {
	n.Erps = append(n.Erps, names...)
}

func (n *Notify) SetAlarmEmails(members []string) {
	n.Mails = append(n.Mails, members...)
}

func (n *Notify) SetAlarmPhones(phones []string) {
	n.Phones = append(n.Phones, phones...)
}

// AlarmToAll 针对所有渠道报警
func (n *Notify) AlarmToAll(subject string, content string, url string) {
	n.AlarmToEmail(subject, content)
	n.AlarmToDongdong(subject, content, url)
	n.AlarmToCall(subject)
}

// AlarmToEmail 针对邮件渠道报警
func (n *Notify) AlarmToEmail(subject string, content string) {
	if n.urlToEmail == "" || n.token == "" {
		return
	}
	n.EmailRequest.To = n.Mails
	n.EmailRequest.Subject = fmt.Sprintf("%s %s", PrefixSubject, subject)
	nowt := time.Now().Format("2006-01-02 15:04:05")
	n.EmailRequest.Content = fmt.Sprintf("【警告】%s\n【时间】%s", content, nowt)
	n.EmailRequest.Labels = n.Labels
	data, err := json.Marshal(n.EmailRequest)

	if err != nil {
		return
	}
	rst, err := n.DoPostNotifyAPI(n.urlToEmail, data)
	if err != nil {
		fmt.Printf("email call failed:%v", err)
		return
	}
	if rst.Code != 1 {
		fmt.Printf("email call failed, result:%v", rst)
		return
	}
	return
}

func (n *Notify) AlarmToEmailWithHtmlContent(subject string, content string) {
	if n.urlToEmail == "" || n.token == "" {
		return
	}
	n.EmailRequest.To = n.Mails
	n.EmailRequest.Subject = fmt.Sprintf("%s %s", PrefixSubject, subject)
	n.EmailRequest.Content = content
	n.EmailRequest.IsHTML = true
	n.EmailRequest.Labels = n.Labels
	data, err := json.Marshal(n.EmailRequest)

	if err != nil {
		return
	}
	rst, err := n.DoPostNotifyAPI(n.urlToEmail, data)
	if err != nil {
		fmt.Printf("email call failed:%v", err)
		return
	}
	if rst.Code != 1 {
		fmt.Printf("email call failed, result:%v", rst)
		return
	}
	return
}

// AlarmToDongdong 针对咚咚渠道报警
func (n *Notify) AlarmToDongdong(subject string, content string, url string) {
	if n.urlToDongdong == "" || n.token == "" {
		return
	}
	n.DongdongExtRequest.Subject = fmt.Sprintf("%s %s", PrefixSubject, subject)
	nowt := time.Now().Format("2006-01-02 15:04:05")
	n.DongdongExtRequest.Content = fmt.Sprintf("【警告】%s\n【时间】%s", content, nowt)
	n.DongdongExtRequest.URL = url
	n.DongdongExtRequest.Timlines = n.Erps
	n.DongdongExtRequest.SendType = "7"
	n.DongdongExtRequest.Labels = n.Labels
	data, err := json.Marshal(n.DongdongExtRequest)
	if err != nil {
		return
	}
	rst, err := n.DoPostNotifyAPI(n.urlToDongdong, data)
	if err != nil {
		fmt.Printf("dongdong call failed:%v", err)
		return
	}
	if rst.Code != 1 {
		fmt.Printf("dongdong call failed, result:%v", rst)
		return
	}
	return
}

// AlarmToCall 针对电话渠道报警
func (n *Notify) AlarmToCall(subject string) {
	if n.urlToCall == "" || n.token == "" {
		return
	}
	if n.AlarmSync {
		for _, p := range n.Phones {
			n.CallRequest.Content = []interface{}{map[string]string{"phone": p}}
			n.CallRequest.Platform = "notify"
			n.CallRequest.Node = fmt.Sprintf("%s %s", PrefixSubject, subject)
			n.CallRequest.Labels = n.Labels
			data, err := json.Marshal(n.CallRequest)
			if err != nil {
				return
			}
			rst, err := n.DoPostNotifyAPI(n.urlToCall, data)
			if err != nil {
				// log.LogWarn(err, msg)
				fmt.Printf("phone call failed:%v", err)
				return
			}
			if rst.Code != 1 {
				// log.LogWarn(rst)
				fmt.Printf("phone call failed, result%v", rst)
				return
			}
		}
	} else {
		var phones []interface{}
		for _, m := range n.Phones {
			phone := map[string]string{
				"phone": m,
			}
			phones = append(phones, phone)

		}

		n.CallRequest.Content = phones
		n.CallRequest.Platform = "notify"
		n.CallRequest.Node = fmt.Sprintf("%s %s", PrefixSubject, subject)
		n.CallRequest.Labels = n.Labels
		data, err := json.Marshal(n.CallRequest)
		if err != nil {
			return
		}
		rst, err := n.DoPostNotifyAPI(n.urlToCall, data)
		if err != nil {
			// log.LogWarn(err, msg)
			return
		}
		if rst.Code != 1 {
			// log.LogWarn(rst)
			return
		}
		return
	}
}

// DoPostNotifyAPI NotifyPost方法
func (n *Notify) DoPostNotifyAPI(url string, data []byte) (rst *ResponseResult, err error) {
	fmt.Println(url, string(data))
	buff := bytes.NewBuffer(data)
	client := &http.Client{Timeout: 5 * time.Second}
	req, err := http.NewRequest(http.MethodPost, url, buff)
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json;charset=utf-8")
	req.Header.Set("Connection", "close")
	req.Header.Set("token", n.token)
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer resp.Body.Close()
	data, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(string(data))
	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("action[notify_DoPost] url[%v],statusCode[%v],requestBody[%v],err:[%v]", url, resp.StatusCode, string(data), err)
		fmt.Println(err)
		return
	}
	rst = &ResponseResult{}

	if err = json.Unmarshal(data, rst); err != nil {
		fmt.Println(err)
		return
	}
	return
}