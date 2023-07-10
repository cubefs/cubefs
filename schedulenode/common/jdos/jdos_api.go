package jdos

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cubefs/cubefs/util/log"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	pageSizeParam   = "pageSize"
	pageSizeValue   = 1000                            //最大不能超过1000
	JCDOnlineSite   = "http://api.jcd-gateway.jd.com" // 行云部署open api前缀
	Erp             = "org.imgsupport"
	OnlineToken     = "3464713a-b5ca-435e-a6de-704fd6f84acd"
	OnlineSite      = JCDOnlineSite //"http://api.jdos.jd.com"
	TestToken       = "2f54b7da-60ac-436a-acca-758e78f39b5a"
	TestSite        = "http://api-test.jdos.jd.com"
	LbStatusActivce = "active"
)

type Groups []*Group
type GroupNames []*GroupName
type GroupConfigs []*GroupConfig
type Pods []*Pod
type Lbs []*Lb

type Group struct {
	ID             string      `json:"id"`
	SystemName     string      `json:"systemName"` //系统名称
	AppName        string      `json:"appName"`    //英文名称
	GroupName      string      `json:"groupName"`  //分组名称
	Nickname       string      `json:"nickname"`   //分组中文昵称
	OrderPriority  int         `json:"orderPriority"`
	Desc           string      `json:"desc"` //分组描述
	RouteInfo      interface{} `json:"routeInfo"`
	PlatformHosted string      `json:"platformHosted"` //容器是否自动拉起:true(是)false(否)
	DataCenter     string      `json:"dataCenter"`
	DataCenterCn   string      `json:"dataCenterCn"`
	Region         string      `json:"region"`      //机房标识
	RegionCN       string      `json:"regionCN"`    //机房名称
	Environment    string      `json:"environment"` //分组所属环境
	Creator        string      `json:"creator"`     //分组创建人
	CreateTime     string      `json:"createTime"`  //分组创建时间
	Deleted        string      `json:"deleted"`
	JsfStatus      string      `json:"jsfStatus"` //是否注册了jsf:active(是)inactive(否)
	UpdateTime     string      `json:"updateTime"`
	Zone           string      `json:"zone"`
	DiskSize       string      `json:"diskSize"`
}

type GroupName struct {
	ID          string `json:"id"`
	SystemName  string `json:"systemName"`  //系统名称
	AppName     string `json:"appName"`     //英文名称
	GroupName   string `json:"groupName"`   //分组名称
	Nickname    string `json:"nickname"`    //分组中文昵称
	Region      string `json:"region"`      //机房标识
	RegionCN    string `json:"regionCN"`    //机房名称
	Environment string `json:"environment"` //分组环境 pre or pro
}

type FlavorInfo struct {
	FlavorName string `json:"flavorName"`
	CPULimit   string `json:"cpuLimit"` //cpu核数
	MemLimit   string `json:"memLimit"`
}

type GroupConfig struct {
	ID          string      `json:"id"`
	SystemName  string      `json:"systemName"`
	AppName     string      `json:"appName"`
	GroupName   string      `json:"groupName"`
	Image       string      `json:"image"` //集群配置选择的镜像
	Flavor      string      `json:"flavor"`
	FlavorInfo  FlavorInfo  `json:"flavorInfo"` //规格信息
	Envs        interface{} `json:"envs"`
	ConfigFiles []string    `json:"configFiles"` //配置文件uuid集合
	UUID        string      `json:"uuid"`        //分组配置uuid
	DiskSize    int         `json:"diskSize"`    //磁盘大小 G
	DiskType    string      `json:"diskType"`    //磁盘类型
	CPUModel    string      `json:"cpuModel"`    //cpu型号
	CreateTime  string      `json:"createTime"`
	SwapOn      int         `json:"swapOn"`
	JforwardOn  int         `json:"jforwardOn"`
	Zone        string      `json:"zone"`
	UserCFS     bool        `json:"userCFS"`
	StorePath   string      `json:"storePath"`
}

type PodFlavor struct {
	Memory string `json:"memory"`
	CPU    string `json:"cpu"`
}

type GroupConfigPerfParam struct {
	ID       interface{} `json:"id"`
	ReadBps  interface{} `json:"readBps"`
	WriteBps interface{} `json:"writeBps"`
}

//容器状态 :部署中(Pending,ContainerCreating),运行中(Running),正在停止(Terminating),
//容器退出(SignalOrExitCode),错误(Failed,Error),镜像拉取失败(ImagePullBackOff,ErrImagePull),
//其余状态表示:部署等待处理
type Pod struct {
	PodName         string    `json:"podName"`
	Status          string    `json:"status"`
	Ready           bool      `json:"ready"`
	PodIP           string    `json:"podIP"`
	HostIP          string    `json:"hostIP"`
	ContainerID     string    `json:"containerID"`
	Image           string    `json:"image"` //当前容器所用的镜像
	Flavor          PodFlavor `json:"flavor"`
	CreateTime      time.Time `json:"createTime"`
	LbStatus        string    `json:"lbStatus"`  //active(在lb中)inactive(不在lb中)
	JsfStatus       string    `json:"jsfStatus"` //active(在jsf中)inactive(不在jsf中)
	UUID            string    `json:"uuid"`
	PublishTime     string    `json:"publishTime"`
	ShowPublishTime string    `json:"showPublishTime"`
}

type Port struct {
	Protocol    string `json:"protocol"`
	Port        string `json:"port"`
	CheckString string `json:"checkString"` //健康检查值
	TargetPort  string `json:"targetPort"`
	CheckURL    string `json:"checkUrl"`  //健康检查url
	Algorithm   string `json:"algorithm"` //负载均衡协议
}

type Lb struct {
	ID               string   `json:"id"`
	SystemName       string   `json:"systemName"`
	AppName          string   `json:"appName"`
	LoadBalancerName string   `json:"loadBalancerName"`
	PortsDb          string   `json:"portsDb"`
	MaxConnect       int      `json:"maxConnect"`
	Region           string   `json:"region"` //机房标识
	GroupTagDb       string   `json:"groupTagDb"`
	CreateTime       string   `json:"createTime"`
	Groups           []Group  `json:"groups"`
	LocalDomain      string   `json:"localDomain"` //域名
	ExternalIP       string   `json:"externalIP"`  //IP
	Deleted          string   `json:"deleted"`
	ServiceType      string   `json:"serviceType"`
	UpdateTime       string   `json:"updateTime"`
	GroupNames       []string `json:"groupNames"` //分组名（groupName）集合
	Vip              bool     `json:"vip"`
	Ports            []Port   `json:"ports"`
}

// 详细文档见：http://help.jdos.jd.com/api.html?highlight=api
type OpenApi struct {
	site       string
	systemName string
	appName    string
	header     map[string]string
	httpSend   *HttpSend
}

func NewJDOSOpenApi(systemName, appName, site, erp, token string) (jApi *OpenApi) {
	jApi = new(OpenApi)
	jApi.systemName = systemName
	jApi.appName = appName
	jApi.site = site
	jApi.header = make(map[string]string, 2)
	jApi.header["erp"] = erp
	jApi.header["token"] = token
	jApi.httpSend = new(HttpSend)
	jApi.httpSend.SetHeader(jApi.header)
	return
}

//分组服务接口-查询应用下对应的所有分组详情(含有region信息)
func (jAPi *OpenApi) GetAllGroupsDetails() (jGroups Groups, err error) {
	var data []byte
	reqUrl := fmt.Sprintf("%v/api/v2/apps/%v/groups", jAPi.site, jAPi.appName)
	//reqUrl := fmt.Sprintf("%v/api/system/%v/app/%v/group", jAPi.site, jAPi.systemName, jAPi.appName)
	if data, err = jAPi.getByList(reqUrl); err != nil {
		err = fmt.Errorf("action[GetAllGroupsDetails] req reqUrl:%v err:%v ", reqUrl, err)
		return
	}
	if err = json.Unmarshal(data, &jGroups); err != nil {
		err = fmt.Errorf("action[GetAllGroupsDetails] unmarshal data failed, err:%v data:%v ", err, string(data))
		return
	}
	return
}

//分组服务接口-查询应用下对应的所有分组名称(含有region信息,去掉了一些详细信息)
func (jAPi *OpenApi) GetAllGroupsNames() (jGroupNames GroupNames, err error) {
	var data []byte
	reqUrl := fmt.Sprintf("%v/api/v2/apps/%v/groups", jAPi.site, jAPi.appName)
	//reqUrl := fmt.Sprintf("%v/api/system/%v/app/%v/group", jAPi.site, jAPi.systemName, jAPi.appName)
	if data, err = jAPi.getByList(reqUrl); err != nil {
		err = fmt.Errorf("action[GetAllGroupsNames] req reqUrl:%v err:%v ", reqUrl, err)
		return
	}
	if err = json.Unmarshal(data, &jGroupNames); err != nil {
		err = fmt.Errorf("action[GetAllGroupsNames] unmarshal data failed, err:%v data:%v ", err, string(data))
		return
	}
	return
}

//分组服务接口-查询分组详情
func (jAPi *OpenApi) GetGroupDetails(groupName string) (jGroup *Group, err error) {
	var data []byte
	reqUrl := fmt.Sprintf("%v/api/v2/apps/%v/groups/%v", jAPi.site, jAPi.appName, groupName)
	//reqUrl := fmt.Sprintf("%v/api/system/%v/app/%v/group/%v", jAPi.site, jAPi.systemName, jAPi.appName, groupName)
	if data, err = jAPi.get(reqUrl); err != nil {
		err = fmt.Errorf("action[GetGroupDetails] req reqUrl:%v err:%v ", reqUrl, err)
		return
	}
	if err = json.Unmarshal(data, &jGroup); err != nil {
		err = fmt.Errorf("action[GetGroupDetails] unmarshal data failed, err:%v data:%v ", err, string(data))
		return
	}
	return
}

//集群管理-查询集群中的所有pod
func (jAPi *OpenApi) GetGroupAllPods(groupName string) (jDOSPods Pods, err error) {
	var data []byte
	reqUrl := fmt.Sprintf("%v/api/v2/apps/%v/groups/%v/cluster/pods", jAPi.site, jAPi.appName, groupName)
	//reqUrl := fmt.Sprintf("%v/api/system/%v/app/%v/group/%v/cluster/podall", jAPi.site, jAPi.systemName, jAPi.appName, groupName)
	if data, err = jAPi.getByList(reqUrl); err != nil {
		err = fmt.Errorf("action[GetGroupAllPods] req reqUrl:%v err:%v ", reqUrl, err)
		return
	}
	if err = json.Unmarshal(data, &jDOSPods); err != nil {
		err = fmt.Errorf("action[GetGroupAllPods] unmarshal data failed, err:%v data:%v ", err, string(data))
		return
	}
	log.LogDebug(fmt.Sprintf("action[GetGroupAllPods] groupName:%v count:%v", groupName, len(jDOSPods)))
	return
}

//负载均衡接口-获取该应用下所有的lb信息
func (jAPi *OpenApi) GetAllLbs() (jDOSLbs Lbs, err error) {
	var data []byte
	url := fmt.Sprintf("%v/api/v2/apps/%v/loadBalances", jAPi.site, jAPi.appName)
	//url := fmt.Sprintf("%v/api/system/%v/app/%v/loadbalance", jAPi.site, jAPi.systemName, jAPi.appName)
	if data, err = jAPi.get(url); err != nil {
		err = fmt.Errorf("action[GetAllLbs] req url:%v err:%v ", url, err)
		return
	}
	if err = json.Unmarshal(data, &jDOSLbs); err != nil {
		err = fmt.Errorf("action[GetAllLbs] unmarshal data failed, err:%v data:%v ", err, string(data))
		return
	}
	return
}

func (jAPi *OpenApi) get(url string) (data []byte, err error) {
	jAPi.httpSend.SetLink(url)
	var bytes []byte
	if bytes, err = jAPi.httpSend.Get(); err != nil {
		return
	}
	body := &struct {
		ReqId   string          `json:"reqId"`
		Success bool            `json:"success"`
		Code    int             `json:"code"`
		Message string          `json:"message"`
		Data    json.RawMessage `json:"data"`
	}{}
	if err = json.Unmarshal(bytes, body); err != nil {
		return nil, fmt.Errorf("unmarshal body failed err:%v", err)
	}
	if !body.Success {
		return nil, fmt.Errorf("req failed Code:%v Message:%v", body.Code, body.Message)
	}
	return body.Data, nil
}

func (jAPi *OpenApi) getByList(url string) (data []byte, err error) {
	if !strings.Contains(url, pageSizeParam) {
		url = fmt.Sprintf("%v?%v=%v", url, pageSizeParam, pageSizeValue)
	}
	jAPi.httpSend.SetLink(url)
	var bytes []byte
	if bytes, err = jAPi.httpSend.Get(); err != nil {
		return
	}
	body := &struct {
		ReqId   string `json:"reqId"`
		Success bool   `json:"success"`
		Code    int    `json:"code"`
		Message string `json:"message"`
		Data    struct {
			PageNum  int             `json:"pageNum"`
			PageSize int             `json:"pageSize"`
			Records  int             `json:"records"`
			List     json.RawMessage `json:"list"`
		}
	}{}
	if err = json.Unmarshal(bytes, body); err != nil {
		return nil, fmt.Errorf("unmarshal body failed err:%v", err)
	}
	if !body.Success {
		return nil, fmt.Errorf("req failed Code:%v Message:%v", body.Code, body.Message)
	}
	return body.Data.List, nil
}

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
	sync.RWMutex
}

func (h *HttpSend) SetLink(link string) {
	h.Lock()
	defer h.Unlock()
	h.Link = link
}

func (h *HttpSend) SetHeader(header map[string]string) {
	h.Lock()
	defer h.Unlock()
	h.Header = header
}

func (h *HttpSend) SetBody(body map[string]interface{}) {
	h.Lock()
	defer h.Unlock()
	h.Body = body
}

func (h *HttpSend) Get() ([]byte, error) {
	return h.send(GetMethod)
}

func (h *HttpSend) Post() ([]byte, error) {
	return h.send(PostMethod)
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
