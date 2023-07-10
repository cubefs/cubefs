package mdc

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

const (
	IdSiteType = "id" //印尼
	ThSiteType = "th" //泰国
	CnSiteType = "cn" //中国
)

const (
	MDCToken = "043e7fe70e26369fae5d81a0989ea4fa"
	AGGToken = "f5d35b09e654379aa79a9fc11899bfd1" //just for operating system aggregation query
	//0 - 查询分钟级数据， 1 - 查询秒级数据
	QueryTypeMinute = 0
	QueryTypeSecond = 1
)

const (
	PingMinute            = "min_ping"              //连通性
	LoadAvgMinute         = "min_load_avg"          //系统负载
	CpuUsagePercentMinute = "min_cpu_usage_percent" //CPU平均使用率
	MemUsagePercentMinute = "min_mem_usage_percent" //内存平均使用率
	NetRXBytesMinute      = "min_net_dev_rx_bytes"  //网络流入速度
	NetTXBytesMinute      = "min_net_dev_tx_bytes"  //网络流出速度
	TcpRetransCountMinute = "min_tcp_retrans_count" //TCP重传次数
	FsUsagePercentMinute  = "min_fs_usage_percent"  //磁盘使用率
	DiskBusyPercentMinute = "min_disk_busy_percent" //磁盘繁忙率

	CpuUsagePercentSecond = "cpu_usage_percent" //秒级 CPU使用率  // 需要确定物理机/容器 IP
	MemUsagePercentSecond = "mem_usage_percent" //秒级 内存使用率 // 需要确定物理机/容器 IP

	FsUsagePercentSecond  = "fs_usage_percent"  //秒级 磁盘使用率 float64 // 需要确定对应的磁盘名
	DiskBusyPercentSecond = "disk_busy_percent" //秒级 磁盘繁忙率 float64 // 需要确定对应的磁盘名

	NetRXBytesSecond   = "net_dev_rx_bytes"      //秒级 网络流入速度 (网络带宽) float64 // 需要确定对应的网卡名
	NetTXBytesSecond   = "net_dev_tx_bytes"      //秒级 网络流出速度 (网络带宽) float64 // 需要确定对应的网卡名
	FsUsagePercentMin  = "min_fs_usage_percent"  //秒级 磁盘使用率 float64 // 需要确定对应的磁盘名
	DiskBusyPercentMin = "min_disk_busy_percent" //秒级 磁盘繁忙率 float64 // 需要确定对应的磁盘名
	MinMemUsagePercent = "min_mem_usage_percent" //分钟级 内存使用率 // 需要确定物理机/容器 IP
)

type NodeInfo struct {
	IPAddr    string    `gorm:"column:ip;comment:"`
	TimeStamp time.Time `gorm:"column:time_stamp;comment:对应的时间"`
	Value     float64
}

func (info NodeInfo) String() string {
	return fmt.Sprintf("IP:%v,Value:%.2f,Time:%v",
		info.IPAddr, info.Value, info.TimeStamp.Format("2006-01-02 15:04:05"))
}

func NewMdcNodeInfo(mdcInfo *MDCInfo) *NodeInfo {
	monitorData := new(NodeInfo)
	monitorData.TimeStamp = time.Now()
	monitorData.IPAddr = mdcInfo.Labels.Ip
	if len(mdcInfo.Series) == 0 {
		return monitorData
	}

	//获取大的时间的值
	latestTime := int64(0)
	for _, data := range mdcInfo.Series {
		if data.Time > latestTime {
			latestTime = data.Time
			monitorData.Value = data.Value
		}
	}
	if latestTime > 0 {
		monitorData.TimeStamp = time.Unix(latestTime, 0)
	}
	return monitorData
}

type Point struct {
	Time  uint64 `json:"t"`
	Value string `json:"v"`
}

type PointNew struct {
	Time  int64   `json:"timestamp"`
	Value float64 `json:"value"`
}

type LabelInfo struct {
	Ip          string `json:"ip"`
	HostIp      string `json:"hostIp"`
	ContainerId string `json:"containerId"`
	MountPath   string `json:"fsName"`
	DeviceName  string `json:"device"`
	NetDevice   string `json:"iface"`
}

type MDCInfo struct {
	Label       *LabelInfo  `json:"label"`
	Labels      *LabelInfo  `json:"labels"`
	MetricName  string      `json:"metric"`
	MonitorData []*Point    `json:"monitorData"`
	Series      []*PointNew `json:"series"`
}

type MdcInfos struct {
	Infos []*MDCInfo
}

type MDCResultInfo struct {
	ResponseData [][]MDCInfo `json:"response_data"`
}

type ResponseInfo struct {
	MetricResponseList []*MDCInfo `json:"metricRespList"`
	Unit               string     `json:"unit"`
	Title              string     `json:"title"`
}

// 详细文档见：https://cf.jd.com/display/computing/MDC3.0+Open+API
type MDCOpenApi struct {
	token    string
	domain   string
	metrics  []string //该时序数据对应的哪个指标的数据，Metric个数乘以labels的entry个数不能超过20个
	httpSend *HttpSend
}

// 使用建议 设置token以及对应指标（不同类型请求，指标不同）
// 同一时间对多种labels进行查询，或者同一个label对多个时间段进行查询
func NewMDCOpenApi(token string, metrics []string, siteType string) (mdcApi *MDCOpenApi) {
	mdcApi = new(MDCOpenApi)
	mdcApi.token = token
	mdcApi.metrics = metrics
	switch siteType {
	case CnSiteType:
		mdcApi.domain = "http://mlaas-gateway.jd.local"
	case IdSiteType:
		mdcApi.domain = "http://mdcgw-online.mdc3.svc.yn.n.jd.local"
	default:
		mdcApi.domain = "http://mlaas-gateway.jd.local"
	}
	mdcApi.httpSend = new(HttpSend)
	mdcApi.httpSend.SetHeader(map[string]string{"token": mdcApi.token, "Content-Type": "application/json"})
	return
}

// Second level monitoring
// 分钟级监控API，指标时间，请使用十位秒级时间戳，单位s，时间需整分钟时刻
func (mdcApi *MDCOpenApi) MinuteMonitor(startTime, endTime string, labels []map[string]string) (rt *MDCResultInfo, err error) {
	//mdcApi.httpSend.SetLink("http://mlaas-gateway.jd.local/v1/dmapis/min/query")
	mdcApi.httpSend.SetLink(fmt.Sprintf("%s/v1/dmapis/min/query", mdcApi.domain))
	body := make(map[string]interface{}, 0)
	body["metrics"] = mdcApi.metrics
	body["startTime"] = startTime
	body["endTime"] = endTime
	body["labels"] = labels
	mdcApi.httpSend.SetJsonTypeBody(body)
	return mdcApi.post()
}

// 秒级监控API
// 指标时间，请使用十位秒级时间戳，单位s，秒级时间范围不能大于30分钟
// 查询类型（对应label名）可选值是ip、containerId、podId，Metric个数乘以labels的entry个数不能超过20个
func (mdcApi *MDCOpenApi) SecondMonitor(startTime, endTime string, labels []map[string]string) (rt *MDCResultInfo, err error) {
	//mdcApi.httpSend.SetLink("http://mlaas-gateway.jd.local/v1/dmapis/sec/query")
	mdcApi.httpSend.SetLink(fmt.Sprintf("%s/v1/dmapis/sec/query", mdcApi.domain))
	body := make(map[string]interface{}, 0)
	body["metrics"] = mdcApi.metrics
	body["startTime"] = startTime
	body["endTime"] = endTime
	body["labels"] = labels
	mdcApi.httpSend.SetJsonTypeBody(body)
	return mdcApi.post()
}

// 分钟级latest时间点查询API
func (mdcApi *MDCOpenApi) MinuteLatestMonitor(labels []map[string]string) (rt *MDCResultInfo, err error) {
	//mdcApi.httpSend.SetLink("http://mlaas-gateway.jd.local/v1/dmapis/latest/query")
	mdcApi.httpSend.SetLink(fmt.Sprintf("%s/v1/dmapis/latest/query", mdcApi.domain))
	body := make(map[string]interface{}, 0)
	body["metrics"] = mdcApi.metrics
	body["labels"] = labels
	mdcApi.httpSend.SetJsonTypeBody(body)
	return mdcApi.post()
}

//新：分钟级&秒级时序数据查询
/*conditions 查询条件 - 必须有查询条件，查询条件中必须有一项填写
	app	string	false	应用名称 （分钟级数据查询存在该条件）
	platform	string	false	平台（分钟级数据查询存在该条件）
	system	string	false	系统（分钟级数据查询存在该条件）
	ip	string	false	ip 信息
	containerId	string	false	容器 id
	hostIp	string	false	宿主机 ip
	tags	map<String, String>	false	其他可查询的 label
metrics	String[]	true	查询指标列表，详见 MDC 分钟级指标说明 和 MDC 秒级指标说明
start	long	true	查询起始时间时间戳（ms）。 分钟级数据请按分钟取整，秒级数据按秒取整
end	long	true	查询结束时间时间戳（ms）。 分钟级数据请按分钟取整，秒级数据按秒取整
queryType	short	true	0 - 查询分钟级数据， 1 - 查询秒级数据
step	int	true	查询间隔，查询分钟级数据传入 60，秒级传入 1
*/
func (mdcApi *MDCOpenApi) Query(startTime, endTime int64, conditions []map[string]interface{}, queryType int) (rt []*ResponseInfo, err error) {
	//mdcApi.httpSend.SetLink("http://mlaas-gateway.jd.local/mdc/v2/open/metric/range_query")
	mdcApi.httpSend.SetLink(fmt.Sprintf("%s/mdc/v2/open/metric/range_query", mdcApi.domain))
	body := make(map[string]interface{}, 0)
	body["conditions"] = conditions
	body["metrics"] = mdcApi.metrics
	body["start"] = startTime
	body["end"] = endTime
	body["queryType"] = queryType
	switch queryType {
	case QueryTypeMinute:
		body["step"] = 60
	case QueryTypeSecond:
		body["step"] = 1
	default:
		return nil, fmt.Errorf("wrong queryType")
	}
	mdcApi.httpSend.SetJsonTypeBody(body)
	return mdcApi.postNew()
}

// 聚合查询
// queryType:  0:分钟级数据查询;   1:秒级数据查询
// aggType:  聚合查询类型,支持最大值、最小值以及平均值的查询，分别为max_over_time,min_over_time,avg_over_time
// 具体见网址：  https://cf.jd.com/display/MDCOperate/MDC3.0+Open+API
func (mdcApi *MDCOpenApi) AggregationQuery(startTime, endTime int64, conditions []map[string]interface{}, queryType int, aggType string) (rt []*ResponseInfo, err error) {
	//mdcApi.httpSend.SetLink("http://mlaas-gateway.jd.local/mdc/v2/open/metric/agg_instance")
	mdcApi.httpSend.SetLink(fmt.Sprintf("%s/mdc/v2/open/metric/agg_instance", mdcApi.domain))
	body := make(map[string]interface{}, 0)
	body["metrics"] = mdcApi.metrics
	body["start"] = startTime
	body["end"] = endTime
	body["conditions"] = conditions
	body["queryType"] = queryType
	body["agg"] = aggType
	mdcApi.httpSend.SetJsonTypeBody(body)
	return mdcApi.postNew()
}

func (mdcApi *MDCOpenApi) post() (rt *MDCResultInfo, err error) {
	var bytes []byte
	if bytes, err = mdcApi.httpSend.Post(); err != nil {
		return
	}
	body := &struct {
		Success bool            `json:"is_success"`
		Data    json.RawMessage `json:"response_data"`
		Code    int             `json:"error_code"`
		Message string          `json:"error_msg"`
	}{}
	if err = json.Unmarshal(bytes, body); err != nil {
		return nil, fmt.Errorf("unmarshal body failed err:%v", err)
	}
	if !body.Success {
		return nil, fmt.Errorf("req failed Code:%v Message:%v", body.Code, body.Message)
	}
	var result = &MDCResultInfo{}
	err = json.Unmarshal(bytes, result)
	if err != nil {
		return nil, fmt.Errorf("json unmarshal failed:%v", err)
	}
	//bytes = body.Data
	return result, nil
}

// mdc v2版本回复体有所变化
func (mdcApi *MDCOpenApi) postNew() (rt []*ResponseInfo, err error) {
	var bytes []byte
	if bytes, err = mdcApi.httpSend.Post(); err != nil {
		return
	}
	body := &struct {
		Data    json.RawMessage `json:"data"`
		Code    string          `json:"code"`
		Message string          `json:"message"`
	}{}
	if err = json.Unmarshal(bytes, body); err != nil {
		return nil, fmt.Errorf("unmarshal body failed err:%v", err)
	}
	if body.Code != "10000" {
		return nil, fmt.Errorf("req failed Code:%v Message:%v", body.Code, body.Message)
	}
	var result = make([]*ResponseInfo, 0)
	//fmt.Println(string(body.Data))
	err = json.Unmarshal([]byte(body.Data), &result)
	if err != nil {
		err = fmt.Errorf("json unmarshal failed：%v", err)
		return nil, err
	}
	return result, nil
}

// GetIPDeviceNameMountPath 给定IP和设备名，找到对应的信息
func GetIPDeviceNameMountPath(ip, deviceName string) (deviceMountPathInfos DeviceMountPathInfo, err error) {
	now := time.Now().Add(-time.Minute * 1)
	endTime := now.Unix() * 1000
	startTime := now.Add(-time.Minute*10).Unix() * 1000
	device, err := CheckDeviceNameAndMountPath(ip, startTime, endTime)
	if err != nil {
		return
	}
	for _, info := range device {
		if info.DeviceName == deviceName {
			return info, nil
		}
	}
	err = fmt.Errorf("can not find")
	return
}

// GetHighDiskBusyInfos 给定一批IP 获取到高于阈值的信息
func GetHighDiskBusyInfos(ips []string, startTime, endTime int64, ratio float64) (deviceMountPathInfos []DeviceMountPathInfo, err error) {
	for _, ip := range ips {
		infos, err1 := CheckDeviceNameAndMountPath(ip, startTime, endTime)
		if err1 != nil {
			err = err1
			return
		}
		for _, info := range infos {
			if info.DiskBusyPercentMin >= ratio {
				deviceMountPathInfos = append(deviceMountPathInfos, info)
			}
		}
		time.Sleep(time.Millisecond * 100) //避免被限速
	}
	return
}

// CheckDeviceNameAndMountPath 一个机器下所有磁盘的 DeviceName MountPath 磁盘繁忙 磁盘使用率
func CheckDeviceNameAndMountPath(ip string, startTime, endTime int64) (deviceMountPathInfos []DeviceMountPathInfo, err error) {
	var (
		diskBusyInfos *ResponseInfo
		diskUsedInfos *ResponseInfo
	)
	metrics := make([]string, 0, 1)
	metrics = append(metrics, FsUsagePercentMin)
	metrics = append(metrics, DiskBusyPercentMin)
	mdcApi := NewMDCOpenApi(AGGToken, metrics, CnSiteType)
	labels := make([]map[string]interface{}, 0)
	conditionMap := map[string]interface{}{
		"ip":   ip,
		"tags": map[string]string{"hostType": "h"}, //hostType = "c" 容器类型
	}
	labels = append(labels, conditionMap)
	//queryType:  0:分钟级数据查询;   1:秒级数据查询
	aggregationQuery, err := mdcApi.Query(startTime, endTime, labels, 0)
	if err != nil || len(aggregationQuery) == 0 {
		return
	}
	for _, info := range aggregationQuery {
		if info.Title == "磁盘繁忙" {
			diskBusyInfos = info
		}
		if info.Title == "磁盘使用率" {
			diskUsedInfos = info
		}
	}
	if diskBusyInfos == nil && diskUsedInfos == nil {
		return nil, fmt.Errorf("nil")
	}

	//磁盘使用率包含DeviceName 通过这个去磁盘繁忙里匹配
	for _, diskInfo := range diskUsedInfos.MetricResponseList {
		//过滤非data
		if !strings.Contains(diskInfo.Labels.MountPath, "/data") {
			continue
		}
		deviceInfo := DeviceMountPathInfo{
			IP:                 diskInfo.Labels.Ip,
			MountPath:          diskInfo.Labels.MountPath,
			DeviceName:         strings.TrimPrefix(diskInfo.Labels.DeviceName, "/dev/"),
			DiskBusyPercentMin: -1,
		}
		if len(diskInfo.Series) != 0 {
			for _, series := range diskInfo.Series {
				deviceInfo.FsUsagePercentMin += series.Value
			}
			deviceInfo.FsUsagePercentMin = deviceInfo.FsUsagePercentMin / float64(len(diskInfo.Series))
		}
		for _, mdcInfo := range diskBusyInfos.MetricResponseList {
			if mdcInfo.Labels.DeviceName == deviceInfo.DeviceName && mdcInfo.Labels.Ip == deviceInfo.IP {
				if len(mdcInfo.Series) != 0 {
					for _, series := range mdcInfo.Series {
						if series.Value > deviceInfo.DiskBusyPercentMin {
							deviceInfo.DiskBusyPercentMin = series.Value
						}
					}
				}
			}
		}
		deviceMountPathInfos = append(deviceMountPathInfos, deviceInfo)
	}
	//排序
	sort.Slice(deviceMountPathInfos, func(i, j int) bool {
		return deviceMountPathInfos[i].DiskBusyPercentMin > deviceMountPathInfos[j].DiskBusyPercentMin
	})

	return
}

type DeviceMountPathInfo struct {
	//{11.5.208.72 sdm /data11 78 12.909090909090908}
	IP                 string
	DeviceName         string
	MountPath          string
	FsUsagePercentMin  float64
	DiskBusyPercentMin float64
}

func GetMountPathDeviceMountPathInfoMap(ip string) (mountPathDeviceMap map[string]DeviceMountPathInfo, err error) {
	mountPathDeviceMap = make(map[string]DeviceMountPathInfo)
	now := time.Now().Add(-time.Minute * 1)
	endTime := now.Unix() * 1000
	startTime := now.Add(-time.Minute*10).Unix() * 1000
	device, err := CheckDeviceNameAndMountPath(ip, startTime, endTime)
	if err != nil {
		return
	}
	for _, info := range device {
		mountPathDeviceMap[info.MountPath] = info
	}
	return
}
