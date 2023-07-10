package mdc

import (
	"fmt"
	"strconv"
	"testing"
	"time"
)

func TestMinuteMonitor(t *testing.T) {
	metrics := make([]string, 0, 6)
	//metrics = append(metrics, "min_ping")
	metrics = append(metrics, "min_load_avg")
	metrics = append(metrics, "min_cpu_usage_percent")
	//metrics = append(metrics, "min_swap_usage_percent")
	//metrics = append(metrics, "min_mem_usage_percent")
	//metrics = append(metrics, "min_ntp_clk_offset_ms")
	mdcApi := NewMDCOpenApi(MDCToken, metrics, CnSiteType)
	//startTime := "1612146360"
	//endTime := "1612146480"
	endTime := time.Now().Unix()
	startTime := endTime - 60*4
	endTimeString := strconv.FormatInt(endTime, 10)
	startTimeString := strconv.FormatInt(startTime, 10)
	labels := make([]map[string]string, 0)
	labels = append(labels, map[string]string{"ip": "10.179.26.164"})
	labels = append(labels, map[string]string{"ip": "10.179.20.22"})
	labels = append(labels, map[string]string{"ip": "11.5.113.165"})
	labels = append(labels, map[string]string{"ip": "10.179.20.22"})
	labels = append(labels, map[string]string{"ip": "10.179.20.22"})
	minuteMonitorResult, err := mdcApi.MinuteMonitor(startTimeString, endTimeString, labels)
	if err != nil {
		t.Errorf("TestMinuteMonitor err:%v", err)
		return
	}
	fmt.Printf("metric:%s\n", minuteMonitorResult.ResponseData[0][0].MetricName)
	return
}

func TestSecondMonitor(t *testing.T) {
	metrics := make([]string, 0, 2)
	metrics = append(metrics, "cpu_usage_percent")
	metrics = append(metrics, "mem_usage_percent")
	mdcApi := NewMDCOpenApi(AGGToken, metrics, CnSiteType)
	//startTime := "1612146470"
	//endTime := "1612146480"
	endTime := time.Now().Unix()
	startTime := endTime - 5
	endTimeString := strconv.FormatInt(endTime, 10)
	startTimeString := strconv.FormatInt(startTime, 10)
	labels := make([]map[string]string, 0)
	labels = append(labels, map[string]string{"ip": "11.42.106.21"})
	labels = append(labels, map[string]string{"ip": "11.42.106.183"})
	minuteMonitor, err := mdcApi.SecondMonitor(startTimeString, endTimeString, labels)
	if err != nil {
		t.Errorf("TestSecondMonitor err:%v", err)
		return
	}
	fmt.Printf("metric:%s", minuteMonitor.ResponseData[0][0].MetricName)
	return
}

func TestMinuteLatestMonitor(t *testing.T) {
	metrics := make([]string, 0, 2)
	metrics = append(metrics, "min_cpu_usage_percent")
	metrics = append(metrics, "min_mem_usage_percent")
	mdcApi := NewMDCOpenApi(MDCToken, metrics, CnSiteType)
	labels := make([]map[string]string, 0)
	labels = append(labels, map[string]string{"ip": "11.42.106.21"})
	labels = append(labels, map[string]string{"ip": "11.42.106.183"})
	minuteMonitor, err := mdcApi.MinuteLatestMonitor(labels)
	if err != nil {
		t.Errorf("TestMinuteLatestMonitor err:%v", err)
		return
	}
	fmt.Printf("metric:%s", minuteMonitor.ResponseData[0][0].MetricName)
	return
}

func TestAggregationQuery(t *testing.T) {
	metrics := make([]string, 0, 1)
	metrics = append(metrics, "net_dev_rx_bytes")
	mdcApi := NewMDCOpenApi(AGGToken, metrics, CnSiteType)
	labels := make([]map[string]interface{}, 0)
	conditionMap := map[string]interface{}{
		"ip":   "10.199.146.201",
		"tags": map[string]string{"hostType": "h"}, //hostType = "c" 容器类型
	}
	labels = append(labels, conditionMap)
	//queryType:  0:分钟级数据查询;   1:秒级数据查询
	//aggType:  聚合查询类型,支持最大值、最小值以及平均值的查询，分别为 max_over_time,min_over_time,avg_over_time
	aggregationQuery, err := mdcApi.AggregationQuery(1622476800000, 1622477100000, labels, 1, "max_over_time")
	if err != nil {
		t.Fatal(err)
	}
	if len(aggregationQuery) != 0 {
		t.Log(aggregationQuery[0])
	}
}

func TestQuery(t *testing.T) {
	metrics := make([]string, 0, 1)
	metrics = append(metrics, MinMemUsagePercent)
	mdcApi := NewMDCOpenApi(AGGToken, metrics, CnSiteType)
	labels := make([]map[string]interface{}, 0)
	conditionMap := map[string]interface{}{
		"ip":   "11.5.208.72",
		"tags": map[string]string{"hostType": "h"}, //hostType = "c" 容器类型
	}
	labels = append(labels, conditionMap)
	//queryType:  0:分钟级数据查询;   1:秒级数据查询
	aggregationQuery, err := mdcApi.Query(1682307535000, 1682307540000, labels, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(aggregationQuery) != 0 {
		t.Log(aggregationQuery[0].MetricResponseList)
		for _, info := range aggregationQuery[0].MetricResponseList {
			fmt.Println(info)
		}
	}
}

func TestCheckDeviceNameAndMountPath(t *testing.T) {
	now := time.Now().Add(-time.Minute * 1)
	endTime := now.UnixNano() / 1e6
	startTime := now.Add(-time.Minute*10).UnixNano() / 1e6
	device, err := CheckDeviceNameAndMountPath("11.5.208.72", startTime, endTime)
	if err != nil {
		t.Fatal(err)
	}
	for _, info := range device {
		fmt.Println(info)
	}
}

func TestGetMountPathDeviceMountPathInfoMap(t *testing.T) {
	device, err := GetMountPathDeviceMountPathInfoMap("11.5.208.110")
	if err != nil {
		t.Fatal(err)
	}
	for _, info := range device {
		fmt.Println(info)
	}
}

func TestGetIPDeviceNameMountPath(t *testing.T) {
	device, err := GetIPDeviceNameMountPath("11.5.208.72", "sdi")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(device)
}

func TestGetHighDiskBusyInfos(t *testing.T) {
	ips := []string{
		"11.5.208.21",
		"11.5.208.72",
		"11.7.148.36",
		"11.5.208.163",
		"11.5.208.195",
		"11.5.208.40",
		"11.5.208.74",
		"11.5.208.181",
		"11.5.208.175",
		"11.5.208.19",
		"11.5.208.234",
		"11.5.208.81",
		"11.5.208.134",
		"11.5.208.68",
		"11.5.208.100",
		"11.5.208.67",
		"11.5.208.78",
		"11.5.208.114",
		"11.5.208.44",
		"11.5.208.77",
	}

	ratio := 90.0
	now := time.Now().Add(-time.Minute * 1)
	endTime := now.UnixNano() / 1e6
	startTime := now.Add(-time.Minute*10).UnixNano() / 1e6
	diskBusyInfos, err := GetHighDiskBusyInfos(ips, startTime, endTime, ratio)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("ip,diskPath,busy,usedRatio,device")
	for _, info := range diskBusyInfos {
		fmt.Printf("%v,%v,%v,%v,%v\n", info.IP, info.MountPath, info.DiskBusyPercentMin, info.FsUsagePercentMin, info.DeviceName)
	}
}
