package cfs

import (
	"fmt"
	"github.com/cubefs/cubefs/schedulenode/common/cfs"
	"github.com/cubefs/cubefs/util/checktool"
	"github.com/cubefs/cubefs/util/checktool/mdc"
	"github.com/cubefs/cubefs/util/log"
	"strings"
	"time"
)

const (
	mdcMetricMonitorInfoBatchCount = 30
	mysqlMemWarnThresholdRatio     = 60 //使用率超过60%告警
)

func (s *ChubaoFSMonitor) scheduleToCheckCFSHighIncreaseMemNodes() {
	s.checkCFSHighIncreaseMemNodes()
	for {
		t := time.NewTimer(10 * 60 * time.Second) // 单独的间隔时间
		select {
		case <-s.ctx.Done():
			return
		case <-t.C:
			s.checkCFSHighIncreaseMemNodes()
		}
	}
}

func (s *ChubaoFSMonitor) checkCFSHighIncreaseMemNodes() {
	for _, host := range s.hosts {
		if host.host != "cn.elasticdb.jd.local" {
			continue
		}
		log.LogInfof("checkCFSHighIncreaseMemNodes [%v] begin", host)
		startTime := time.Now()
		s.checkCFSNodeInfosThenWarn(host)
		log.LogInfof("checkCFSHighIncreaseMemNodes [%v] end,cost[%v]", host, time.Since(startTime))
	}
}

func (s *ChubaoFSMonitor) checkCFSNodeInfosThenWarn(host *ClusterHost) {
	var err error
	defer func() {
		if err != nil {
			log.LogError(fmt.Sprintf("checkCFSNodeInfosThenWarn host:%v err:%v", host.host, err))
		}
	}()
	now := time.Now()
	endTime := now.Unix() * 1000
	startTime := now.Add(-5*time.Minute).Unix() * 1000
	mdcNodeInfos, err := getCFSNodeInfos(host, startTime, endTime, []string{mdc.MinMemUsagePercent})
	if err != nil {
		host.nodeMemInfo = make(map[string]float64)
		return
	}
	defer func() {
		highMemNodeIps := make([]string, 0)
		highMemNodeMsg := new(strings.Builder)
		host.nodeMemInfo = make(map[string]float64)
		for _, nodeInfo := range mdcNodeInfos {
			host.nodeMemInfo[nodeInfo.IPAddr] = nodeInfo.Value
			if nodeInfo.Value >= mysqlMemWarnThresholdRatio {
				highMemNodeIps = append(highMemNodeIps, nodeInfo.IPAddr)
				highMemNodeMsg.WriteString(fmt.Sprintf("node:%s,", nodeInfo))
			}
		}
		log.LogDebug(fmt.Sprintf("checkCFSNodeInfosThenWarn host:%v update success", host))
		if len(highMemNodeIps) > 0 {
			msg := fmt.Sprintf("HighMem host:%v count:%v ips:%v", host.host, len(highMemNodeIps), highMemNodeIps)
			log.LogDebug(fmt.Sprintf("checkCFSNodeInfosThenWarn %v", highMemNodeMsg.String()))
			checktool.WarnBySpecialUmpKey(UMPCFSMySqlMemWarnKey, msg)
		}
	}()
	if len(host.nodeMemInfo) == 0 {
		return
	}
	nodeCount := 0
	warnMsg := new(strings.Builder)
	for _, nodeInfo := range mdcNodeInfos {
		if nodeInfo.Value < s.nodeRapidMemIncWarnThreshold {
			continue
		}
		lastVal, ok := host.nodeMemInfo[nodeInfo.IPAddr]
		if !ok || lastVal <= 0 {
			continue
		}
		ratio := (nodeInfo.Value - lastVal) / lastVal
		if ratio > s.nodeRapidMemIncreaseWarnRatio && nodeInfo.Value > 50 {
			nodeCount++
			warnMsg.WriteString(fmt.Sprintf("ip:%v,val:%.f,increase:%.2f;", nodeInfo.IPAddr, nodeInfo.Value, ratio))
		}
	}
	if nodeCount != 0 {
		msg := fmt.Sprintf("Rapid Mem Increase host:%v count:%v detail:%v", host.host, nodeCount, warnMsg.String())
		checktool.WarnBySpecialUmpKey(UMPCFSRapidMemIncreaseWarnKey, msg)
	}
}

func getCFSNodeInfos(host *ClusterHost, startTime, endTime int64, metrics []string) (mdcNodeInfos []*mdc.NodeInfo, err error) {
	ipMap, err := getCFSClusterNodeIpMap(host.host, host.isReleaseCluster)
	if err != nil {
		return
	}
	ips := make([]string, 0, len(ipMap))
	for ip := range ipMap {
		ips = append(ips, ip)
	}
	mdcNodeInfos = getMetricMonitorInfoByIpsFromMDC(startTime, endTime, ips, metrics)
	return
}

func getCFSClusterNodeIpMap(host string, isRelease bool) (ipMap map[string]bool, err error) {
	clusterView, err := cfs.GetCluster(host, isRelease)
	if err != nil {
		return
	}
	ipMap = make(map[string]bool, len(clusterView.DataNodes))
	for _, metaNode := range clusterView.MetaNodes {
		ip := getIpFromIpPort(metaNode.Addr)
		ipMap[ip] = true
	}
	for _, dataNode := range clusterView.DataNodes {
		ip := getIpFromIpPort(dataNode.Addr)
		ipMap[ip] = true
	}
	return
}

func getMetricMonitorInfoByIpsFromMDC(startTime, endTime int64, ips []string, metrics []string) (totalResults []*mdc.NodeInfo) {
	totalResults = make([]*mdc.NodeInfo, 0, len(ips))
	diskLabels := make([]map[string]interface{}, 0, 40)
	for i, ip := range ips {
		if ip == "" {
			log.LogError(fmt.Sprintf("getMetricMonitorInfoByIpsFromMDC empty ip:%v index:%v", ip, i))
			continue
		}
		conditionMap := map[string]interface{}{
			"ip":   ip,
			"tags": map[string]string{"hostType": "h"},
		}
		diskLabels = append(diskLabels, conditionMap)
		if len(diskLabels) >= mdcMetricMonitorInfoBatchCount {
			results := getMetricMonitorInfo(startTime, endTime, diskLabels, metrics)
			totalResults = append(totalResults, results...)
			//time.Sleep(time.Millisecond * 10) // 避免被限速
			diskLabels = make([]map[string]interface{}, 0, 40)
		}
	}
	if len(diskLabels) != 0 {
		results := getMetricMonitorInfo(startTime, endTime, diskLabels, metrics)
		totalResults = append(totalResults, results...)
	}
	return
}

// 开始时间-结束时间，分钟级，每一条只会保留最近的时间的
func getMetricMonitorInfo(startTime, endTime int64, labels []map[string]interface{}, metrics []string) (results []*mdc.NodeInfo) {
	if len(labels) == 0 {
		return
	}
	var err error
	defer func() {
		if err != nil {
			msg := fmt.Sprintf("getMetricMonitorInfo err:%v labels:%v labels:%v", err, len(labels), labels)
			log.LogError(msg)
		}
	}()
	mdcApi := mdc.NewMDCOpenApi(mdc.AGGToken, metrics, mdc.CnSiteType)
	//queryType:  0:分钟级数据查询;   1:秒级数据查询
	var result []*mdc.ResponseInfo
	for i := 0; i < 3; i++ {
		result, err = mdcApi.Query(startTime, endTime, labels, 0)
		if err == nil {
			break
		}
	}
	for _, info := range result {
		for _, mdcInfo := range info.MetricResponseList {
			r := mdc.NewMdcNodeInfo(mdcInfo)
			if r != nil {
				results = append(results, r)
			}
		}
	}
	return
}
