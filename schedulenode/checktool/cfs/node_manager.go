package cfs

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/util/checktool"
	"github.com/cubefs/cubefs/util/log"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"time"
)

func (s *ChubaoFSMonitor) scheduleToCheckNodesAlive() {
	ticker := time.NewTicker(time.Duration(s.scheduleInterval) * time.Second)
	defer func() {
		ticker.Stop()
	}()
	s.checkNodesAlive()
	for {
		select {
		case <-ticker.C:
			s.checkNodesAlive()
		}
	}
}

func (s *ChubaoFSMonitor) checkNodesAlive() {
	for _, host := range s.hosts {
		checkNodeWg.Add(1)
		go func(host *ClusterHost) {
			defer checkNodeWg.Done()
			log.LogInfof("checkNodesAlive [%v] begin", host)
			startTime := time.Now()
			cv, err := getCluster(host)
			if err != nil {
				if isConnectionRefusedFailure(err) {
					msg := fmt.Sprintf("get cluster info from %v failed, err:%v ", host.host, err)
					checktool.WarnBySpecialUmpKey(UMPCFSClusterConnRefused, msg)
					return
				}
				msg := fmt.Sprintf("get cluster info from %v failed,err:%v ", host.host, err)
				checktool.WarnBySpecialUmpKey(UMPCFSNormalWarnKey, msg)
				return
			}
			cv.checkMetaNodeAlive(host)
			cv.checkDataNodeAlive(host, s)
			cv.checkFlashNodeAlive(host)
			cv.checkMetaNodeDiskStat(host, defaultMNDiskMinWarnSize)
			cv.checkMetaNodeDiskStatByMDCInfoFromSre(host, s)
			cv.checkMetaNodeRaftLogBackupAlive(host)
			host.warnInactiveNodesBySpecialUMPKey()
			log.LogInfof("checkNodesAlive [%v] end,cost[%v]", host, time.Since(startTime))
		}(host)
	}
	checkNodeWg.Wait()
}

func (cv *ClusterView) checkMetaNodeAlive(host *ClusterHost) {
	deadNodes := make([]MetaNodeView, 0)
	for _, mn := range cv.MetaNodes {
		if mn.Status == false {
			deadNodes = append(deadNodes, mn)
		}
	}
	inactiveLen := len(deadNodes)
	if inactiveLen == 0 {
		if len(host.deadMetaNodes) != 0 {
			host.deadMetaNodes = make(map[string]*DeadNode, 0)
		}
		return
	}

	inactiveMetaNodes := make(map[string]*DeadNode, 0)
	var (
		metaNode *DeadNode
		ok       bool
	)
	for _, mn := range deadNodes {
		metaNode, ok = host.deadMetaNodes[mn.Addr]
		if !ok {
			metaNode = &DeadNode{ID: mn.ID, Addr: mn.Addr, LastReportTime: time.Now()}
		}
		inactiveMetaNodes[mn.Addr] = metaNode
	}
	host.deadMetaNodes = inactiveMetaNodes
	log.LogWarnf("action[checkMetaNodeAlive] %v has %v inactive meta nodes %v", host.host, len(inactiveMetaNodes), deadNodes)
	msg := fmt.Sprintf("%v has %v inactive meta nodes,some of which have been inactive for five minutes,", host, inactiveLen)
	host.doProcessAlarm(host.deadMetaNodes, msg, metaNodeType)
	if len(inactiveMetaNodes) == 1 {
		mn, err := getMetaNode(host, metaNode.Addr)
		if err != nil {
			return
		}
		if time.Since(mn.ReportTime) > 60*time.Minute && time.Since(host.lastTimeOfflineMetaNode) > 24*time.Hour {
			if isPhysicalMachineFailure(metaNode.Addr) {
				log.LogErrorf("action[isPhysicalMachineFailure] %v meta node:%v", host.host, metaNode.Addr)
				if host.host == "cn.chubaofs.jd.local" || host.host == "cn.chubaofs-seqwrite.jd.local" || host.host == "cn.elasticdb.jd.local" {
					offlineMetaNode(host, metaNode.Addr)
					host.lastTimeOfflineMetaNode = time.Now()
				}
			}
		}
	}
}

func (ch *ClusterHost) doProcessAlarm(nodes map[string]*DeadNode, msg string, nodeType int) {
	var (
		inOneCycle bool
		needAlarm  bool
	)
	for _, dd := range nodes {
		inOneCycle = time.Since(dd.LastReportTime) < checktool.DefaultWarnInternal*time.Second
		if !inOneCycle {
			needAlarm = true
			msg = msg + dd.String() + "\n"
			dd.LastReportTime = time.Now()
		}
	}
	// 荷兰CFS 存在故障节点就执行普通告警
	if needAlarm && ch.host == "nl.chubaofs.jd.local" && len(nodes) > 0 {
		checktool.WarnBySpecialUmpKey(UMPCFSNormalWarnKey, msg)
	}
	if needAlarm && len(nodes) >= defaultMaxInactiveNodes {
		checktool.WarnBySpecialUmpKey(UMPKeyInactiveNodes, msg)
	}
	if len(nodes) == 1 {
		return
	}
	if len(nodes) <= defaultMaxInactiveNodes && nodeType == dataNodeType {
		ch.doProcessDangerousDp(nodes)
	}
	return
}

func (ch *ClusterHost) doProcessDangerousDp(nodes map[string]*DeadNode) {
	inOneCycle := time.Since(ch.lastTimeAlarmDP) < checktool.DefaultWarnInternal*time.Second
	if inOneCycle {
		return
	}
	sentryMap := make(map[uint64]int, 0)
	dangerDps := make([]uint64, 0)
	for _, dd := range nodes {
		dataNode, err := ch.getDataNode(dd.Addr)
		if err != nil {
			continue
		}
		for _, id := range dataNode.PersistenceDataPartitions {
			count, ok := sentryMap[id]
			if !ok {
				sentryMap[id] = 1
			} else {
				sentryMap[id] = count + 1
				dangerDps = append(dangerDps, id)
			}
		}
	}

	if len(dangerDps) != 0 {
		ips := ""
		for addr, _ := range nodes {
			ips += ips + addr + ","
		}
		msg := fmt.Sprintf("%v has %v inactive data nodes,ips[%v],dangerous data partitions[%v]", ch.host, len(nodes), ips, dangerDps)
		checktool.WarnBySpecialUmpKey(UMPKeyInactiveNodes, msg)
	}
	return
}

func (ch *ClusterHost) getDataNode(addr string) (dataNode *DataNode, err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("action[getDataNode] host[%v],addr[%v],err[%v]", ch.host, addr, err)
		}
	}()
	reqURL := fmt.Sprintf("http://%v/dataNode/get?addr=%v", ch.host, addr)
	data, err := doRequest(reqURL, ch.isReleaseCluster)
	if err != nil {
		return
	}
	dataNode = &DataNode{}
	if err = json.Unmarshal(data, dataNode); err != nil {
		return
	}
	return
}

func (cv *ClusterView) checkFlashNodeAlive(host *ClusterHost) {
	deadNodes := make([]FlashNodeView, 0)
	for _, fn := range cv.FlashNodes {
		if fn.Status == false {
			deadNodes = append(deadNodes, fn)
		}
	}
	inactiveLen := len(deadNodes)
	if inactiveLen == 0 {
		if len(host.deadFlashNodes) != 0 {
			host.deadFlashNodes = make(map[string]*DeadNode, 0)
		}
		return
	}
	inactiveFlashNodes := make(map[string]*DeadNode, 0)
	var (
		flashNode *DeadNode
		ok        bool
	)
	for _, fn := range deadNodes {
		flashNode, ok = host.deadFlashNodes[fn.Addr]
		if !ok {
			flashNode = &DeadNode{Addr: fn.Addr, LastReportTime: time.Now()}
		}
		inactiveFlashNodes[fn.Addr] = flashNode
	}
	host.deadFlashNodes = inactiveFlashNodes
	log.LogWarnf("action[checkFlashNodeAlive] %v has %v inactive flash nodes", host.host, len(inactiveFlashNodes))
	msg := fmt.Sprintf("%v has %v inactive flash nodes,some of which have been inactive for five minutes,", host, inactiveLen)
	host.doProcessAlarm(host.deadFlashNodes, msg, flashNodeType)
	for _, fn := range inactiveFlashNodes {
		flashNodeView, err := getFlashNode(host, fn.Addr)
		if err != nil {
			return
		}
		//offlineBadFlashNode(host)
		if time.Since(flashNodeView.ReportTime) > 30*time.Minute {
			offlineFlashNode(host, flashNodeView.Addr)
		}
	}
}

func (cv *ClusterView) checkDataNodeAlive(host *ClusterHost, s *ChubaoFSMonitor) {
	deadNodes := make([]DataNodeView, 0)
	for _, dn := range cv.DataNodes {
		if dn.Status == false {
			deadNodes = append(deadNodes, dn)
		}
	}
	inactiveLen := len(deadNodes)
	if inactiveLen == 0 {
		if len(host.deadDataNodes) != 0 {
			host.deadDataNodes = make(map[string]*DeadNode, 0)
		}
		return
	}
	inactiveDataNodes := make(map[string]*DeadNode, 0)
	var (
		dataNode *DeadNode
		ok       bool
	)
	for _, dn := range deadNodes {
		dataNode, ok = host.deadDataNodes[dn.Addr]
		if !ok {
			dataNode = &DeadNode{Addr: dn.Addr, LastReportTime: time.Now()}
		}
		inactiveDataNodes[dn.Addr] = dataNode
	}
	host.deadDataNodes = inactiveDataNodes
	log.LogWarnf("action[checkDataNodeAlive] %v has %v inactive data nodes %v", host.host, len(inactiveDataNodes), deadNodes)
	msg := fmt.Sprintf("%v has %v inactive data nodes,some of which have been inactive for five minutes,", host, inactiveLen)
	host.doProcessAlarm(host.deadDataNodes, msg, dataNodeType)
	nodeZoneMap, err := getNodeToZoneMap(host)
	if err != nil {
		log.LogErrorf("action[getNodeToZoneMap] host[%v] err[%v]", host.host, err)
	}
	if len(inactiveDataNodes) == 1 {
		for _, dn := range inactiveDataNodes {
			dataNodeView, err := getDataNode(host, dn.Addr)
			if err != nil {
				return
			}
			if time.Since(dataNodeView.ReportTime) > 30*time.Minute {
				if isPhysicalMachineFailure(dn.Addr) {
					log.LogErrorf("action[isPhysicalMachineFailure],addr[%v],err[%v]", dn.Addr, err)
					//超过一定时间 尝试重启机器
					if host.host == "cn.chubaofs.jd.local" {
						if err1 := s.checkThenRestartNode(dn.Addr, host.host); err1 != nil {
							log.LogErrorf("action[checkThenRestartNode] addr[%v] err1[%v]", dn.Addr, err1)
						}
					}
					/* 不再发起节点下线xbp单子
					var reqURL string
					if host.isReleaseCluster {
						reqURL = fmt.Sprintf("http://%v/dataNode/offline?addr=%v", host, deadNodes[0].Addr)
					} else {
						reqURL = fmt.Sprintf("http://%v/dataNode/decommission?addr=%v", host, deadNodes[0].Addr)
					}
					s.addDataNodeOfflineXBPTicket(deadNodes[0].Addr, reqURL, cv.Name, host.isReleaseCluster)
					*/
					// 清理超过24小时的记录
					for key, t := range host.offlineDataNodesIn24Hour {
						if time.Since(t) > 24*time.Hour {
							delete(host.offlineDataNodesIn24Hour, key)
						}
					}
					// 如果持续小于1个小时，不进行自动处理
					if time.Since(dataNodeView.ReportTime) < time.Hour {
						continue
					}
					// 判断24小时内下线的数目，符合要求则加入待执行下线磁盘的DataNode map
					if len(host.offlineDataNodesIn24Hour) < s.offlineDataNodeMaxCountIn24Hour {
						host.offlineDataNodesIn24Hour[dataNodeView.Addr] = time.Now()
						log.LogDebugf("action[checkDataNodeAlive] offlineDataNodesIn24Hour:%v", host.offlineDataNodesIn24Hour)
						if _, ok = host.inOfflineDiskDataNodes[dataNodeView.Addr]; !ok {
							host.inOfflineDiskDataNodes[dataNodeView.Addr] = dataNodeView.ReportTime
							log.LogDebugf("action[checkDataNodeAlive] inOfflineDiskDataNodes:%v", host.inOfflineDiskDataNodes)
						}
					}
					// 对待下线DataNode执行下线磁盘操作
					offlineBadDataNodeOneDisk(host)
					zoneName := nodeZoneMap[dataNodeView.Addr]
					if (isSSD(host.host, zoneName) && time.Since(dataNodeView.ReportTime) > time.Hour) ||
						time.Since(dataNodeView.ReportTime) > 8*time.Hour {
						if isSSD(host.host, zoneName) {
							offlineDataNode(host, dataNodeView.Addr)
							delete(host.inOfflineDiskDataNodes, dataNodeView.Addr)
							continue
						}
						if _, ok = host.inOfflineDiskDataNodes[dataNodeView.Addr]; ok {
							// 如果超过8小时 直接执行节点下线
							// 需要控制节点剩余DP数量，避免正在下线的DP数量过多
							badDPsCount, err1 := getBadPartitionIDsCount(host)
							if err1 != nil || badDPsCount > maxBadDataPartitionsCount {
								log.LogWarn(fmt.Sprintf("action[checkDataNodeAlive] getBadPartitionIDsCount host:%v badDPsCount:%v err:%v", host, badDPsCount, err1))
								continue
							}
							nodeView, err1 := getDataNode(host, dataNodeView.Addr)
							if err1 != nil {
								log.LogWarn(fmt.Sprintf("action[checkDataNodeAlive] getDataNode host:%v addr:%v err:%v", host, dataNodeView.Addr, err1))
								continue
							}
							badDPsCount += len(nodeView.PersistenceDataPartitions)
							if badDPsCount > maxBadDataPartitionsCount {
								continue
							}
							offlineDataNode(host, dataNodeView.Addr)
							delete(host.inOfflineDiskDataNodes, dataNodeView.Addr)
						}
					}
				}
			}
		}
	}
}

func offlineBadDataNodeOneDisk(host *ClusterHost) {
	var diskPathsMap = map[int][]string{
		0: {"/data0", "/data6"},
		1: {"/data1", "/data7"},
		2: {"/data2", "/data8"},
		3: {"/data3", "/data9"},
		4: {"/data4", "/data10"},
		5: {"/data5", "/data11"},
	}
	var dbbakDiskPathsMap = map[int][]string{
		0: {"/cfsd0", "/cfsd6"},
		1: {"/cfsd1", "/cfsd7"},
		2: {"/cfsd2", "/cfsd8"},
		3: {"/cfsd3", "/cfsd9"},
		4: {"/cfsd4", "/cfsd10"},
		5: {"/cfsd5", "/cfsd11"},
	}
	for dataNodeAddr, lastOfflineDiskTime := range host.inOfflineDiskDataNodes {
		if time.Since(lastOfflineDiskTime) > time.Minute*30 {
			if !isPhysicalMachineFailure(dataNodeAddr) {
				delete(host.inOfflineDiskDataNodes, dataNodeAddr)
				continue
			}
			dataNodeView, err := getDataNode(host, dataNodeAddr)
			if err != nil {
				return
			}
			if time.Since(dataNodeView.ReportTime) < 5*time.Hour {
				delete(host.inOfflineDiskDataNodes, dataNodeAddr)
				continue
			}
			// 获取要被下线的磁盘地址 根据时间计算
			offlineDiskIndex := int((time.Since(dataNodeView.ReportTime) - 5*time.Hour) / (30 * time.Minute))
			diskPaths, ok := diskPathsMap[offlineDiskIndex%6]
			if !ok {
				continue
			}
			for _, diskPath := range diskPaths {
				offlineDataNodeDisk(host, dataNodeAddr, diskPath, false)
			}
			if host.isReleaseCluster {
				diskPaths, ok = dbbakDiskPathsMap[offlineDiskIndex%6]
				if !ok {
					continue
				}
				for _, diskPath := range diskPaths {
					offlineDataNodeDisk(host, dataNodeAddr, diskPath, false)
				}
			}
			host.inOfflineDiskDataNodes[dataNodeAddr] = time.Now()
		}
	}
}

func isPhysicalMachineFailure(addr string) (isPhysicalFailure bool) {
	var err error
	defer func() {
		if err != nil {
			log.LogWarnf("action[isPhysicalMachineFailure] node:%v err:%v isPhysicalFailure:%v", addr, err, isPhysicalFailure)
		}
	}()
	_, err = net.DialTimeout("tcp", addr, 3*time.Second)
	if err == nil {
		return false
	}
	oe, ok := err.(net.Error)
	if ok {
		isPhysicalFailure = oe.Timeout()
		return
	}
	return false
}

func doRequest(reqUrl string, isReleaseCluster bool) (data []byte, err error) {
	var resp *http.Response
	client := http.Client{Timeout: time.Minute * 5}
	req, err := http.NewRequest(http.MethodGet, reqUrl, nil)
	if err != nil {
		log.LogErrorf("action[doRequest] reqRUL[%v] new request occurred err:%v\n", reqUrl, err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Connection", "close")
	if resp, err = client.Do(req); err != nil {
		log.LogErrorf("action[doRequest] reqRUL[%v] err:%v\n", reqUrl, err)
		return
	}
	defer resp.Body.Close()

	if data, err = ioutil.ReadAll(resp.Body); err != nil {
		log.LogErrorf("action[doRequest] reqRUL[%v] remoteAddr:%v,err:%v\n", reqUrl, resp.Request.RemoteAddr, err)
		if len(data) != 0 {
			log.LogErrorf("action[doRequest] ioutil.ReadAll data:%v", string(data))
		}
		return
	}
	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("action[doRequest] reqRUL[%v],statusCode[%v],body[%v]", reqUrl, resp.StatusCode, string(data))
		return
	}
	if !isReleaseCluster {
		reply := HTTPReply{}
		if err = json.Unmarshal(data, &reply); err != nil {
			log.LogErrorf("action[doRequest] reqRUL[%v] err:%v\n", reqUrl, err)
			return
		}
		data = reply.Data
		if len(data) <= 4 && string(data) == "null" && len(reply.Msg) != 0 {
			data = []byte(reply.Msg)
		}
	}
	return
}

func doRequestWithTimeOut(reqUrl string, overtime time.Duration) (data []byte, err error) {
	if overtime < 0 {
		overtime = 5
	}
	var resp *http.Response
	client := http.Client{Timeout: overtime * time.Second}
	if resp, err = client.Get(reqUrl); err != nil {
		return
	}
	defer resp.Body.Close()

	if data, err = ioutil.ReadAll(resp.Body); err != nil {
		return
	}
	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("action[doRequestWithTimeOut] reqRUL[%v],statusCode[%v],body[%v]", reqUrl, resp.StatusCode, string(data))
		return
	}
	return
}

func getCluster(host *ClusterHost) (cv *ClusterView, err error) {
	reqURL := fmt.Sprintf("http://%v/admin/getCluster", host)
	data, err := doRequest(reqURL, host.isReleaseCluster)
	if err != nil {
		return
	}
	cv = &ClusterView{}
	if err = json.Unmarshal(data, cv); err != nil {
		log.LogErrorf("get cluster from %v failed ,data:%v,err:%v", host, string(data), err)
		return
	}
	if !host.isReleaseCluster {
		cv.DataNodeStat = cv.DataNodeStatInfo
		cv.MetaNodeStat = cv.MetaNodeStatInfo
		if cv.VolStat, err = GetVolStatFromVolList(host.host, host.isReleaseCluster); err != nil {
			return
		}
		if len(cv.VolStatInfo) == 0 {
			cv.VolStatInfo = cv.VolStat
		}
	}
	log.LogInfof("action[getCluster],host[%v],len(VolStat)=%v,len(metaNodes)=%v,len(dataNodes)=%v",
		host, len(cv.VolStat), len(cv.MetaNodes), len(cv.DataNodes))
	return
}

func GetVolStatFromVolList(host string, isReleaseCluster bool) (volStats []*VolSpaceStat, err error) {
	volInfos, err := GetVolList(host, isReleaseCluster)
	if err != nil {
		return
	}
	volStats = make([]*VolSpaceStat, 0)
	for _, volInfo := range volInfos {
		stat := &VolSpaceStat{
			Name: volInfo.Name,
		}
		if volInfo.TotalSize >= 0 {
			stat.TotalSize = uint64(volInfo.TotalSize)
		}
		if volInfo.UsedSize >= 0 {
			stat.UsedSize = uint64(volInfo.UsedSize)
		}
		stat.TotalGB = stat.TotalSize / GB
		stat.UsedGB = stat.UsedSize / GB
		if stat.TotalSize != 0 {
			stat.UsedRatio = fmt.Sprintf("%.3f", float64(stat.UsedSize)/float64(stat.TotalSize))
		}
		volStats = append(volStats, stat)
	}
	return
}

func GetVolList(host string, isReleaseCluster bool) (volInfos []VolInfo, err error) {
	reqURL := fmt.Sprintf("http://%v/vol/list", host)
	data, err := doRequest(reqURL, isReleaseCluster)
	if err != nil {
		return
	}
	if err = json.Unmarshal(data, &volInfos); err != nil {
		log.LogErrorf("get vol list %v failed ,data:%v,err:%v", host, string(data), err)
		return
	}
	log.LogInfof("action[GetVolList],host[%v],len(VolStat)=%v ", host, len(volInfos))
	return
}

func getNodeToZoneMap(host *ClusterHost) (nodeZoneMap map[string]string, err error) {
	topologyView, err := getTopology(host)
	if err != nil {
		return
	}
	nodeZoneMap = make(map[string]string, 0)
	for _, zoneView := range topologyView.Zones {
		for _, setView := range zoneView.NodeSet {
			for _, dn := range setView.DataNodes {
				nodeZoneMap[dn.Addr] = zoneView.Name
			}
			for _, mn := range setView.MetaNodes {
				nodeZoneMap[mn.Addr] = zoneView.Name
			}
		}
	}
	return
}

func getTopology(host *ClusterHost) (tv *TopologyView, err error) {
	reqURL := fmt.Sprintf("http://%v/topo/get", host)
	data, err := doRequest(reqURL, host.isReleaseCluster)
	if err != nil {
		return
	}
	tv = &TopologyView{}
	if err = json.Unmarshal(data, tv); err != nil {
		log.LogErrorf("getTopology from %v failed ,data:%v,err:%v", host, string(data), err)
		return
	}
	return
}

func getMetaNode(host *ClusterHost, addr string) (mn *MetaNodeView, err error) {
	reqURL := fmt.Sprintf("http://%v/metaNode/get?addr=%v", host, addr)
	data, err := doRequest(reqURL, host.isReleaseCluster)
	if err != nil {
		return
	}
	mn = &MetaNodeView{}
	if err = json.Unmarshal(data, mn); err != nil {
		log.LogErrorf("get metaNode information from %v failed ,data:%v,err:%v", host, string(data), err)
		return
	}
	log.LogInfof("action[getMetaNode],host[%v],addr[%v],reportTime[%v]",
		host, addr, mn.ReportTime)
	return
}

func getDataNode(host *ClusterHost, addr string) (dn *DataNodeView, err error) {
	reqURL := fmt.Sprintf("http://%v/dataNode/get?addr=%v", host, addr)
	data, err := doRequest(reqURL, host.isReleaseCluster)
	if err != nil {
		return
	}
	dn = &DataNodeView{}
	if err = json.Unmarshal(data, dn); err != nil {
		log.LogErrorf("get getDataNode information from %v failed ,data:%v,err:%v", host, string(data), err)
		return
	}
	log.LogInfof("action[getDataNode],host[%v],addr[%v],reportTime[%v]",
		host, addr, dn.ReportTime)
	return
}

func offlineMetaNode(host *ClusterHost, addr string) {
	var reqURL string
	if host.isReleaseCluster {
		reqURL = fmt.Sprintf("http://%v/metaNode/offline?addr=%v", host, addr)
	} else {
		reqURL = fmt.Sprintf("http://%v/metaNode/decommission?addr=%v", host, addr)
	}
	data, err := doRequest(reqURL, host.isReleaseCluster)
	if err != nil {
		log.LogErrorf("action[offlineMetaNode] occurred err,url[%v],err %v", reqURL, err)
		return
	}
	msg := fmt.Sprintf("action[offlineMetaNode] reqURL[%v],data[%v]", reqURL, string(data))
	checktool.WarnBySpecialUmpKey(UMPCFSNormalWarnKey, msg)
	return
}

func offlineDataNode(host *ClusterHost, addr string) {
	var reqURL string
	if host.isReleaseCluster {
		reqURL = fmt.Sprintf("http://%v/dataNode/offline?addr=%v", host, addr)
	} else {
		reqURL = fmt.Sprintf("http://%v/dataNode/decommission?addr=%v", host, addr)
	}
	data, err := doRequest(reqURL, host.isReleaseCluster)
	if err != nil {
		log.LogErrorf("action[offlineDataNode] occurred err,url[%v],err %v", reqURL, err)
		return
	}
	msg := fmt.Sprintf("action[offlineDataNode] reqURL[%v],data[%v]", reqURL, string(data))
	checktool.WarnBySpecialUmpKey(UMPCFSNormalWarnKey, msg)
	return
}

func (cv *ClusterView) checkMetaNodeDiskStat(host *ClusterHost, diskMinWarnSize int) {
	var port string
	// exclude hosts which have not update the meta node disk stat API
	if host.host == "cn.chubaofs-seqwrite.jd.local" || host.host == "id.chubaofs-seqwrite.jd.local" || host.host == "th.chubaofs-seqwrite.jd.local" {
		return
	}
	// set meta node port
	checkedCount := 0
	port = host.getMetaNodePProfPort()
	diskWarnNodes := make([]string, 0)
	for _, mn := range cv.MetaNodes {
		if mn.Status == false {
			continue
		}
		ipPort := strings.Split(mn.Addr, ":")
		isNeedTelAlarm, err := doCheckMetaNodeDiskStat(ipPort[0], port, host.isReleaseCluster, diskMinWarnSize)
		if err != nil {
			log.LogErrorf("action[checkMetaNodeDiskStat] host[%v] addr[%v] doCheckMetaNodeDiskStat err[%v]", host, mn.Addr, err)
			continue
		}
		checkedCount++
		if isNeedTelAlarm {
			diskWarnNodes = append(diskWarnNodes, mn.Addr)
		}
	}
	if len(diskWarnNodes) == 0 {
		log.LogInfof("action[checkMetaNodeDiskStat] host[%v] MN count[%v] checkedCount:%v diskStatWarnMetaNodes is 0", host, len(cv.MetaNodes), checkedCount)
		return
	}
	msg := fmt.Sprintf("%v has disk less than %vGB meta nodes:%v", host, diskMinWarnSize/GB, diskWarnNodes)
	if time.Since(host.metaNodeDiskUsedWarnTime) >= time.Minute*5 {
		checktool.WarnBySpecialUmpKey(UMPKeyMetaNodeDiskSpace, msg)
		host.metaNodeDiskUsedWarnTime = time.Now()
	} else {
		log.LogWarnf("action[checkMetaNodeDiskStat] :%v", msg)
	}
}

var (
	excludeCheckMetaNodeRaftLogBackupHosts = []string{"nl.chubaofs.ochama.com", "nl.chubaofs.jd.local"}
)

func (cv *ClusterView) checkMetaNodeRaftLogBackupAlive(host *ClusterHost) {
	for _, raftLogBackupHost := range excludeCheckMetaNodeRaftLogBackupHosts {
		if host.host == raftLogBackupHost {
			return
		}
	}
	raftLogBackupWarnMetaNodes := make([]string, 0)
	port := 15000
	for _, metaNode := range cv.MetaNodes {
		ipPort := strings.Split(metaNode.Addr, ":")
		isNeedAlarm, err := doCheckMetaNodeRaftLogBackupStat(ipPort[0], port)
		if err != nil {
			log.LogWarnf("action[checkMetaNodeRaftLogBackupAlive] host[%v] addr[%v] doCheckMetaNodeDiskStat err[%v]", host, metaNode.Addr, err)
		}
		if isNeedAlarm {
			raftLogBackupWarnMetaNodes = append(raftLogBackupWarnMetaNodes, metaNode.Addr)
		}
	}
	if len(raftLogBackupWarnMetaNodes) == 0 {
		return
	}
	msg := fmt.Sprintf("checkMetaNodeRaftLogBackupAlive: host[%v], fault count[%v] fault ips[%v]", host.host, len(raftLogBackupWarnMetaNodes), raftLogBackupWarnMetaNodes)
	checktool.WarnBySpecialUmpKey(UMPCFSRaftlogBackWarnKey, msg)
}

func doCheckMetaNodeDiskStat(ip, port string, isReleaseCluster bool, diskMinWarnSize int) (isNeedTelAlarm bool, err error) {
	type Disk struct {
		Path      string  `json:"Path"`
		Total     float64 `json:"Total"`
		Used      float64 `json:"Used"`
		Available float64 `json:"Available"`
	}
	// curl "http://172.26.36.130:17220/getDiskStat"
	reqURL := fmt.Sprintf("http://%v:%v/getDiskStat", ip, port)
	data, err := doRequest(reqURL, isReleaseCluster)
	if err != nil {
		return
	}
	disks := make([]Disk, 0)
	if err = json.Unmarshal(data, &disks); err != nil {
		return
	}
	for _, disk := range disks {
		if disk.Total > 0 && disk.Used/disk.Total < defaultMNDiskMinWarnRatio {
			continue
		}
		if disk.Available < float64(diskMinWarnSize) {
			isNeedTelAlarm = true
			break
		}
	}
	return
}

func doCheckMetaNodeRaftLogBackupStat(ip string, port int) (bool, error) {
	var returnErr error = nil
	for i := 0; i <= 10; i++ {
		reqURL := fmt.Sprintf("http://%v:%v/status", ip, port)
		data, err := doRequestWithTimeOut(reqURL, 3)
		if err != nil {
			// 端口递增检测
			port++
			returnErr = err
			continue
		}
		if string(data) == "running" {
			return false, nil
		} else {
			return true, err
		}
	}
	return true, returnErr
}

func (ch *ClusterHost) doProcessMetaNodeDiskStatAlarm(nodes map[string]*DeadNode, msg string) {
	var (
		inOneCycle bool
		needAlarm  bool
	)
	for _, dd := range nodes {
		inOneCycle = time.Since(dd.LastReportTime) < checktool.DefaultWarnInternal*time.Second
		if !inOneCycle {
			needAlarm = true
			msg = msg + dd.String() + "\n"
			dd.LastReportTime = time.Now()
		}
	}
	if needAlarm {
		checktool.WarnBySpecialUmpKey(UMPKeyMetaNodeDiskSpace, msg)
	}
	return
}

func isConnectionRefusedFailure(err error) bool {
	if strings.Contains(strings.ToLower(err.Error()), strings.ToLower(errorConnRefused)) {
		return true
	}
	return false
}

func (ch *ClusterHost) warnInactiveNodesBySpecialUMPKey() {
	if len(ch.deadDataNodes) == 0 && len(ch.deadMetaNodes) == 0 {
		return
	}
	//可能存在节点在线，但是因为机器负载大，进而导致没能按时给master上报心跳的情况，而这种情况运维也暂时无法处理
	//对于异常机器，检查进程是否存在，master视图异常但进程连续多次存在，才执行告警
	ch.checkDeadNodesProcessStatus(10)
	ch.doWarnInactiveNodesBySpecialUMPKey()
}

// 如果检测统计的ProcessStatusCount 大于 needWarnCount 会执行告警，约1分钟一次
func (ch *ClusterHost) checkDeadNodesProcessStatus(needWarnCount int) {
	// 检查进程的启动情况
	for _, deadNode := range ch.deadDataNodes {
		ch.checkDeadNodeStartStatus(deadNode, ch.getDataNodePProfPort(), needWarnCount)
	}
	for _, deadNode := range ch.deadMetaNodes {
		ch.checkDeadNodeStartStatus(deadNode, ch.getMetaNodePProfPort(), needWarnCount)
	}
}

func (ch *ClusterHost) checkDeadNodeStartStatus(deadNode *DeadNode, port string, needWarnCount int) {
	nodeStatus, err := checkNodeStartStatus(fmt.Sprintf("%v:%v", strings.Split(deadNode.Addr, ":")[0], port), 5)
	if err == nil && nodeStatus.Version != "" {
		//Version信息不为空时才认为是获取成功了
		deadNode.ProcessStatusCount++
		if deadNode.ProcessStatusCount >= needWarnCount {
			deadNode.IsNeedWarnBySpecialUMPKey = true
		}
	} else {
		deadNode.IsNeedWarnBySpecialUMPKey = true
	}
}

func (ch *ClusterHost) getDataNodePProfPort() (port string) {
	switch ch.host {
	case "id.chubaofs.jd.local", "th.chubaofs.jd.local":
		port = "17320"
	case "cn.chubaofs.jd.local", "cn.elasticdb.jd.local", "cn.chubaofs-seqwrite.jd.local", "idbbak.chubaofs.jd.local", "nl.chubaofs.jd.local", "nl.chubaofs.ochama.com":
		port = "6001"
	case "192.168.0.11:17010", "192.168.0.12:17010", "192.168.0.13:17010":
		port = "17320"
	default:
		port = "6001"
	}
	return
}

func (ch *ClusterHost) getMetaNodePProfPort() (port string) {
	switch ch.host {
	case "id.chubaofs.jd.local", "th.chubaofs.jd.local":
		port = "17220"
	case "cn.chubaofs.jd.local", "cn.elasticdb.jd.local", "cn.chubaofs-seqwrite.jd.local", "idbbak.chubaofs.jd.local", "nl.chubaofs.jd.local", "nl.chubaofs.ochama.com":
		port = "9092"
	case "192.168.0.11:17010", "192.168.0.12:17010", "192.168.0.13:17010":
		port = "17220"
	default:
		port = "9092"
	}
	return
}

func (ch *ClusterHost) doWarnInactiveNodesBySpecialUMPKey() {
	if time.Since(ch.lastTimeWarn) <= time.Minute*5 {
		return
	}
	inactiveDataNodeCount := 0
	inactiveMetaNodeCount := 0
	nodes := make([]string, 0)
	for _, deadNode := range ch.deadDataNodes {
		if deadNode.IsNeedWarnBySpecialUMPKey {
			nodes = append(nodes, deadNode.Addr)
			inactiveDataNodeCount++
		}
	}
	for _, deadNode := range ch.deadMetaNodes {
		if deadNode.IsNeedWarnBySpecialUMPKey {
			nodes = append(nodes, deadNode.Addr)
			inactiveMetaNodeCount++
		}
	}
	if inactiveDataNodeCount == 0 && inactiveMetaNodeCount == 0 {
		return
	}
	//如果有一台机器故障不告警, 两台需要判断是否是同一个机器
	//检查是不是机器异常，如果是机器异常才可以忽略
	if len(nodes) == 0 {
		return
	}
	if len(nodes) == 1 || len(nodes) == 2 && getIpFromIpPort(nodes[0]) == getIpFromIpPort(nodes[1]) {
		if isPhysicalMachineFailure(nodes[0]) {
			log.LogInfo(fmt.Sprintf("doWarnInactiveNodesBySpecialUMPKey isPhysicalMachineFailure ignore warn,cluster:%v, nodes:%v", ch.host, nodes))
			return
		}
		//一台机器，能连通的情况下，查询机器的启动时间，小于30分钟不进行告警
		nodeIp := getIpFromIpPort(nodes[0])
		totalStartupTime, err := GetNodeTotalStartupTime(nodeIp)
		if err == nil && totalStartupTime < MinUptimeThreshold {
			log.LogInfo(fmt.Sprintf("doWarnInactiveNodesBySpecialUMPKey cluster:%v,totalStartupTime:%v,ignore warn, nodes:%v", ch.host, totalStartupTime, nodes))
			return
		}
		if err != nil {
			deadNode, ok := ch.deadDataNodes[nodes[0]]
			if !ok {
				deadNode = ch.deadMetaNodes[nodes[0]]
			}
			if deadNode != nil && time.Since(deadNode.LastReportTime) < 45*time.Minute {
				log.LogErrorf("cluster:%v,GetNodeTotalStartupTime failed,ignore warn,totalStartupTime:%v,nodes:%v,err:%v", ch.host, totalStartupTime, nodes, err)
				return
			}
		}
	}
	if inactiveDataNodeCount == 1 && inactiveMetaNodeCount == 1 && ch.host != "cn.elasticdb.jd.local" {
		return
	}
	sb := new(strings.Builder)
	sb.WriteString(fmt.Sprintf("host:%v,", ch.host))
	if inactiveDataNodeCount != 0 {
		sb.WriteString(fmt.Sprintf("inactive DataNode总数:%v,", inactiveDataNodeCount))
	}
	if inactiveMetaNodeCount != 0 {
		sb.WriteString(fmt.Sprintf("inactive MetaNodes总数:%v,", inactiveMetaNodeCount))
	}
	sb.WriteString(fmt.Sprintf("详情:%v", nodes))
	ch.lastTimeWarn = time.Now()
	checktool.WarnBySpecialUmpKey(UMPCFSInactiveNodeWarnKey, sb.String())
}

// 获取 mdc 存入到数据库的日志 磁盘使用率 大于阈值 电话告警
func (cv *ClusterView) checkMetaNodeDiskStatByMDCInfoFromSre(host *ClusterHost, s *ChubaoFSMonitor) {
	if host.host != "cn.elasticdb.jd.local" {
		return
	}
	var (
		err             error
		dashboardMdcIps []string
		highRatioNodes  []string
	)
	defer func() {
		if err != nil {
			log.LogError(fmt.Sprintf("action[checkMetaNodeDiskStatByMDCInfoFromSre] err:%v", err))
		}
	}()
	if s.sreDB == nil {
		err = fmt.Errorf("sreDB is nil")
		return
	}
	//select * from `tb_dashboard_mdc` where origin='cfs' and disk_path='/export' and fs_usage_percent > 75 and time_stamp >= now()-interval 20 minute;
	sqlStr := fmt.Sprintf(" select DISTINCT(ip) from `%s` where origin='cfs' and disk_path='/export' and fs_usage_percent > %v "+
		"and time_stamp >= now()-interval 10 minute ",
		DashboardMdc{}.TableName(), s.metaNodeExportDiskUsedRatio)
	if err = s.sreDB.Raw(sqlStr).Scan(&dashboardMdcIps).Error; err != nil {
		return
	}

	log.LogInfo(fmt.Sprintf("action[checkMetaNodeDiskStatByMDCInfoFromSre] dashboardMdcIps:%v", dashboardMdcIps))
	nodeAddrMap := make(map[string]bool)
	for _, mn := range cv.MetaNodes {
		nodeAddrMap[strings.Split(mn.Addr, ":")[0]] = true
	}
	for _, ip := range dashboardMdcIps {
		if nodeAddrMap[ip] {
			highRatioNodes = append(highRatioNodes, ip)
		}
	}

	if len(highRatioNodes) == 0 {
		return
	}
	msg := fmt.Sprintf("%v has meta nodes export disk used ratio more than %v%%,detail:%v", host, s.metaNodeExportDiskUsedRatio, highRatioNodes)
	if time.Since(host.metaNodeDiskRatioWarnTime) >= time.Minute*5 {
		checktool.WarnBySpecialUmpKey(UMPKeyMetaNodeDiskRatio, msg)
		host.metaNodeDiskRatioWarnTime = time.Now()
	} else {
		log.LogWarnf("action[checkMetaNodeDiskStatByMDCInfoFromSre] :%v", msg)
	}
}

func isSSD(host, zoneName string) bool {
	if host == "cn.elasticdb.jd.local" {
		return true
	}
	if host == "sparkchubaofs.jd.local" && strings.Contains(zoneName, "_ssd") {
		return true
	}
	return false
}

func getFlashNode(host *ClusterHost, addr string) (fn *FlashNodeView, err error) {
	reqURL := fmt.Sprintf("http://%v/flashNode/get?addr=%v", host, addr)
	data, err := doRequest(reqURL, host.isReleaseCluster)
	if err != nil {
		return
	}
	fn = &FlashNodeView{}
	if err = json.Unmarshal(data, fn); err != nil {
		log.LogErrorf("get getDataNode information from %v failed ,data:%v,err:%v", host, string(data), err)
		return
	}
	log.LogInfof("action[getDataNode],host[%v],addr[%v],reportTime[%v]",
		host, addr, fn.ReportTime)
	return
}

func offlineFlashNode(host *ClusterHost, addr string) {
	var reqURL string
	if host.isReleaseCluster {
		return
	} else {
		reqURL = fmt.Sprintf("http://%v/flashNode/decommission?addr=%v", host, addr)
	}
	data, err := doRequest(reqURL, host.isReleaseCluster)
	if err != nil {
		log.LogErrorf("action[offlineFlashNode] occurred err,url[%v],err %v", reqURL, err)
		return
	}
	msg := fmt.Sprintf("action[offlineFlashNode] reqURL[%v],data[%v]", reqURL, string(data))
	checktool.WarnBySpecialUmpKey(UMPCFSNormalWarnKey, msg)
	return
}
