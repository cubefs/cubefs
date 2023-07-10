package cfs

import (
	"fmt"
	"github.com/cubefs/cubefs/util/checktool"
	"github.com/cubefs/cubefs/util/log"
	"time"
)

const (
	otherZone = "other"
)

func (s *ChubaoFSMonitor) scheduleToCheckAndWarnFaultToUsers() {
	s.initWarnFaultToTargetUsers()
	s.checkAndWarnFaultToUsers()
	for {
		t := time.NewTimer(time.Duration(s.WarnFaultToUsersCheckInterval) * time.Second)
		select {
		case <-t.C:
			s.checkAndWarnFaultToUsers()
		}
	}
}

func (s *ChubaoFSMonitor) initWarnFaultToTargetUsers() {
	for _, warnFaultToUser := range s.WarnFaultToUsers {
		warnFaultToUser.host = newClusterHost(warnFaultToUser.ClusterDomain)
	}
}

func (s *ChubaoFSMonitor) checkAndWarnFaultToUsers() {
	defer checktool.HandleCrash()
	for _, warnFaultToUser := range s.WarnFaultToUsers {
		log.LogInfof("checkAndWarnFaultToUsers [%v] begin", warnFaultToUser.host)
		startTime := time.Now()
		err := warnFaultToUser.doCheckAndWarnFaultToUsers()
		if err != nil {
			log.LogErrorf("checkAndWarnFaultToUsers [%v] err:%v", warnFaultToUser.host, err)
		}
		log.LogInfof("checkAndWarnFaultToUsers [%v] end,cost[%v]", warnFaultToUser.host, time.Since(startTime))
	}
}

//定期从master getCluster获取 故障节点 坏盘
//和用户配置的zone信息进行匹配，相同则告警
func (warnFaultToUser *WarnFaultToTargetUsers) doCheckAndWarnFaultToUsers() (err error) {
	if err = warnFaultToUser.updateDeadNodesAndBadDisks(); err != nil {
		return
	}
	// 没有故障信息，直接返回即可
	if len(warnFaultToUser.host.deadDataNodes) == 0 && len(warnFaultToUser.host.deadMetaNodes) == 0 && len(warnFaultToUser.dataNodeBadDisks) == 0 {
		return
	}
	if err = warnFaultToUser.formatClusterBadInfosToZoneMap(); err != nil {
		return
	}
	warnFaultToUser.warnFaultToUsers()
	return
}

func (warnFaultToUser *WarnFaultToTargetUsers) updateDeadNodesAndBadDisks() (err error) {
	cv, err := getCluster(warnFaultToUser.host)
	if err != nil {
		return
	}
	//统计所有异常节点
	inactiveDataNodes := make(map[string]*DeadNode, 0)
	for _, dn := range cv.DataNodes {
		if dn.Status == false {
			dataNode, ok := warnFaultToUser.host.deadDataNodes[dn.Addr]
			if !ok {
				dataNode = &DeadNode{Addr: dn.Addr, LastReportTime: time.Now()}
			}
			inactiveDataNodes[dn.Addr] = dataNode
		}
	}
	warnFaultToUser.host.deadDataNodes = inactiveDataNodes

	inactiveMetaNodes := make(map[string]*DeadNode, 0)
	for _, mn := range cv.MetaNodes {
		if mn.Status == false {
			metaNode, ok := warnFaultToUser.host.deadMetaNodes[mn.Addr]
			if !ok {
				metaNode = &DeadNode{ID: mn.ID, Addr: mn.Addr, LastReportTime: time.Now()}
			}
			inactiveMetaNodes[mn.Addr] = metaNode
		}
	}
	warnFaultToUser.host.deadMetaNodes = inactiveMetaNodes
	warnFaultToUser.dataNodeBadDisks = cv.DataNodeBadDisks
	return
}

func (warnFaultToUser *WarnFaultToTargetUsers) formatClusterBadInfosToZoneMap() (err error) {
	nodeZoneMap, err := getNodeToZoneMap(warnFaultToUser.host)
	if err != nil {
		return
	}
	warnFaultToUser.zoneBadDataNodeMap = make(map[string][]*DeadNode)
	warnFaultToUser.zoneBadMetaNodeMap = make(map[string][]*DeadNode)
	warnFaultToUser.zoneBadNodeDisks = make(map[string][]DataNodeBadDisksView)

	for _, deadNode := range warnFaultToUser.host.deadDataNodes {
		if zoneName, ok := nodeZoneMap[deadNode.Addr]; ok {
			warnFaultToUser.zoneBadDataNodeMap[zoneName] = append(warnFaultToUser.zoneBadDataNodeMap[zoneName], deadNode)
		} else {
			warnFaultToUser.zoneBadDataNodeMap[otherZone] = append(warnFaultToUser.zoneBadDataNodeMap[otherZone], deadNode)
			log.LogWarn(fmt.Sprintf("action[formatClusterBadInfosToZoneMap] data addr:%v can not get zone", deadNode.Addr))
		}
	}
	for _, deadNode := range warnFaultToUser.host.deadMetaNodes {
		if zoneName, ok := nodeZoneMap[deadNode.Addr]; ok {
			warnFaultToUser.zoneBadMetaNodeMap[zoneName] = append(warnFaultToUser.zoneBadMetaNodeMap[zoneName], deadNode)
		} else {
			log.LogWarn(fmt.Sprintf("action[formatClusterBadInfosToZoneMap] meta addr:%v can not get zone", deadNode.Addr))
			warnFaultToUser.zoneBadMetaNodeMap[otherZone] = append(warnFaultToUser.zoneBadMetaNodeMap[otherZone], deadNode)
		}
	}
	// 故障disk
	for _, dataNodeBadDisk := range warnFaultToUser.dataNodeBadDisks {
		if zoneName, ok := nodeZoneMap[dataNodeBadDisk.Addr]; ok {
			warnFaultToUser.zoneBadNodeDisks[zoneName] = append(warnFaultToUser.zoneBadNodeDisks[zoneName], dataNodeBadDisk)
		} else {
			log.LogWarn(fmt.Sprintf("action[formatClusterBadInfosToZoneMap] bad disk addr:%v can not get zone", dataNodeBadDisk.Addr))
			warnFaultToUser.zoneBadNodeDisks[otherZone] = append(warnFaultToUser.zoneBadNodeDisks[otherZone], dataNodeBadDisk)
		}
	}
	return
}

func (warnFaultToUser *WarnFaultToTargetUsers) warnFaultToUsers() {
	for _, targetUserInfo := range warnFaultToUser.Users {
		if msg := warnFaultToUser.getDataNodeWarnMsgOfTargetZones(targetUserInfo.Zones); len(msg) != 0 {
			checktool.WarnToTargetGidByDongDongAlarm(targetUserInfo.TargetGid, targetUserInfo.AppName, "DataNode节点故障", msg)
		}
		if msg := warnFaultToUser.getMetaNodeWarnMsgOfTargetZones(targetUserInfo.Zones); len(msg) != 0 {
			checktool.WarnToTargetGidByDongDongAlarm(targetUserInfo.TargetGid, targetUserInfo.AppName, "MetaNode节点故障", msg)
		}
		if msg := warnFaultToUser.getBadDiskWarnMsgOfTargetZones(targetUserInfo.Zones); len(msg) != 0 {
			checktool.WarnToTargetGidByDongDongAlarm(targetUserInfo.TargetGid, targetUserInfo.AppName, "磁盘故障", msg)
		}
	}
}

func (warnFaultToUser *WarnFaultToTargetUsers) getDataNodeWarnMsgOfTargetZones(zones []string) (msg string) {
	count := 0
	for _, zoneName := range zones {
		if deadDataNodes, ok := warnFaultToUser.zoneBadDataNodeMap[zoneName]; ok {
			for _, dataNode := range deadDataNodes {
				count++
				msg += fmt.Sprintf("%v,", dataNode.Addr)
			}
		}
	}
	if count == 0 {
		return ""
	}
	return fmt.Sprintf("故障DataNode总数:%v\n详情:%v", count, msg)
}

func (warnFaultToUser *WarnFaultToTargetUsers) getMetaNodeWarnMsgOfTargetZones(zones []string) (msg string) {
	count := 0
	for _, zoneName := range zones {
		if deadMetaNodes, ok := warnFaultToUser.zoneBadMetaNodeMap[zoneName]; ok {
			for _, metaNode := range deadMetaNodes {
				count++
				msg += fmt.Sprintf("%v,", metaNode.Addr)
			}
		}
	}
	if count == 0 {
		return ""
	}
	return fmt.Sprintf("故障MetaNode总数:%v\n详情:%v", count, msg)
}

func (warnFaultToUser *WarnFaultToTargetUsers) getBadDiskWarnMsgOfTargetZones(zones []string) (msg string) {
	count := 0
	for _, zoneName := range zones {
		if badNodeDisks, ok := warnFaultToUser.zoneBadNodeDisks[zoneName]; ok {
			for _, badNodeDisk := range badNodeDisks {
				count += len(badNodeDisk.BadDiskPath)
				msg += fmt.Sprintf("addr[%v],DiskPaths:%v", badNodeDisk.Addr, badNodeDisk.BadDiskPath)
			}
		}
	}
	if count == 0 {
		return ""
	}
	return fmt.Sprintf("故障磁盘总数:%v\n详情:%v", count, msg)
}
