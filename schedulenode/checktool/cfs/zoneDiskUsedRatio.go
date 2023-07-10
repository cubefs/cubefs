package cfs

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/schedulenode/common/cfs"
	"github.com/cubefs/cubefs/util/checktool"
	"github.com/cubefs/cubefs/util/log"
	"strings"
	"sync"
	"time"
)

const (
	usedRatioMinThresholdSSD   = 0.82
	usedRatioMinThresholdMysql = 0.75
	usedRatioMaxThresholdMysql = 0.80
	usedRatioMinThresholdOther = 0.75
	usedRatioMaxThresholdOther = 0.80
	masterDomainMysql          = "cn.elasticdb.jd.local"
)

func (s *ChubaoFSMonitor) scheduleToCheckZoneDiskUsedRatio() {
	s.checkZoneDiskUsedRatio()
	for {
		t := time.NewTimer(time.Duration(s.scheduleInterval) * time.Second)
		select {
		case <-t.C:
			s.checkZoneDiskUsedRatio()
		}
	}
}

func (s *ChubaoFSMonitor) checkZoneDiskUsedRatio() {
	var wg sync.WaitGroup
	cluserUsedRatioAlarmInfo := make(map[string]*PackAlarmInfo)
	cluserUsedRatioAlarmInfoLock := new(sync.RWMutex)
	for _, host := range s.hosts {
		wg.Add(1)
		go func(host *ClusterHost) {
			defer checktool.HandleCrash()
			defer wg.Done()
			if host.isReleaseCluster {
				// dbbak master /cluster/stat does not exist
				return
			}
			log.LogDebugf("checkZoneDiskUsedRatio [%v] begin", host)
			startTime := time.Now()
			csv, err := cfs.GetClusterStat(host.host, host.isReleaseCluster)
			if err != nil {
				_, ok := err.(*json.SyntaxError)
				if ok {
					return
				}
				log.LogErrorf("get cluster stat info from %v failed,err:%v", host.host, err)
				return
			}
			var alarmInfo *PackAlarmInfo
			if host.host == masterDomainMysql {
				alarmInfo = s.doCheckZoneDiskUsedRatio(csv, usedRatioMinThresholdMysql, usedRatioMaxThresholdMysql, host)
			} else {
				alarmInfo = s.doCheckZoneDiskUsedRatio(csv, usedRatioMinThresholdOther, usedRatioMaxThresholdOther, host)
			}
			cluserUsedRatioAlarmInfoLock.Lock()
			cluserUsedRatioAlarmInfo[host.host] = alarmInfo
			cluserUsedRatioAlarmInfoLock.Unlock()
			log.LogDebugf("checkZoneDiskUsedRatio [%v] end,cost[%v]", host, time.Since(startTime))
		}(host)
	}
	wg.Wait()
	finalAlarmMsg := strings.Builder{}
	shouldAlarm := false
	shouldTelAlarm := false
	for host, zoneAlarmInfo := range cluserUsedRatioAlarmInfo {
		if !zoneAlarmInfo.shouldAlarm {
			continue
		}
		if zoneAlarmInfo.shouldTelAlarm {
			shouldTelAlarm = true
		}
		finalAlarmMsg.WriteString(fmt.Sprintf("domain[%v] ", host))
		for _, alarmInfo := range zoneAlarmInfo.zoneAlarmInfo {
			if !alarmInfo.shouldAlarm {
				continue
			}
			finalAlarmMsg.WriteString(alarmInfo.alarmMsg)
			shouldAlarm = true
		}
	}
	alarmMsg := finalAlarmMsg.String()
	if shouldAlarm {
		// 1小时一次告警
		if time.Since(s.lastZoneDiskUsedRatioAlarmTime) > time.Hour {
			checktool.WarnBySpecialUmpKey(UMPCFSNormalWarnKey, alarmMsg)
			s.lastZoneDiskUsedRatioAlarmTime = time.Now()
		}
	}
	if shouldTelAlarm {
		if time.Since(s.lastZoneDiskUsedRatioTelAlarm) > time.Hour*24 {
			checktool.WarnBySpecialUmpKey(UMPCFSZoneUsedRatioWarnKey, alarmMsg)
			s.lastZoneDiskUsedRatioTelAlarm = time.Now()
		}
		//超过80%，每60分钟报一次警
		if time.Since(s.lastZoneDiskUsedRatioTelOpAlarm) > time.Minute*60 {
			checktool.WarnBySpecialUmpKey(UMPCFSZoneUsedRatioOPWarnKey, alarmMsg)
			s.lastZoneDiskUsedRatioTelOpAlarm = time.Now()
		}
	}
}

type AlarmInfo struct {
	shouldAlarm bool
	alarmMsg    string
}

type PackAlarmInfo struct {
	shouldAlarm    bool
	shouldTelAlarm bool
	zoneAlarmInfo  map[string]*AlarmInfo
}

func (s *ChubaoFSMonitor) doCheckZoneDiskUsedRatio(csv *cfs.ClusterStatInfoView, usedRatioMin, usedRatioMax float64, host *ClusterHost) (collectAlarmInfo *PackAlarmInfo) {
	collectAlarmInfo = &PackAlarmInfo{
		zoneAlarmInfo: make(map[string]*AlarmInfo),
	}
	for zoneName, zoneStat := range csv.ZoneStatInfo {
		//如果是SSD zone 低于阈值直接忽略
		if host.isSSDZone(zoneName) && zoneStat.DataNodeStat.UsedRatio < usedRatioMinThresholdSSD && zoneStat.MetaNodeStat.UsedRatio < usedRatioMinThresholdSSD {
			log.LogDebug(fmt.Sprintf("ssd zone:%v dataRatio:%v,metaRatio:%v", zoneName, zoneStat.DataNodeStat.UsedRatio, zoneStat.MetaNodeStat.UsedRatio))
			continue
		}
		alarmInfo := &AlarmInfo{}
		zoneAlarmInfo := fmt.Sprintf("zone[%v]:", zoneName)
		if zoneStat.DataNodeStat.UsedRatio > usedRatioMin {
			collectAlarmInfo.shouldAlarm = true
			alarmInfo.shouldAlarm = true
			zoneAlarmInfo += fmt.Sprintf(" datanode usedRatio is now %v,", zoneStat.DataNodeStat.UsedRatio)
		}
		if zoneStat.MetaNodeStat.UsedRatio > usedRatioMin {
			collectAlarmInfo.shouldAlarm = true
			alarmInfo.shouldAlarm = true
			zoneAlarmInfo += fmt.Sprintf(" metanode usedRatio is now %v,\n", zoneStat.MetaNodeStat.UsedRatio)
		}
		if zoneStat.DataNodeStat.UsedRatio >= usedRatioMax || zoneStat.MetaNodeStat.UsedRatio >= usedRatioMax {
			collectAlarmInfo.shouldTelAlarm = true
		}
		alarmInfo.alarmMsg = zoneAlarmInfo
		collectAlarmInfo.zoneAlarmInfo[zoneName] = alarmInfo
	}
	return
}

func (ch *ClusterHost) isSSDZone(zoneName string) (ok bool) {
	//elasticdb 集群 包含hdd的是hdd,其它为ssd, 即不包含hdd则为ssd
	//spark集群 包含ssd的是ssd，其它为hdd
	switch ch.host {
	case "cn.elasticdb.jd.local":
		return !strings.Contains(zoneName, "hdd")
	case "cn.chubaofs.jd.local":
		return strings.Contains(zoneName, "ssd")
	default:
		return false
	}
}
