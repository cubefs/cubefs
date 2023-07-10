package cfs

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/util/checktool"
	"github.com/cubefs/cubefs/util/log"

	"sync"
	"time"
)

const (
	writeAbilityRate = 0.50
)

func (s *ChubaoFSMonitor) scheduleToCheckZoneMnDnWriteAbilityRate() {
	s.checkZoneMnDnWriteAbilityRate()
	for {
		t := time.NewTimer(time.Duration(s.scheduleInterval) * time.Second)
		select {
		case <-t.C:
			s.checkZoneMnDnWriteAbilityRate()
		}
	}
}

func (s *ChubaoFSMonitor) checkZoneMnDnWriteAbilityRate() {
	var wg sync.WaitGroup
	cluserAlarmInfo := make(map[string]*hostLowWriteAbility)
	cluserAlarmInfoLock := new(sync.RWMutex)
	for _, host := range s.hosts {
		wg.Add(1)
		go func(host *ClusterHost) {
			defer checktool.HandleCrash()
			defer wg.Done()
			if host.isReleaseCluster {
				// dbbak master /topo/get does not exist
				return
			}
			log.LogDebugf("checkZoneMnDnWriteAbilityRate [%v] begin", host)
			startTime := time.Now()
			topologyView, err := GetTopology(host.host, host.isReleaseCluster)
			if err != nil {
				_, ok := err.(*json.SyntaxError)
				if ok {
					return
				}
				log.LogErrorf("get topology view from %v failed, err:%v", host.host, err)
				return
			}
			lowWriteAbilityMns, lowWriteAbilityDns := analyzeData(topologyView)
			if len(lowWriteAbilityMns) == 0 && len(lowWriteAbilityDns) == 0 {
				return
			}
			cluserAlarmInfoLock.Lock()
			lowMnDns := &hostLowWriteAbility{
				lowWriteAbilityMns,
				lowWriteAbilityDns,
			}
			cluserAlarmInfo[host.host] = lowMnDns
			cluserAlarmInfoLock.Unlock()
			log.LogDebugf("checkZoneMnDnWriteAbilityRate [%v] end,cost[%v]", host, time.Since(startTime))
		}(host)
	}
	wg.Wait()
	var alarmMsg string
	for host, alarmInfo := range cluserAlarmInfo {
		for _, lowMn := range alarmInfo.mns {
			alarmMsg += fmt.Sprintf("host[%v] zone[%v]: The writeability rate of metanode is now %v, lower than %v\n",
				host, lowMn.zoneName, fmt.Sprintf("%.2f", lowMn.metaNodeWriteAbilityRate), writeAbilityRate)
		}
		for _, lowDn := range alarmInfo.dns {
			alarmMsg += fmt.Sprintf("host[%v] zone[%v]: The writeability rate of datanode is now %v, lower than %v\n", host, lowDn.zoneName, fmt.Sprintf("%.2f", lowDn.dataNodeWriteAbilityRate), writeAbilityRate)
		}
	}
	if len(alarmMsg) > 0 {
		checktool.WarnBySpecialUmpKey(UMPCFSZoneWriteAbilityWarnKey, alarmMsg)
	}
}

type hostLowWriteAbility struct {
	mns []LowWriteAbilityRateMetaNode
	dns []LowWriteAbilityRateDataNode
}

type LowWriteAbilityRateMetaNode struct {
	zoneName                            string
	metaNodeLen, metaNodeIsWriteAbleLen int
	metaNodeWriteAbilityRate            float64
}

type LowWriteAbilityRateDataNode struct {
	zoneName                            string
	dataNodeLen, dataNodeIsWriteAbleLen int
	dataNodeWriteAbilityRate            float64
}

type zoneMnDnIsWriteAbleLen struct {
	metaNodeLen, dataNodeLen, metaNodeIsWriteAbleLen, dataNodeIsWriteAbleLen int
	metaNodeWriteAbilityRate, dataNodeWriteAbilityRate                       float64
}

func analyzeData(tv *TopologyView) (lowWriteAbilityMetaNodes []LowWriteAbilityRateMetaNode, lowWriteAbilityDataNodes []LowWriteAbilityRateDataNode) {
	for _, zone := range tv.Zones {
		zwl := analyzeNodeSet(zone.NodeSet)
		if zwl.metaNodeWriteAbilityRate < writeAbilityRate {
			lowWriteAbilityMetaNodes = append(lowWriteAbilityMetaNodes, LowWriteAbilityRateMetaNode{
				zoneName:                 zone.Name,
				metaNodeLen:              zwl.metaNodeLen,
				metaNodeIsWriteAbleLen:   zwl.metaNodeIsWriteAbleLen,
				metaNodeWriteAbilityRate: zwl.metaNodeWriteAbilityRate,
			})
		}
		if zwl.dataNodeWriteAbilityRate < writeAbilityRate {
			lowWriteAbilityDataNodes = append(lowWriteAbilityDataNodes, LowWriteAbilityRateDataNode{
				zoneName:                 zone.Name,
				dataNodeLen:              zwl.dataNodeLen,
				dataNodeIsWriteAbleLen:   zwl.dataNodeIsWriteAbleLen,
				dataNodeWriteAbilityRate: zwl.dataNodeWriteAbilityRate,
			})
		}
	}
	return
}

func analyzeNodeSet(nsv map[uint64]*nodeSetView) (zwl zoneMnDnIsWriteAbleLen) {
	var metaNodeLen, dataNodeLen, metaNodeIsWriteAbleLen, dataNodeIsWriteAbleLen int
	for _, ns := range nsv {
		metaNodeLen += ns.MetaNodeLen
		dataNodeLen += ns.DataNodeLen
		for _, mn := range ns.MetaNodes {
			if mn.IsWritable {
				metaNodeIsWriteAbleLen++
			}
		}
		for _, dn := range ns.DataNodes {
			if dn.IsWritable {
				dataNodeIsWriteAbleLen++
			}
		}
	}
	zwl = zoneMnDnIsWriteAbleLen{
		metaNodeLen,
		dataNodeLen,
		metaNodeIsWriteAbleLen,
		dataNodeIsWriteAbleLen,
		float64(metaNodeIsWriteAbleLen) / float64(metaNodeLen),
		float64(dataNodeIsWriteAbleLen) / float64(dataNodeLen),
	}
	return
}

func GetTopology(host string, isReleaseCluster bool) (tv *TopologyView, err error) {
	reqURL := fmt.Sprintf("http://%v/topo/get", host)
	data, err := doRequest(reqURL, isReleaseCluster)
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
