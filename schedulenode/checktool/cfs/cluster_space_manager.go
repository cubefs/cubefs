package cfs

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/util/checktool"
	"github.com/cubefs/cubefs/util/log"
	"strconv"
	"sync"
	"time"
)

const (
	nlCluster = "nl.chubaofs.ochama.com"
)

func (s *ChubaoFSMonitor) scheduleToCheckClusterUsedRatio() {
	s.checkClusterUsedRatio()
	for {
		t := time.NewTimer(time.Duration(s.scheduleInterval) * time.Second)
		select {
		case <-s.ctx.Done():
			return
		case <-t.C:
			s.checkClusterUsedRatio()
		}
	}
}

func (s *ChubaoFSMonitor) checkClusterUsedRatio() {
	var wg sync.WaitGroup
	for _, host := range s.hosts {
		wg.Add(1)
		go func(host *ClusterHost) {
			defer func() {
				wg.Done()
				checktool.HandleCrash()
			}()
			log.LogInfof("checkClusterUsedRatio [%v] begin", host)
			startTime := time.Now()
			cv, err := getCluster(host)
			if err != nil {
				_, ok := err.(*json.SyntaxError)
				if ok {
					return
				}
				msg := fmt.Sprintf("get cluster info from %v failed,err:%v", host.host, err)
				checktool.WarnBySpecialUmpKey(UMPCFSNormalWarnKey, msg)
				return
			}
			s.doCheckDataNodeDiskUsedRatio(cv, host)
			s.doCheckMetaNodeDiskUsedRatio(cv, host)
			log.LogInfof("checkClusterUsedRatio [%v] end,cost[%v]", host, time.Since(startTime))
		}(host)
	}
	wg.Wait()
}

func (s *ChubaoFSMonitor) doCheckDataNodeDiskUsedRatio(cv *ClusterView, host *ClusterHost) {
	var clusterUsedRatio string
	if host.isReleaseCluster {
		clusterUsedRatio = cv.DataNodeStat.UsedRatio
	} else {
		clusterUsedRatio = cv.DataNodeStatInfo.UsedRatio
	}

	usedRatio, err := strconv.ParseFloat(clusterUsedRatio, 10)
	if err != nil {
		log.LogErrorf("action[doCheckDataNodeDiskUsedRatio] parse %v to float64 failed,err %v",
			clusterUsedRatio, err)
		return
	}

	if host.host == nlCluster {
		log.LogInfof("space check host:%v ClusterUsedRatio:%v", host.host, s.nlClusterUsedRatio)
		if usedRatio > s.nlClusterUsedRatio {
			msg := fmt.Sprintf("cluster[%v] dataNode used ratio [%v] larger than [%v]",
				host.host, usedRatio, s.nlClusterUsedRatio)
			host.doProcessClusterUsedRatioAlarm(msg)
		}
		return
	}

	log.LogInfof("space check host:%v ClusterUsedRatio:%v", host.host, s.clusterUsedRatio)
	if usedRatio > s.clusterUsedRatio {
		msg := fmt.Sprintf("cluster[%v] dataNode used ratio [%v] larger than [%v]",
			host.host, usedRatio, s.clusterUsedRatio)
		host.doProcessClusterUsedRatioAlarm(msg)
	}
}

func (s *ChubaoFSMonitor) doCheckMetaNodeDiskUsedRatio(cv *ClusterView, host *ClusterHost) {
	var clusterUsedRatio string
	if host.isReleaseCluster {
		clusterUsedRatio = cv.MetaNodeStat.UsedRatio
	} else {
		clusterUsedRatio = cv.MetaNodeStatInfo.UsedRatio
	}

	usedRatio, err := strconv.ParseFloat(clusterUsedRatio, 10)
	if err != nil {
		log.LogErrorf("action[doCheckMetaNodeDiskUsedRatio] parse %v to float64 failed,err %v",
			clusterUsedRatio, err)
		return
	}

	if host.host == nlCluster {
		if usedRatio > s.nlClusterUsedRatio {
			msg := fmt.Sprintf("cluster[%v] dataNode used ratio [%v] larger than [%v]",
				host.host, usedRatio, s.nlClusterUsedRatio)
			host.doProcessClusterUsedRatioAlarm(msg)
		}
		return
	}

	if usedRatio > s.clusterUsedRatio {
		msg := fmt.Sprintf("cluster[%v] meta node used ratio [%v] larger than [%v]",
			host.host, usedRatio, s.clusterUsedRatio)
		host.doProcessClusterUsedRatioAlarm(msg)
	}
}

func (ch *ClusterHost) doProcessClusterUsedRatioAlarm(msg string) {

	if ch.lastTimeAlarmClusterUsedRatio.IsZero() {
		ch.lastTimeAlarmClusterUsedRatio = time.Now()
		checktool.WarnBySpecialUmpKey(UMPCFSClusterUsedRatio, msg)
		return
	}
	inOneCycle := time.Since(ch.lastTimeAlarmClusterUsedRatio) < time.Hour
	if inOneCycle {
		return
	}
	ch.lastTimeAlarmClusterUsedRatio = time.Now()
	checktool.WarnBySpecialUmpKey(UMPCFSClusterUsedRatio, msg)
	return
}
