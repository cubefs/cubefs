package cfs

import (
	"fmt"
	"github.com/cubefs/cubefs/util/checktool"
	"github.com/cubefs/cubefs/util/log"
	"time"
)

func (s *ChubaoFSMonitor) scheduleToCheckIDMetaNodeDiskStat() {
	ticker := time.NewTicker(time.Minute * 5)
	defer func() {
		ticker.Stop()
	}()
	idHosts := []string{"id.chubaofs.jd.local", "idbbak.chubaofs.jd.local"}
	s.idHosts = make([]*ClusterHost, 0)
	for _, host := range idHosts {
		s.idHosts = append(s.idHosts, newClusterHost(host))
	}

	s.checkIDMetaNodeDiskStat()
	for {
		select {
		case <-ticker.C:
			s.checkIDMetaNodeDiskStat()
		}
	}
}

func (s *ChubaoFSMonitor) checkIDMetaNodeDiskStat() {
	for _, host := range s.idHosts {
		log.LogInfof("checkIDMetaNodeDiskStat [%v] begin", host)
		startTime := time.Now()
		cv, err := getCluster(host)
		if err != nil {
			if isConnectionRefusedFailure(err) {
				msg := fmt.Sprintf("checkIDMetaNodeDiskStat get cluster info from %v failed, err:%v ", host.host, err)
				checktool.WarnBySpecialUmpKey(UMPCFSClusterConnRefused, msg)
				return
			}
			return
		}
		cv.checkMetaNodeDiskStat(host, GB*5)
		log.LogInfof("checkIDMetaNodeDiskStat [%v] end,cost[%v]", host, time.Since(startTime))
	}
}
