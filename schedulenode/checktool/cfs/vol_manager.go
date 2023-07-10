package cfs

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/schedulenode/common/cfs"
	"github.com/cubefs/cubefs/util/checktool"
	"github.com/cubefs/cubefs/util/log"
	"strconv"
	"strings"
	"time"
)

func (s *ChubaoFSMonitor) checkAvailSpaceAndVolsStatus() {
	for _, host := range s.hosts {
		checkVolWg.Add(1)
		go func(host *ClusterHost) {
			defer checkVolWg.Done()
			log.LogInfof("checkAvailSpaceAndVolsStatus [%v] begin", host)
			startTime := time.Now()
			s.CheckVolHealth(host)
			log.LogInfof("checkAvailSpaceAndVolsStatus [%v] end,cost[%v]", host, time.Since(startTime))
		}(host)
	}
	checkVolWg.Wait()
}

func (c *ChubaoFSMonitor) doProcessMetrics(item, umpKey, msg string) {
	metric, ok := c.metrics[item]
	if !ok {
		c.metrics[item] = &AlarmMetric{name: item, lastAlarmTime: time.Now()}
		return
	}
	if time.Since(metric.lastAlarmTime) < time.Hour {
		return
	} else {
		metric.lastAlarmTime = time.Now()
	}
	checktool.WarnBySpecialUmpKey(umpKey, msg)
}

func (s *ChubaoFSMonitor) CheckVolHealth(ch *ClusterHost) {
	ch.updateInactiveNodes()
	volStats, leaderAddr, err := getAllVolStat(ch)
	if err != nil {
		log.LogErrorf("action[CheckVolHealth] occurred error,err:%v", err)
		return
	}
	if ch.LeaderAddr != leaderAddr {
		log.LogErrorf("action[CheckVolHealth] leader changed LeaderAddr:%v newLeaderAddr:%v", ch.LeaderAddr, leaderAddr)
		ch.LeaderAddr = leaderAddr
		s.volNeedAllocateDPContinuedTimes = make(map[string]int)
		ch.LeaderAddrUpdateTime = time.Now()
		return
	}
	if time.Since(ch.LeaderAddrUpdateTime) < time.Minute*5 {
		log.LogErrorf("action[CheckVolHealth] LeaderAddrUpdateTime:%v less than 5min", ch.LeaderAddrUpdateTime)
		return
	}
	for _, vss := range volStats {
		//fmt.Printf("vol[%v],capacity:%v,used:%v,ratio:%v\n", volName, vss.TotalGB, vss.UsedGB, vss.UsedRatio)
		useRatio, err := strconv.ParseFloat(vss.UsedRatio, 64)
		if err != nil {
			log.LogErrorf("check vol[%v] failed,err[%v]\n", vss.Name, err)
			return
		}

		vol, err := getVolSimpleView(vss.Name, ch)
		if err != nil {
			log.LogErrorf("check vol[%v] failed,err[%v]\n", vss.Name, err)
			return
		}
		if vol.Status == 1 {
			hostVolKey := fmt.Sprintf("%s#%s", ch.host, vol.Name)
			lastWarnTime, ok := s.markDeleteVols[hostVolKey]
			if !ok || time.Now().Sub(lastWarnTime) > 24*time.Hour {
				s.markDeleteVols[hostVolKey] = time.Now()
				// 第一次检测到，进行普通告警通知, 每隔24小时 如果还是这个状态 就告警
				warnMsg := fmt.Sprintf("host[%v],vol[%v] Status[%v] has been markDelete", ch.host, vol.Name, vol.Status)
				checktool.WarnBySpecialUmpKey(UMPCFSNormalWarnKey, warnMsg)
			}
			continue
		}
		availableSpaceRatio := float64(vol.AvailSpaceAllocated) / float64(vol.Capacity)
		//if useRatio > s.usedRatio {
		//	msg := ch.host + fmt.Sprintf(" vol[%v],useRatio larger than the configured threshold [%v],RwCnt[%v],DpCnt[%v],capGB:[%v],useRatio[%v],usedGB[%v],AvailSpaceAllocatedGB[%v]\n",
		//		vol.Name, s.usedRatio, vol.RwDpCnt, vol.DpCnt, vol.Capacity, useRatio, vss.UsedGB, vol.AvailSpaceAllocated)
		//	s.doProcessMetrics("useRatio", UMPCFSNormalWarnKey, msg)
		//}
		//fmt.Printf("vol[%v],cap[%v],AvailSpaceAllocated[%v],RwCnt[%v],DpCnt[%v]\n", volName, vol.Capacity, vol.AvailSpaceAllocated, vol.RwDpCnt, vol.DpCnt)
		var canCreateNewDP bool
		if vol.RwDpCnt < int(s.minReadWriteCount) || float64(vol.RwDpCnt)/float64(vol.DpCnt) < s.readWriteDpRatio ||
			ch.isReleaseCluster && availableSpaceRatio < s.availSpaceRatio {
			canCreateNewDP = s.canCreateNewDP(ch.host, vol.Name)
		}
		if canCreateNewDP && vol.RwDpCnt < int(s.minReadWriteCount) {
			msg := ch.host + fmt.Sprintf(" vol[%v],RwCnt less than the configured threshold  [%v],RwCnt[%v],DpCnt[%v],capGB:[%v],useRatio[%v],usedGB[%v],AvailSpaceAllocatedGB[%v]\n",
				vol.Name, s.minReadWriteCount, vol.RwDpCnt, vol.DpCnt, vol.Capacity, useRatio, vss.UsedGB, vol.AvailSpaceAllocated)
			_, err = doRequest(generateAllocateDataPartitionURL(ch.host, vss.Name, 10), ch.isReleaseCluster)
			if err != nil {
				log.LogErrorf("%v,err:%v", msg, err)
			}
			log.LogWarnf(msg)
		}

		if canCreateNewDP && float64(vol.RwDpCnt)/float64(vol.DpCnt) < s.readWriteDpRatio {
			msg := ch.host + fmt.Sprintf(" vol[%v],readWriteDpRatio less than the configured threshold  [%v],RwCnt[%v],DpCnt[%v],capGB:[%v],useRatio[%v],usedGB[%v],AvailSpaceAllocatedGB[%v]\n",
				vol.Name, s.readWriteDpRatio, vol.RwDpCnt, vol.DpCnt, vol.Capacity, useRatio, vss.UsedGB, vol.AvailSpaceAllocated)
			count := float64(vol.DpCnt) * s.readWriteDpRatio
			if count > 100 {
				count = 100
				checktool.WarnBySpecialUmpKey(UMPCFSNormalWarnKey, msg)
			}
			_, err = doRequest(generateAllocateDataPartitionURL(ch.host, vss.Name, int(count)), ch.isReleaseCluster)
			if err != nil {
				log.LogErrorf("%v,err:%v", msg, err)
			}
			log.LogWarnf(msg)
		}
		if ch.host == "id.chubaofs-seqwrite.jd.local" {
			continue
		}

		if canCreateNewDP && ch.isReleaseCluster && availableSpaceRatio < s.availSpaceRatio {
			if vss.UsedGB < vol.AvailSpaceAllocated {
				continue
			}
			if vol.Capacity > 100*1024 && vol.AvailSpaceAllocated > 100*1024 {
				continue
			}
			msg := fmt.Sprintf("vol[%v],availableSpaceRatio less than the configured threshold [%v],RwCnt[%v],DpCnt[%v],capGB:[%v],useRatio[%v],usedGB[%v],AvailSpaceAllocatedGB[%v] ",
				vol.Name, s.availSpaceRatio, vol.RwDpCnt, vol.DpCnt, vol.Capacity, useRatio, vss.UsedGB, vol.AvailSpaceAllocated)
			leftSpace := vol.Capacity - vol.AvailSpaceAllocated - vss.UsedGB
			count := leftSpace / 120
			if count > 1000 {
				count = 1000
			}
			msg = ch.host + " " + msg + " " + generateAllocateDataPartitionURL(ch.host, vss.Name, int(count))
			_, err = doRequest(generateAllocateDataPartitionURL(ch.host, vss.Name, int(count)), ch.isReleaseCluster)
			if err != nil {
				log.LogErrorf("%v,err:%v", msg, err)
			}
			log.LogWarnf(msg)
			//util.WarnBySpecialUmpKey(UMPCFSNormalWarnKey, msg)
		}

		go func(name string, host *ClusterHost) {
			if s.ignoreCheckMp {
				return
			}
			checkMetaPartitions(name, ch)
		}(vss.Name, ch)
	}
}

func (s *ChubaoFSMonitor) canCreateNewDP(host, volName string) (can bool) {
	key := fmt.Sprintf("%v;%v", host, volName)
	s.volNeedAllocateDPContinuedTimes[key]++
	if s.volNeedAllocateDPContinuedTimes[key] >= 2 {
		can = true
		delete(s.volNeedAllocateDPContinuedTimes, key)
	}
	return
}

func getAllVolStat(ch *ClusterHost) (vols map[string]*VolSpaceStat, leaderAddr string, err error) {
	cv, err := getCluster(ch)
	if err != nil {
		return
	}
	leaderAddr = cv.LeaderAddr
	vols = make(map[string]*VolSpaceStat, 0)
	for _, vss := range cv.VolStat {
		vols[vss.Name] = vss
	}
	return
}

func isReleaseCluster(host string) bool {
	return strings.Contains(host, keyWordReleaseCluster)
}

func generateAllocateDataPartitionURL(host, volName string, count int) string {
	return fmt.Sprintf("http://%v/dataPartition/create?name=%v&count=%v&type=extent", host, volName, count)
}

func checkMetaPartitions(volName string, ch *ClusterHost) {
	var err error
	defer func() {
		if err != nil {
			log.LogErrorf("action[checkMetaPartitions] host[%v] vol[%v]", ch.host, volName)
		}
	}()
	if volName == "offline_logs" && ch.host == "cn.chubaofs.jd.local" {
		return
	}
	reqURL := fmt.Sprintf("http://%v/client/metaPartitions?name=%v", ch.host, volName)
	data, err := doRequest(reqURL, ch.isReleaseCluster)
	if err != nil {
		return
	}
	mpvs := make([]*MetaPartitionView, 0)
	if err = json.Unmarshal(data, &mpvs); err != nil {
		return
	}
	for _, mp := range mpvs {
		if ch.host == "cn.chubaofs.jd.local" && mp.PhyPid != mp.PartitionID {
			continue
		}
		checkMetaPartition(ch, volName, mp.PartitionID)
	}
	return
}

func checkMetaPartition(ch *ClusterHost, volName string, id uint64) {

	var (
		reqURL string
		err    error
	)
	defer func() {
		if err != nil {
			log.LogErrorf("action[checkMetaPartition] host[%v],vol[%v],id[%v],err[%v]", ch.host, volName, id, err)
		}
	}()
	if volName == "mysql-backup" && id == 2007 {
		return
	}
	if ch.host == strings.TrimSpace("id.chubaofs-seqwrite.jd.local") {
		reqURL = fmt.Sprintf("http://%v/client/metaPartition?name=%v&id=%v", ch.host, volName, id)
	} else {
		reqURL = fmt.Sprintf("http://%v/metaPartition/get?name=%v&id=%v", ch.host, volName, id)
	}
	data, err := doRequest(reqURL, ch.isReleaseCluster)
	if err != nil {
		return
	}
	mp := &MetaPartition{}
	if err = json.Unmarshal(data, mp); err != nil {
		log.LogErrorf("unmarshal to mp data[%v],err[%v]", string(data), err)
		return
	}
	if len(mp.Replicas) < int(mp.ReplicaNum+mp.LearnerNum) {
		warnMsg := fmt.Sprintf("host[%v],vol[%v],meta partition[%v],miss replica,json[%v]", ch.host, volName, mp.PartitionID, string(data))
		//缺少副本都是已知离线节点的 不再告警
		if ch.missReplicaIsInactiveNodes(mp) {
			log.LogWarn(warnMsg)
		} else {
			checktool.WarnBySpecialUmpKey(UMPCFSNormalWarnKey, warnMsg)
		}
	}
	noLeader := true
	for _, replica := range mp.Replicas {
		if replica.IsLeader {
			noLeader = false
		}
	}
	if noLeader {
		log.LogWarnf("host[%v],vol[%v],meta partition[%v],no leader,json[%v]", ch.host, volName, mp.PartitionID, string(data))
		key := fmt.Sprintf("%v_%v", ch.host, mp.PartitionID)
		noLeaderMp, ok := noLeaderMps.Load(key)
		if !ok {
			noLeaderMps.Store(key, newNoLeaderMetaPartition(mp))
		} else {
			tmpMp, ok := noLeaderMp.(*NoLeaderMetaPartition)
			if !ok {
				return
			}
			if hasLeaderOnMetanode(mp, ch.isReleaseCluster, ch.host) {
				return
			}
			tmpMp.Count = tmpMp.Count + 1
			inOneCycle := time.Now().Unix()-tmpMp.LastCheckedTime < defaultMpNoLeaderWarnInternal
			if inOneCycle && tmpMp.Count >= defaultMpNoLeaderMinCount {
				warnMsg := fmt.Sprintf("host[%v],vol[%v],meta partition[%v],no leader,json[%v]", ch.host, volName, mp.PartitionID, string(data))
				checktool.WarnBySpecialUmpKey(UMPKeyMetaPartitionNoLeader, warnMsg)
				tmpMp.Count = 0
				tmpMp.LastCheckedTime = time.Now().Unix()
			}
			if !inOneCycle {
				tmpMp.Count = 0
				tmpMp.LastCheckedTime = time.Now().Unix()
			}
		}
	}
	return
}

func hasLeaderOnMetanode(mp *MetaPartition, isReleaseCluster bool, host string) bool {
	for _, mr := range mp.Replicas {
		addrs := strings.Split(mr.Addr, ":")
		if len(addrs) != 2 {
			continue
		}
		if addrs[1] == "9021" {
			addrs[1] = "9092"
		} else {
			addrs[1] = "17220"
		}
		data, err := cfs.DoRequestToMn(isReleaseCluster, fmt.Sprintf("http://%v:%v/getPartitionById?pid=%v", addrs[0], addrs[1], mp.PartitionID))
		if err != nil {
			log.LogErrorf("DoRequestToMn occurred err :%v", err)
			continue
		}
		mpOnMn := &cfs.MpInfoOnMn{}
		err = json.Unmarshal(data, mpOnMn)
		if err != nil {
			log.LogErrorf("Unmarshal mp for metanode occurred err :%v", err)
			continue
		}
		log.LogWarnf("host[%v] meta partition[%v] info on metanode %v,%v,%v", host, mp.PartitionID, mpOnMn.LeaderAddr, mpOnMn.Cursor, mpOnMn.NodeId)
		if mpOnMn.LeaderAddr != "" {
			return true
		}
	}
	return false
}

func getVolSimpleView(volName string, ch *ClusterHost) (vsv *SimpleVolView, err error) {
	reqURL := fmt.Sprintf("http://%v/admin/getVol?name=%v", ch.host, volName)
	data, err := doRequest(reqURL, ch.isReleaseCluster)
	if err != nil {
		return
	}
	vsv = &SimpleVolView{}
	if err = json.Unmarshal(data, vsv); err != nil {
		return
	}
	if len(vsv.Name) == 0 {
		err = fmt.Errorf("vol:%v maybe not exist", volName)
		return
	}
	return
}

func (ch *ClusterHost) updateInactiveNodes() {
	ch.inactiveNodesForCheckVolLock.Lock()
	defer ch.inactiveNodesForCheckVolLock.Unlock()

	ch.inactiveNodesForCheckVol = make(map[string]bool)
	clusterView, err := getCluster(ch)
	if err != nil {
		return
	}
	for _, metaNode := range clusterView.MetaNodes {
		if metaNode.Status == false {
			ch.inactiveNodesForCheckVol[metaNode.Addr] = true
		}
	}
}

func (ch *ClusterHost) missReplicaIsInactiveNodes(mp *MetaPartition) bool {
	ch.inactiveNodesForCheckVolLock.RLock()
	defer ch.inactiveNodesForCheckVolLock.RUnlock()

	missHosts := mp.getMissReplicas()
	if len(missHosts) == 0 {
		return false
	}
	for _, host := range missHosts {
		if !ch.inactiveNodesForCheckVol[host] {
			return false
		}
	}
	return true
}
