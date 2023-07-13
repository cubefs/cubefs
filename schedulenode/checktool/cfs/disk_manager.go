package cfs

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/schedulenode/common/xbp"
	"github.com/cubefs/cubefs/util/checktool"
	"github.com/cubefs/cubefs/util/log"
	"sync"
	"time"
)

func (s *ChubaoFSMonitor) scheduleToCheckDiskError() {
	s.checkDiskError()
	for {
		t := time.NewTimer(time.Duration(s.scheduleInterval) * time.Second)
		select {
		case <-s.ctx.Done():
			return
		case <-t.C:
			s.checkDiskError()
		}
	}
}

func (s *ChubaoFSMonitor) checkDiskError() {
	var wg sync.WaitGroup
	for _, host := range s.hosts {
		wg.Add(1)
		go func(host *ClusterHost) {
			defer checktool.HandleCrash()
			defer wg.Done()
			log.LogDebugf("checkDiskError [%v] begin", host)
			startTime := time.Now()
			cv, err := getClusterDataNodeBadDisks(host)
			if err != nil {
				_, ok := err.(*json.SyntaxError)
				if ok {
					return
				}
				msg := fmt.Sprintf("get cluster info from %v failed,err:%v", host.host, err)
				checktool.WarnBySpecialUmpKey(UMPCFSNormalWarnKey, msg)
				return
			}
			s.doCheckDataNodeDiskError(cv, host)
			log.LogDebugf("checkDiskError [%v] end,cost[%v]", host, time.Since(startTime))
		}(host)
	}
	wg.Wait()
}

func (s *ChubaoFSMonitor) doCheckDataNodeDiskError(cv *ClusterDataNodeBadDisks, host *ClusterHost) {
	var (
		url string
	)
	newCheckedDataNodeBadDisk := make(map[string]time.Time, 0)
	for _, badDiskOnNode := range cv.DataNodeBadDisks {
		for _, badDisk := range badDiskOnNode.BadDiskPath {
			dataNodeBadDiskKey := fmt.Sprintf("%s#%s", badDiskOnNode.Addr, badDisk)
			newCheckedDataNodeBadDisk[dataNodeBadDiskKey] = time.Now()
			if firstReportTime, ok := host.dataNodeBadDisk[dataNodeBadDiskKey]; ok {
				newCheckedDataNodeBadDisk[dataNodeBadDiskKey] = firstReportTime
				// 清理超过24小时的记录
				for key, t := range host.offlineDisksIn24Hour {
					if time.Since(t) > 24*time.Hour {
						delete(host.offlineDisksIn24Hour, key)
					}
				}
				if time.Since(firstReportTime) > s.offlineDiskMinDuration && len(host.offlineDisksIn24Hour) < s.offlineDiskMaxCountIn24Hour {
					log.LogDebugf("action[doCheckDataNodeDiskError] host[%s] Addr[%s] badDisk[%s]", host, badDiskOnNode.Addr, badDisk)
					// 控制单块盘的下线间隔时间
					lastOfflineThisDiskTime := host.offlineDisksIn24Hour[dataNodeBadDiskKey]
					if time.Since(lastOfflineThisDiskTime) > time.Minute*30 {
						offlineDataNodeDisk(host, badDiskOnNode.Addr, badDisk, true)
						host.offlineDisksIn24Hour[dataNodeBadDiskKey] = time.Now()
					}
				}
			}
			//24小时内自动下线的就不用发xbp, 超过24小时内自动下线阈值才发起XBP单子
			if len(host.offlineDisksIn24Hour) < s.offlineDiskMaxCountIn24Hour {
				continue
			}
			if host.isReleaseCluster {
				url = fmt.Sprintf("http://%v/disk/offline?addr=%v&disk=%v&auto=true", host.host, badDiskOnNode.Addr, badDisk)
			} else {
				url = fmt.Sprintf("http://%v/disk/decommission?addr=%v&disk=%v&auto=true", host.host, badDiskOnNode.Addr, badDisk)
			}

			badDiskXBPTicketKey := fmt.Sprintf("%s#%s", badDiskOnNode.Addr, badDisk)
			value, ok := s.badDiskXBPTickets.Load(badDiskXBPTicketKey)
			if !ok {
				newTicketInfo, err := CreateOfflineXBPTicket(cv.Name, badDiskOnNode.Addr, fmt.Sprintf("datanode disk err %s", badDisk), url, host.isReleaseCluster)
				if err != nil {
					log.LogErrorf("action[doCheckDataNodeDiskError] err:%v", err)
					continue
				}
				newTicketInfo.ticketType = XBPTicketTypeNodeDisk
				s.badDiskXBPTickets.Store(badDiskXBPTicketKey, newTicketInfo)
			} else {
				ticketInfo, ok := value.(XBPTicketInfo)
				if !ok {
					continue
				}
				// 订单号为0 或者单子已经处理（驳回/完结），但是超过一定时间还有告警
				if ticketInfo.tickerID == 0 || (ticketInfo.status == xbp.TicketStatusReject && time.Now().Sub(ticketInfo.lastUpdateTime) > 5*time.Hour) ||
					(ticketInfo.status == xbp.TicketStatusFinish && time.Now().Sub(ticketInfo.lastUpdateTime) > 1*time.Hour) {
					newTicketInfo, err := CreateOfflineXBPTicket(cv.Name, badDiskOnNode.Addr, fmt.Sprintf("datanode disk err %s", badDisk), url, host.isReleaseCluster)
					if err != nil {
						log.LogErrorf("action[doCheckDataNodeDiskError] err:%v", err)
						continue
					}
					newTicketInfo.ticketType = XBPTicketTypeNodeDisk
					s.badDiskXBPTickets.Store(badDiskXBPTicketKey, newTicketInfo)
				}
			}
		}
	}
	host.dataNodeBadDisk = newCheckedDataNodeBadDisk
}

func CreateOfflineXBPTicket(clusterID, nodeAddr, detailMsg, url string, isReleaseCluster bool) (ticketInfo XBPTicketInfo, err error) {
	m := map[string]string{
		"集群ID":  clusterID,
		"节点信息":  nodeAddr,
		"故障类型":  detailMsg,
		"执行URL": url}
	ticketId, err := xbp.CreateTicket(xbp.OfflineTicketProcessId, xbp.Domain, "yangqingyuan8", xbp.Sign, xbp.Erp, m)
	if err != nil {
		err = fmt.Errorf("%v:%v add xbp ticket failed, err:%v", nodeAddr, detailMsg, err)
		return
	}
	ticketInfo = XBPTicketInfo{
		tickerID:         ticketId,
		url:              url,
		nodeAddr:         nodeAddr,
		isReleaseCluster: isReleaseCluster,
		lastUpdateTime:   time.Now(),
	}
	return
}

type ClusterDataNodeBadDisks struct {
	Name             string
	DataNodeBadDisks []DataNodeBadDisksView
}

type ReleaseClusterDataNodeBadDisks struct {
	Name             string
	DataNodeBadDisks map[string][]string
}

func getClusterDataNodeBadDisks(host *ClusterHost) (cv *ClusterDataNodeBadDisks, err error) {
	reqURL := fmt.Sprintf("http://%v/admin/getCluster", host)
	data, err := doRequest(reqURL, host.isReleaseCluster)
	if err != nil {
		return
	}
	cv = &ClusterDataNodeBadDisks{}
	err = json.Unmarshal(data, cv)
	if err != nil {
		if host.isReleaseCluster {
			rcv := &ReleaseClusterDataNodeBadDisks{}
			err = json.Unmarshal(data, rcv)
			if err == nil {
				for addr, badDiskPath := range rcv.DataNodeBadDisks {
					cv.DataNodeBadDisks = append(cv.DataNodeBadDisks, DataNodeBadDisksView{
						Addr:        addr,
						BadDiskPath: badDiskPath,
					})
				}
				return
			}
		}
		log.LogErrorf("get cluster from %v failed ,data:%v,err:%v", host, string(data), err)
		return
	}
	return
}

func doOfflineDataNodeDisk(host *ClusterHost, addr, badDisk string, force bool) {
	var reqURL string
	if host.isReleaseCluster {
		reqURL = fmt.Sprintf("http://%v/disk/offline?addr=%v&disk=%v", host.host, addr, badDisk)
	} else {
		reqURL = fmt.Sprintf("http://%v/disk/decommission?addr=%v&disk=%v", host.host, addr, badDisk)
		if force {
			reqURL = fmt.Sprintf("http://%v/disk/decommission?addr=%v&disk=%v&force=true", host.host, addr, badDisk)
		}
	}
	data, err := doRequest(reqURL, host.isReleaseCluster)
	if err != nil {
		log.LogErrorf("action[offlineDataNodeDisk] occurred err,url[%v],err %v", reqURL, err)
		return
	}
	msg := fmt.Sprintf("action[offlineDataNodeDisk] reqURL[%v],data[%v]", reqURL, string(data))
	checktool.WarnBySpecialUmpKey(UMPCFSNormalWarnKey, msg)
	return
}

func offlineDataNodeDisk(host *ClusterHost, addr, badDisk string, force bool) {
	badDPsCount, err := getBadPartitionIDsCount(host)
	if err != nil {
		log.LogWarn(fmt.Sprintf("action[offlineDataNodeDisk] getBadPartitionIDsCount host:%v err:%v", host, err))
		return
	}
	if badDPsCount >= maxBadDataPartitionsCount {
		log.LogWarn(fmt.Sprintf("action[offlineDataNodeDisk] host:%v badDPsCount:%v more than maxBadDataPartitionsCount:%v addr:%v, badDisk:%v",
			host, badDPsCount, maxBadDataPartitionsCount, addr, badDisk))
		return
	}
	log.LogDebug(fmt.Sprintf("action[offlineDataNodeDisk] host:%v badDPsCount:%v maxBadDataPartitionsCount:%v addr:%v, badDisk:%v force:%v",
		host, badDPsCount, maxBadDataPartitionsCount, addr, badDisk, force))
	doOfflineDataNodeDisk(host, addr, badDisk, force)
}

//当前处于恢复中的DP数量
func getBadPartitionIDsCount(host *ClusterHost) (badDPsCount int, err error) {
	cv, err := getCluster(host)
	if err != nil {
		return
	}
	for _, badPartitionView := range cv.BadPartitionIDs {
		badDPsCount += len(badPartitionView.PartitionIDs)
		if badPartitionView.PartitionID != 0 {
			badDPsCount++
		}
	}
	for _, migratedDataPartition := range cv.MigratedDataPartitions {
		badDPsCount += len(migratedDataPartition.PartitionIDs)
		if migratedDataPartition.PartitionID != 0 {
			badDPsCount++
		}
	}
	return
}
