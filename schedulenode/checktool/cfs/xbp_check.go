package cfs

import (
	"fmt"
	"github.com/cubefs/cubefs/schedulenode/common/xbp"
	"github.com/cubefs/cubefs/util/checktool"
	"github.com/cubefs/cubefs/util/log"
	"time"
)

const (
	XBPTicketTypeDataNode = "DataNode"
	XBPTicketTypeNodeDisk = "BadDisk"
)

func (s *ChubaoFSMonitor) scheduleToCheckXBPTicket() {
	t := time.NewTimer(time.Duration(s.scheduleInterval) * time.Second)
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-t.C:
			s.checkXBPTicket()
			t.Reset(time.Duration(s.scheduleInterval) * time.Second)
		}
	}
}

func (s *ChubaoFSMonitor) checkXBPTicket() {
	defer checktool.HandleCrash()
	ticketIDs := make([]int, 0)
	s.badDiskXBPTickets.Range(func(_, value interface{}) bool {
		if ticketInfo, ok := value.(XBPTicketInfo); ok {
			if ticketInfo.status == xbp.TicketStatusReject || ticketInfo.status == xbp.TicketStatusFinish {
				return true
			}
			ticketIDs = append(ticketIDs, ticketInfo.tickerID)
		}
		return true
	})
	if len(ticketIDs) == 0 {
		return
	}
	ticketStatus, err := xbp.GetTicketsStatus(xbp.Domain, xbp.Sign, xbp.Erp, ticketIDs)
	if err != nil {
		log.LogErrorf("action[checkXBPTicket] failed ticketIDs:%v err:%v", ticketIDs, err)
		return
	}
	s.badDiskXBPTickets.Range(func(key, value interface{}) bool {
		badDiskXBPTicketKey, ok := key.(string)
		if !ok {
			return true
		}
		ticketInfo, ok := value.(XBPTicketInfo)
		if !ok {
			return true
		}
		if ticketInfo.status == xbp.TicketStatusReject || ticketInfo.status == xbp.TicketStatusFinish {
			return true
		}
		for _, ts := range ticketStatus {
			if ts.TicketID == ticketInfo.tickerID {
				switch ts.Status {
				case xbp.TicketStatusCancel:
					s.badDiskXBPTickets.Delete(badDiskXBPTicketKey)
				case xbp.TicketStatusReject:
					// 如果驳回 就删掉 那么后面检测到 可能会再次添加， 如果不删除掉 后面再次 检测就不会在添加了
					// 通过状态和时间标记
					ticketInfo.status = xbp.TicketStatusReject
					ticketInfo.lastUpdateTime = time.Now()
					s.badDiskXBPTickets.Store(badDiskXBPTicketKey, ticketInfo)
				case xbp.TicketStatusFinish:
					//再次探活节点是否需要下线
					if !recheckIsNeedOfflineXBPTicket(ticketInfo) {
						msg := fmt.Sprintf("ticketID:%v node:%s type:%s is not need offline any more",
							ticketInfo.tickerID, ticketInfo.nodeAddr, ticketInfo.ticketType)
						checktool.WarnBySpecialUmpKey(UMPCFSNormalWarnKey, msg)
						ticketInfo.status = xbp.TicketStatusFinish
						ticketInfo.lastUpdateTime = time.Now()
						s.badDiskXBPTickets.Store(badDiskXBPTicketKey, ticketInfo)
					}
					if data, err := doRequest(ticketInfo.url, ticketInfo.isReleaseCluster); err == nil {
						//处理之后 添加标记 + 对应时间记录，超过一定时间 还有相同的 就创建新的XBP单子
						//避免 1.清除之后又进行了添 或者 后面有进行了添加 问题
						msg := fmt.Sprintf("ticketID:%v send url[%v] success, resp[%v]", ticketInfo.tickerID, ticketInfo.url, string(data))
						checktool.WarnBySpecialUmpKey(UMPCFSNormalWarnKey, msg)
						ticketInfo.status = xbp.TicketStatusFinish
						ticketInfo.lastUpdateTime = time.Now()
						s.badDiskXBPTickets.Store(badDiskXBPTicketKey, ticketInfo)
					} else {
						log.LogErrorf("key:%v url[%v] ticketId:%v failed,data:%v err:%v",
							badDiskXBPTicketKey, ticketInfo.url, ticketInfo.tickerID, string(data), err)
						ticketInfo.status = xbp.TicketStatusFinish
						ticketInfo.lastUpdateTime = time.Now()
						s.badDiskXBPTickets.Store(badDiskXBPTicketKey, ticketInfo)
					}
				case xbp.TicketStatusInProgress:
					break
				default:
					break
				}
				break
			}
		}
		return true
	})
}

func (s *ChubaoFSMonitor) addDataNodeOfflineXBPTicket(nodeAddr, url, clusterID string, isReleaseCluster bool) {
	dataNodeXBPTicketKey := fmt.Sprintf("%s", nodeAddr)
	value, ok := s.badDiskXBPTickets.Load(dataNodeXBPTicketKey)
	if !ok {
		newTicketInfo, err := CreateOfflineXBPTicket(clusterID, nodeAddr, fmt.Sprintf("datanode err"), url, isReleaseCluster)
		if err != nil {
			log.LogErrorf("action[addDataNodeOfflineXBPTicket] err:%v", err)
			return
		}
		newTicketInfo.ticketType = XBPTicketTypeDataNode
		s.badDiskXBPTickets.Store(dataNodeXBPTicketKey, newTicketInfo)
	} else {
		ticketInfo, ok := value.(XBPTicketInfo)
		if !ok {
			return
		}
		// 订单号为0 或者单子已经处理（驳回/完结），但是超过一定时间还有告警
		if ticketInfo.tickerID == 0 || (ticketInfo.status == xbp.TicketStatusReject && time.Now().Sub(ticketInfo.lastUpdateTime) > 5*time.Hour) ||
			(ticketInfo.status == xbp.TicketStatusFinish && time.Now().Sub(ticketInfo.lastUpdateTime) > 3*time.Hour) {
			newTicketInfo, err := CreateOfflineXBPTicket(clusterID, nodeAddr, fmt.Sprintf("datanode err"), url, isReleaseCluster)
			if err != nil {
				log.LogErrorf("action[addDataNodeOfflineXBPTicket] err:%v", err)
				return
			}
			newTicketInfo.ticketType = XBPTicketTypeDataNode
			s.badDiskXBPTickets.Store(dataNodeXBPTicketKey, newTicketInfo)
		}
	}
}

func recheckIsNeedOfflineXBPTicket(ticketInfo XBPTicketInfo) (isNeedOffline bool) {
	switch ticketInfo.ticketType {
	case XBPTicketTypeDataNode:
		return isPhysicalMachineFailure(ticketInfo.nodeAddr)
	case XBPTicketTypeNodeDisk:
		return true
	default:
		return false
	}
}
