package datanode

import (
	"context"
	"time"

	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/statinfo"
	"github.com/cubefs/cubefs/util/statistics"
	"golang.org/x/time/rate"
)

const (
	DefaultMarkDeleteLimitBurst           = 512
	UpdateNodeBaseInfoTicket              = 1 * time.Minute
	DefaultFixTinyDeleteRecordLimitOnDisk = 1
	DefaultRepairTaskLimitOnDisk          = 5
	DefaultNormalExtentDeleteExpireTime   = 4 * 3600
	DefaultLazyLoadParallelismPerDisk     = 2
)

var (
	nodeInfoStopC     = make(chan struct{}, 0)
	deleteLimiteRater = rate.NewLimiter(rate.Inf, DefaultMarkDeleteLimitBurst)

	logMaxSize uint64
)

func (s *DataNode) startUpdateNodeInfo() {
	updateNodeBaseInfoTicker := time.NewTicker(UpdateNodeBaseInfoTicket)
	defer func() {
		updateNodeBaseInfoTicker.Stop()
	}()

	// call once on init before first tick
	s.updateNodeBaseInfo()
	for {
		select {
		case <-nodeInfoStopC:
			log.LogInfo("datanode nodeinfo goroutine stopped")
			return
		case <-updateNodeBaseInfoTicker.C:
			s.updateNodeBaseInfo()
		}
	}
}

func (s *DataNode) stopUpdateNodeInfo() {
	nodeInfoStopC <- struct{}{}
}

func (s *DataNode) updateNodeBaseInfo() {
	//todo: better using a light weighted interface
	limitInfo, err := MasterClient.AdminAPI().GetLimitInfo("")
	if err != nil {
		log.LogWarnf("[updateNodeBaseInfo] get limit info err: %s", err.Error())
		return
	}

	r := limitInfo.DataNodeDeleteLimitRate
	l := rate.Limit(r)
	if r == 0 {
		l = rate.Inf
	}
	deleteLimiteRater.SetLimit(l)

	s.space.SetDiskFixTinyDeleteRecordLimit(limitInfo.DataNodeFixTinyDeleteRecordLimitOnDisk)
	if limitInfo.DataNodeRepairTaskCountZoneLimit != nil {
		if taskLimit, ok := limitInfo.DataNodeRepairTaskCountZoneLimit[s.zoneName]; ok {
			limitInfo.DataNodeRepairTaskLimitOnDisk = taskLimit
		}
	}
	s.space.SetDiskRepairTaskLimit(limitInfo.DataNodeRepairTaskLimitOnDisk)
	s.space.SetForceFlushFDInterval(limitInfo.DataNodeFlushFDInterval)
	s.space.SetSyncWALOnUnstableEnableState(limitInfo.DataSyncWALOnUnstableEnableState)
	s.space.SetForceFlushFDParallelismOnDisk(limitInfo.DataNodeFlushFDParallelismOnDisk)
	s.space.SetPartitionConsistencyMode(limitInfo.DataPartitionConsistencyMode)

	s.space.SetNormalExtentDeleteExpireTime(limitInfo.DataNodeNormalExtentDeleteExpire)
	if statistics.StatisticsModule != nil {
		statistics.StatisticsModule.UpdateMonitorSummaryTime(limitInfo.MonitorSummarySec)
		statistics.StatisticsModule.UpdateMonitorReportTime(limitInfo.MonitorReportSec)
	}

	s.updateLogMaxSize(limitInfo.LogMaxSize)
}

func (s *DataNode) getVolPartMap() map[string]map[uint64]bool {
	volPartMap := make(map[string]map[uint64]bool)
	var (
		partMap map[uint64]bool
		ok      bool
	)
	s.space.WalkPartitions(func(dp *DataPartition) bool {
		partMap, ok = volPartMap[dp.volumeID]
		if !ok {
			partMap = make(map[uint64]bool)
			volPartMap[dp.volumeID] = partMap
		}
		partMap[dp.ID()] = true
		return true
	})
	return volPartMap
}

func (s *DataNode) updateLogMaxSize(val uint64) {
	if val != 0 && logMaxSize != val {
		oldLogMaxSize := logMaxSize
		logMaxSize = val
		log.SetLogMaxSize(int64(val))
		log.LogInfof("updateLogMaxSize, logMaxSize(old:%v, new:%v)", oldLogMaxSize, logMaxSize)
	}
}

func DeleteLimiterWait() {
	deleteLimiteRater.Wait(context.Background())
}

func (s *DataNode) startUpdateProcessStatInfo() {
	s.processStatInfo = statinfo.NewProcessStatInfo()
	s.processStatInfo.ProcessStartTime = time.Now().Format("2006-01-02 15:04:05")
	go s.processStatInfo.UpdateStatInfoSchedule()
}
