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
)

var (
	nodeInfoStopC     = make(chan struct{}, 0)
	deleteLimiteRater = rate.NewLimiter(rate.Inf, DefaultMarkDeleteLimitBurst)

	logMaxSize uint64
)

func (m *DataNode) startUpdateNodeInfo() {
	updateNodeBaseInfoTicker := time.NewTicker(UpdateNodeBaseInfoTicket)
	defer func() {
		updateNodeBaseInfoTicker.Stop()
	}()

	// call once on init before first tick
	m.updateNodeBaseInfo()
	for {
		select {
		case <-nodeInfoStopC:
			log.LogInfo("datanode nodeinfo goroutine stopped")
			return
		case <-updateNodeBaseInfoTicker.C:
			m.updateNodeBaseInfo()
		}
	}
}

func (m *DataNode) stopUpdateNodeInfo() {
	nodeInfoStopC <- struct{}{}
}

func (m *DataNode) updateNodeBaseInfo() {
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

	m.space.SetDiskFixTinyDeleteRecordLimit(limitInfo.DataNodeFixTinyDeleteRecordLimitOnDisk)
	if limitInfo.DataNodeRepairTaskCountZoneLimit != nil {
		if taskLimit, ok := limitInfo.DataNodeRepairTaskCountZoneLimit[m.zoneName]; ok {
			limitInfo.DataNodeRepairTaskLimitOnDisk = taskLimit
		}
	}
	m.space.SetDiskRepairTaskLimit(limitInfo.DataNodeRepairTaskLimitOnDisk)
	m.space.SetForceFlushFDInterval(limitInfo.DataNodeFlushFDInterval)
	m.space.SetSyncWALOnUnstableEnableState(limitInfo.DataSyncWALOnUnstableEnableState)
	m.space.SetForceFlushFDParallelismOnDisk(limitInfo.DataNodeFlushFDParallelismOnDisk)
	m.space.SetPartitionConsistencyMode(limitInfo.DataPartitionConsistencyMode)

	m.space.SetNormalExtentDeleteExpireTime(limitInfo.DataNodeNormalExtentDeleteExpire)
	if statistics.StatisticsModule != nil {
		statistics.StatisticsModule.UpdateMonitorSummaryTime(limitInfo.MonitorSummarySec)
		statistics.StatisticsModule.UpdateMonitorReportTime(limitInfo.MonitorReportSec)
	}

	m.updateLogMaxSize(limitInfo.LogMaxSize)
}

func (m *DataNode) getVolPartMap() map[string]map[uint64]bool {
	volPartMap := make(map[string]map[uint64]bool)
	var (
		partMap map[uint64]bool
		ok      bool
	)
	m.space.RangePartitions(func(dp *DataPartition) bool {
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

func (m *DataNode) updateLogMaxSize(val uint64) {
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

func (m *DataNode) startUpdateProcessStatInfo() {
	m.processStatInfo = statinfo.NewProcessStatInfo()
	m.processStatInfo.ProcessStartTime = time.Now().Format("2006-01-02 15:04:05")
	go m.processStatInfo.UpdateStatInfoSchedule()
}
