package data

import (
	"fmt"
	"runtime/debug"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/log"
)

const (
	defaultMetricReportSec   = 600
	defaultMetricFetchSec    = 60
	metricCleanFetchErrCount = 30
	metricFetchMaxErrCount   = 3

	followerReadDelaySummaryInterval = 1 * 60 //second
)

func (w *Wrapper) ScheduleDataPartitionMetricsReport() {
	defer w.wg.Done()
	defer func() {
		if r := recover(); r != nil {
			log.LogErrorf("ScheduleDataPartitionMetricsReport panic: err(%v) stack(%v)", r, string(debug.Stack()))
			msg := fmt.Sprintf("ScheduleDataPartitionMetricsReport panic: err(%v)", r)
			handleUmpAlarm(w.clusterName, w.volName, "ScheduleDataPartitionMetricsReport", msg)
		}
	}()
	reportIntervalSec := w.dpMetricsReportConfig.ReportIntervalSec
	reportTicker := time.NewTicker(time.Duration(reportIntervalSec) * time.Second)
	defer reportTicker.Stop()

	fetchIntervalSec := w.dpMetricsReportConfig.FetchIntervalSec
	fetchTicker := time.NewTicker(time.Duration(fetchIntervalSec) * time.Second)
	defer fetchTicker.Stop()

	for {
		select {
		case <-w.stopC:
			return
		case <-reportTicker.C:
			w.reportMetrics()
			if w.dpMetricsReportConfig.ReportIntervalSec != reportIntervalSec {
				reportIntervalSec = w.dpMetricsReportConfig.ReportIntervalSec
				reportTicker.Reset(time.Duration(reportIntervalSec) * time.Second)
				log.LogInfof("ScheduleDataPartitionMetricsReport: reset reportIntervalSec(%v)", reportIntervalSec)
			}
		case <-fetchTicker.C:
			w.refreshMetrics()
			if w.dpMetricsReportConfig.FetchIntervalSec != fetchIntervalSec {
				fetchIntervalSec = w.dpMetricsReportConfig.FetchIntervalSec
				fetchTicker.Reset(time.Duration(fetchIntervalSec) * time.Second)
				log.LogInfof("ScheduleDataPartitionMetricsReport: reset fetchIntervalSec(%v)", fetchIntervalSec)
			}
		}
	}
}

func (w *Wrapper) reportMetrics() {
	defer func() {
		if r := recover(); r != nil {
			log.LogErrorf("reportMetrics panic: err(%v) stack(%v)", r, string(debug.Stack()))
			msg := fmt.Sprintf("reportMetrics panic: err(%v)", r)
			handleUmpAlarm(w.clusterName, w.volName, "reportMetrics", msg)
		}
	}()
	if w.dpMetricsReportConfig.EnableReport && w.dpMetricsFetchErrCount <= metricFetchMaxErrCount {
		dpMetrics := w.SummaryDataPartitionMetrics()
		if len(dpMetrics) == 0 {
			return
		}
		msg := &proto.DataPartitionMetricsMessage{
			DpMetrics: dpMetrics,
		}
		body := msg.EncodeBinary()
		if err := w.schedulerClient.SchedulerAPI().ReportDpMetrics(proto.MetricVersion, w.clusterName, w.volName, LocalIP, time.Now().Unix(), body); err != nil {
			log.LogWarnf("reportMetrics err: %v, vol(%v)", err, w.volName)
			return
		}
		log.LogInfof("reportMetrics: report dp metrics total(%v)", len(dpMetrics))
	}
	return
}

func (w *Wrapper) refreshMetrics() {
	defer func() {
		if r := recover(); r != nil {
			log.LogErrorf("refreshMetrics panic: err(%v) stack(%v)", r, string(debug.Stack()))
			msg := fmt.Sprintf("refreshMetrics panic: err(%v)", r)
			handleUmpAlarm(w.clusterName, w.volName, "refreshMetrics", msg)
		}
	}()
	var (
		message *proto.DataPartitionMetricsMessage
		err     error
	)
	dpMetricsMap := make(map[uint64]*proto.DataPartitionMetrics)
	if w.dpMetricsRefreshCount%metricCleanFetchErrCount == 0 {
		w.dpMetricsFetchErrCount = 0
	}
	enableRemote := w.dpMetricsReportConfig.EnableReport && w.dpMetricsFetchErrCount <= metricFetchMaxErrCount
	w.dpMetricsRefreshCount++
	if enableRemote {
		var reply []byte
		if reply, err = w.schedulerClient.SchedulerAPI().GetDpMetrics(proto.MetricVersion, w.clusterName, w.volName, LocalIP, time.Now().Unix()); err != nil {
			w.dpMetricsFetchErrCount++
			log.LogWarnf("refreshMetrics: getMetricsFromScheduler err(%v) vol(%v)", err, w.volName)
			return
		}
		message = &proto.DataPartitionMetricsMessage{}
		if err = message.DecodeBinary(reply); err != nil {
			w.dpMetricsFetchErrCount++
			log.LogWarnf("refreshMetrics: DecodeBinary err(%v) replyLen(%v) vol(%v)", err, len(reply), w.volName)
			return
		}
		for _, metrics := range message.DpMetrics {
			dpMetricsMap[metrics.PartitionId] = metrics
		}
	}
	if err = w.RefreshDataPartitionMetrics(enableRemote, dpMetricsMap); err != nil {
		log.LogWarnf("refreshMetrics: err(%v) vol(%v)", err, w.volName)
		return
	}
	log.LogInfof("RefreshMetrics: refresh dp metrics, enableRemote(%v) remote metrics total(%v)", enableRemote, len(dpMetricsMap))
}

func (w *Wrapper) dpFollowerReadDelayCollect() {
	defer w.wg.Done()
	defer func() {
		if r := recover(); r != nil {
			log.LogErrorf("dpFollowerReadDelaySummary panic: err(%v) stack(%v)", r, string(debug.Stack()))
			msg := fmt.Sprintf("dpFollowerReadDelaySummary panic: err(%v)", r)
			handleUmpAlarm(w.clusterName, w.volName, "dpFollowerReadDelaySummary", msg)
		}
	}()
	var (
		summaryInterval int64
		needClear       bool
	)
	// set ticker: time can be set in config or default
	if w.dpFollowerReadDelayConfig.DelaySummaryInterval != 0 {
		summaryInterval = w.dpFollowerReadDelayConfig.DelaySummaryInterval
	} else {
		summaryInterval = followerReadDelaySummaryInterval
	}
	summaryTicker := time.NewTicker(time.Duration(summaryInterval) * time.Second)
	defer summaryTicker.Stop()
	for {
		select {
		case <-summaryTicker.C:
			// collect dps and it's hosts FollowerRead Delay, compute average delay over summary interval,
			// And sort hosts by average delay, update ReadMetrics in DP.
			if w.followerRead && w.dpFollowerReadDelayConfig.EnableCollect {
				w.SummaryAndSortReadDelay()
				needClear = true
			} else if needClear {
				w.clearDpReadMetrics()
				needClear = !needClear
			}
			if w.dpFollowerReadDelayConfig.DelaySummaryInterval > 0 && w.dpFollowerReadDelayConfig.DelaySummaryInterval != summaryInterval {
				summaryInterval = w.dpFollowerReadDelayConfig.DelaySummaryInterval
				summaryTicker.Reset(time.Duration(summaryInterval) * time.Second)
				log.LogInfof("dpFollowerReadDelaySummary: reset summaryInterval(%v)", summaryInterval)
			}
		case <-w.stopC:
			return
		}
	}
}

func (w *Wrapper) SummaryAndSortReadDelay() {
	w.RLock()
	dataPartitions := w.partitions
	w.RUnlock()

	sumMetrics := make([]*proto.ReadMetrics, 0)
	for _, dp := range dataPartitions {
		metrics := dp.RemoteReadMetricsSummary()
		if metrics != nil {
			sumMetrics = append(sumMetrics, metrics)
		}
	}

	w.GetAvgDelayAndSort(sumMetrics)
}

func (w *Wrapper) GetAvgDelayAndSort(sumMetrics []*proto.ReadMetrics) {
	if len(sumMetrics) < 1 {
		return
	}
	for _, metrics := range sumMetrics {
		readOpNum := metrics.FollowerReadOpNum
		summaryDelay := metrics.SumFollowerReadHostDelay
		metrics.AvgFollowerReadHostDelay = make(map[string]int64)
		if len(summaryDelay) != len(readOpNum) {
			log.LogErrorf("GetAvgDelayAndSort failed: metrics len is not equal, dpID(%v)", metrics.PartitionId)
			continue
		}
		for host, delay := range summaryDelay {
			var (
				num int64
				ok  bool
			)
			if num, ok = readOpNum[host]; !ok || num == 0 {
				log.LogErrorf("GetAvgDelayAndSort failed: illegal metrics")
				continue
			}
			metrics.AvgFollowerReadHostDelay[host] = delay / num
		}
		// Sort and update dp.ReadMetrics
		sortedHosts, err := w.SortAvgDelay(metrics)
		if err != nil {
			continue
		}
		w.updateDpReadMetrics(sortedHosts)
	}
}

func (w *Wrapper) SortAvgDelay(metrics *proto.ReadMetrics) (sortedHosts []string, err error) {
	dpId := metrics.PartitionId
	hostsAvgDelay := metrics.AvgFollowerReadHostDelay
	if hostsAvgDelay == nil || len(hostsAvgDelay) < 1 {
		err = fmt.Errorf("SortAvgDelay failed: metrics is nil/len < 1, dpID(%v)", dpId)
		return nil, err
	}
	// find dp's unvisited host, append to the tail
	w.RLock()
	dpAllHosts := w.partitions[dpId].Hosts
	w.RUnlock()

	var unVisitedHosts []string
	for _, host := range dpAllHosts {
		_, ok := hostsAvgDelay[host]
		if !ok {
			unVisitedHosts = append(unVisitedHosts, host)
		}
	}
	sortedHosts = sortHostAvgDelayMap(hostsAvgDelay)
	sortedHosts = append(sortedHosts, unVisitedHosts...)
	if log.IsDebugEnabled() {
		log.LogDebugf("SortAvgDelay success: sorted hosts(%v), dpID(%v)", sortedHosts, dpId)
	}
	return sortedHosts, nil
}

func sortHostAvgDelayMap(hostsAvgDelay map[string]int64) (sortedHosts []string) {
	size := len(hostsAvgDelay)
	sortedHosts = make([]string, 0, size)
	for host := range hostsAvgDelay {
		sortedHosts = append(sortedHosts, host)
	}
	for i := 0; i < size; i++ {
		for j := i + 1; j < size; j++ {
			if hostsAvgDelay[sortedHosts[i]] > hostsAvgDelay[sortedHosts[j]] {
				sortedHosts[i], sortedHosts[j] = sortedHosts[j], sortedHosts[i]
			}
		}
	}
	return
}

func (w *Wrapper) updateDpReadMetrics(sortedHosts []string) {
	w.RLock()
	dataPartitions := w.partitions
	w.RUnlock()
	for _, dp := range dataPartitions {
		dp.UpdateReadMetricsHost(sortedHosts)
	}
}

func (w *Wrapper) clearDpReadMetrics() {
	w.RLock()
	dataPartitions := w.partitions
	w.RUnlock()
	for _, dp := range dataPartitions {
		dp.ClearReadMetrics()
	}
}
