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
)

func (w *Wrapper) ScheduleDataPartitionMetricsReport() {
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
		case <-w.stopC:
			return
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
