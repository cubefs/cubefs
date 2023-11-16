package statistics

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/util/tpmonitor"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/log"
)

const (
	defaultSummarySecond = 5
	defaultReportSecond  = 10
)

var (
	StatisticsModule  *Statistics
	once              sync.Once
	targetCluster     string
	targetModuleName  string
	targetNodeAddr    string
	targetSummaryFunc func(reportTime int64) []*MonitorData
)

type Statistics struct {
	cluster       string
	module        string
	address       string
	sendList      []*ReportData // store data per second
	sendListLock  sync.RWMutex
	monitorAddr   string
	summarySecond uint64
	reportSecond  uint64
	stopC         chan bool
	Range         func(deal func(data *MonitorData, volName, diskPath string, pid uint64))
}

type MonitorData struct {
	Action      int
	ActionStr   string
	Size        uint64 // the num of read/write byte
	Count       uint64
	tpMonitor   *tpmonitor.TpMonitor
}

type ReportData struct {
	VolName     string
	PartitionID uint64
	Action      int
	ActionStr   string
	Size        uint64 // the num of read/write byte
	Count       uint64
	Tp99        uint64
	Avg         uint64
	Max         uint64
	ReportTime  int64
	TimeStr     string
	IsTotal     bool
	DiskPath    string // disk of dp
}

type ReportInfo struct {
	Cluster string
	Addr    string
	Module  string
	Infos   []*ReportData
}

func (m *Statistics) String() string {
	return fmt.Sprintf("{Cluster(%v) Module(%v) IP(%v) MonitorAddr(%v)}", m.cluster, m.module, m.address, m.monitorAddr)
}

func (data *MonitorData) String() string {
	return fmt.Sprintf("{Action(%v)ActionNum(%v)Count(%v)Size(%v)}",
		data.ActionStr, data.Action, data.Count, data.Size)
}

func newStatistics(monitorAddr, cluster, moduleName, nodeAddr string) *Statistics {
	return &Statistics{
		cluster:       cluster,
		module:        moduleName,
		address:       nodeAddr,
		monitorAddr:   monitorAddr,
		sendList:      make([]*ReportData, 0),
		summarySecond: defaultSummarySecond,
		reportSecond:  defaultReportSecond,
		stopC:         make(chan bool),
	}
}

func InitStatistics(cfg *config.Config, cluster, moduleName, nodeAddr string, IterFunc func(deal func(data *MonitorData, volName, diskPath string, pid uint64))) {
	targetCluster = cluster
	targetModuleName = moduleName
	targetNodeAddr = nodeAddr
	monitorAddr := cfg.GetString(ConfigMonitorAddr)
	if monitorAddr == "" || StatisticsModule != nil {
		return
	}
	once.Do(func() {
		StatisticsModule = newStatistics(monitorAddr, cluster, moduleName, nodeAddr)
		StatisticsModule.Range = IterFunc
		go StatisticsModule.summaryJob()
		go StatisticsModule.reportJob()
	})
}

func (m *Statistics) UpdateMonitorSummaryTime(newSecondTime uint64) {
	if m != nil && newSecondTime > 0 && newSecondTime != m.summarySecond {
		atomic.StoreUint64(&m.summarySecond, newSecondTime)
	}
}

func (m *Statistics) UpdateMonitorReportTime(newSecondTime uint64) {
	if m != nil && newSecondTime > 0 && newSecondTime != m.reportSecond {
		atomic.StoreUint64(&m.reportSecond, newSecondTime)
	}
}

func (m *Statistics) GetMonitorSummaryTime() uint64 {
	return atomic.LoadUint64(&m.summarySecond)
}

func (m *Statistics) GetMonitorReportTime() uint64 {
	return atomic.LoadUint64(&m.reportSecond)
}

func (m *Statistics) CloseStatistics() {
	if m.stopC != nil {
		close(m.stopC)
	}
}

func InitMonitorData(module string) []*MonitorData {
	var num int
	var actionMap map[int]string
	switch module {
	case ModelDataNode:
		num = len(proto.ActionDataMap)
		actionMap = proto.ActionDataMap
	case ModelMetaNode:
		num = len(proto.ActionMetaMap)
		actionMap = proto.ActionMetaMap
	case ModelObjectNode:
		num = len(ActionObjectMap)
		actionMap = ActionObjectMap
	case ModelFlashNode:
		num = len(proto.ActionFlashMap)
		actionMap = proto.ActionFlashMap
	}
	m := make([]*MonitorData, num)
	for i := 0; i < num; i++ {
		m[i] = &MonitorData{
			tpMonitor: tpmonitor.NewTpMonitor(),
			Action: i,
			ActionStr: actionMap[i],
		}
	}
	return m
}

type TpObject struct {
	sTime   time.Time
	monitor *MonitorData
}

func (data *MonitorData) BeforeTp() *TpObject {
	return &TpObject{
		sTime:   time.Now(),
		monitor: data,
	}
}

func (tpObject *TpObject) AfterTp(dataSize uint64) {
	if StatisticsModule == nil || tpObject == nil || tpObject.monitor == nil {
		return
	}
	data := tpObject.monitor
	data.tpMonitor.Accumulate(int(time.Since(tpObject.sTime).Microseconds()), tpObject.sTime)
	atomic.AddUint64(&data.Count, 1)
	atomic.AddUint64(&data.Size, dataSize)
}

func (data *MonitorData) SetCost(dataSize uint64, costUs int, start time.Time) {
	if StatisticsModule == nil {
		return
	}
	atomic.AddUint64(&data.Count, 1)
	atomic.AddUint64(&data.Size, dataSize)
	if data.tpMonitor != nil {
		data.tpMonitor.Accumulate(costUs, start)
	}
	return
}

func (data *MonitorData) UpdateData(dataSize uint64) {
	if StatisticsModule == nil {
		return
	}
	atomic.AddUint64(&data.Count, 1)
	atomic.AddUint64(&data.Size, dataSize)
}


func (data *MonitorData) GenReportData(vol, path string, pid uint64, reportTime int64) *ReportData{
	if atomic.LoadUint64(&data.Count) == 0 {
		return nil
	}

	reportData := &ReportData{
		VolName:     vol,
		PartitionID: pid,
		Action:      data.Action,
		ActionStr:   data.ActionStr,
		ReportTime:  reportTime,
		DiskPath:    path,
		Size:        atomic.SwapUint64(&data.Size, 0),
		Count:       atomic.SwapUint64(&data.Count, 0),
	}
	if data.tpMonitor != nil {
		reportData.Max, reportData.Avg, reportData.Tp99 = data.tpMonitor.CalcTp()
	}

	return reportData
}

func (m *Statistics) summaryJob() {
	defer func() {
		if err := recover(); err != nil {
			log.LogErrorf("Monitor: summary job panic(%v) module(%v) ip(%v)", err, m.module, m.address)
		}
	}()
	summaryTime := m.GetMonitorSummaryTime()
	sumTicker := time.NewTicker(time.Duration(summaryTime) * time.Second)
	defer sumTicker.Stop()
	log.LogInfof("Monitor: start summary job, ticker (%v)s", summaryTime)
	for {
		select {
		case <-sumTicker.C:
			reportTime := time.Now().Unix()
			dataList := make([]*ReportData, 0)
			m.Range(func(data *MonitorData, vol, path string, pid uint64) {
				reportData := data.GenReportData(vol, path, pid, reportTime)
				if reportData != nil && reportData.Count != 0 {
					dataList = append(dataList, reportData)
				}

			})
			m.sendListLock.Lock()
			m.sendList = append(m.sendList, dataList...)
			m.sendListLock.Unlock()
			// check summary time
			newSummaryTime := m.GetMonitorSummaryTime()
			if newSummaryTime > 0 && newSummaryTime != summaryTime {
				summaryTime = newSummaryTime
				sumTicker.Reset(time.Duration(newSummaryTime) * time.Second)
				log.LogInfof("Monitor: summaryJob reset ticker (%v)s", newSummaryTime)
			}
		case <-m.stopC:
			log.LogWarnf("Monitor: stop summary job")
			return
		}
	}
}

func (m *Statistics) reportJob() {
	defer func() {
		if err := recover(); err != nil {
			log.LogErrorf("Monitor: report job panic(%v) module(%v) ip(%v)", err, m.module, m.address)
		}
	}()
	reportTime := m.GetMonitorReportTime()
	reportTicker := time.NewTicker(time.Duration(reportTime) * time.Second)
	defer reportTicker.Stop()
	log.LogInfof("Monitor: start report job, ticker (%v)s", reportTime)
	for {
		select {
		case <-reportTicker.C:
			sendList := m.currentSendList()
			if len(sendList) > 0 {
				m.reportToMonitor(sendList)
			}
			// check report time
			newReportTime := m.GetMonitorReportTime()
			if newReportTime > 0 && newReportTime != reportTime {
				reportTime = newReportTime
				reportTicker.Reset(time.Duration(newReportTime) * time.Second)
				log.LogInfof("Monitor: reportJob reset ticker (%v)s", newReportTime)
			}
		case <-m.stopC:
			log.LogWarnf("Monitor: stop report job")
			return
		}
	}
}

func (m *Statistics) currentSendList() []*ReportData {
	m.sendListLock.Lock()
	defer m.sendListLock.Unlock()

	sendList := m.sendList
	m.sendList = make([]*ReportData, 0)
	return sendList
}

func (m *Statistics) reportToMonitor(sendList []*ReportData) {
	report := &ReportInfo{
		Cluster: m.cluster,
		Module:  m.module,
		Addr:    m.address,
		Infos:   sendList,
	}
	data, _ := json.Marshal(report)
	m.sendToMonitor(data)
}
