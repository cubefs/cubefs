package statistics

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chubaofs/chubaofs/util/config"
	"github.com/chubaofs/chubaofs/util/log"
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
	sendList      []*MonitorData // store data per second
	sendListLock  sync.RWMutex
	monitorAddr   string
	summarySecond uint64
	reportSecond  uint64
	stopC         chan bool
}

type MonitorData struct {
	VolName     string
	PartitionID uint64
	Action      int
	ActionStr   string
	Size        uint64 // the num of read/write byte
	Count       uint64
	ReportTime  int64
	TimeStr     string
	IsTotal     bool
	DiskPath    string // disk of dp
}

type ReportInfo struct {
	Cluster string
	Addr    string
	Module  string
	Infos   []*MonitorData
}

func (m *Statistics) String() string {
	return fmt.Sprintf("{Cluster(%v) Module(%v) IP(%v) MonitorAddr(%v)}", m.cluster, m.module, m.address, m.monitorAddr)
}

func (data *MonitorData) String() string {
	return fmt.Sprintf("{Vol(%v)Pid(%v)Action(%v)ActionNum(%v)Count(%v)Size(%v)Disk(%v)ReportTime(%v)IsTotal(%v)}",
		data.VolName, data.PartitionID, data.ActionStr, data.Action, data.Count, data.Size, data.DiskPath, data.ReportTime, data.IsTotal)
}

func newStatistics(monitorAddr, cluster, moduleName, nodeAddr string) *Statistics {
	return &Statistics{
		cluster:       cluster,
		module:        moduleName,
		address:       nodeAddr,
		monitorAddr:   monitorAddr,
		sendList:      make([]*MonitorData, 0),
		summarySecond: defaultSummarySecond,
		reportSecond:  defaultReportSecond,
		stopC:         make(chan bool),
	}
}

func InitStatistics(cfg *config.Config, cluster, moduleName, nodeAddr string, summaryFunc func(reportTime int64) []*MonitorData) {
	targetCluster = cluster
	targetModuleName = moduleName
	targetNodeAddr = nodeAddr
	targetSummaryFunc = summaryFunc
	monitorAddr := cfg.GetString(ConfigMonitorAddr)
	if monitorAddr == "" || StatisticsModule != nil {
		return
	}
	once.Do(func() {
		StatisticsModule = newStatistics(monitorAddr, cluster, moduleName, nodeAddr)
		go StatisticsModule.summaryJob(summaryFunc)
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
	switch module {
	case ModelDataNode:
		num = len(ActionDataMap)
	case ModelMetaNode:
		num = len(ActionMetaMap)
	case ModelObjectNode:
		num = len(ActionObjectMap)
	}
	m := make([]*MonitorData, num)
	for i := 0; i < num; i++ {
		m[i] = &MonitorData{}
	}
	return m
}

func (data *MonitorData) UpdateData(dataSize uint64) {
	if StatisticsModule == nil {
		return
	}
	atomic.AddUint64(&data.Count, 1)
	atomic.AddUint64(&data.Size, dataSize)
}

func (m *Statistics) summaryJob(summaryFunc func(reportTime int64) []*MonitorData) {
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
			dataList := summaryFunc(reportTime)
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

func (m *Statistics) currentSendList() []*MonitorData {
	m.sendListLock.Lock()
	defer m.sendListLock.Unlock()

	sendList := m.sendList
	m.sendList = make([]*MonitorData, 0)
	return sendList
}

func (m *Statistics) reportToMonitor(sendList []*MonitorData) {
	report := &ReportInfo{
		Cluster: m.cluster,
		Module:  m.module,
		Addr:    m.address,
		Infos:   sendList,
	}
	data, _ := json.Marshal(report)
	m.sendToMonitor(data)
}
