package exporter

import (
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

const StatPeriod = time.Minute * time.Duration(1)

type VersionMetrics struct {
	stopC      chan struct{}
	Metric     *Counter
	moduleName string
	statPeriod time.Duration
}

func NewVersionMetrics(module string) *VersionMetrics {
	return &VersionMetrics{
		moduleName: module,
		Metric:     NewCounter(Version),
		stopC:      make(chan struct{}),
		statPeriod: StatPeriod,
	}
}

func (m *VersionMetrics) SetStatPeriod(period time.Duration) {
	m.statPeriod = period
}

func (m *VersionMetrics) Start() {
	ticker := time.NewTicker(m.statPeriod)
	for {
		select {
		case <-m.stopC:
			ticker.Stop()
			log.LogInfof("stop version metrics ticker")
			return
		case <-ticker.C:
			m.doStat()
		}
	}
}

func (m *VersionMetrics) Stop() {
	close(m.stopC)
	log.LogInfof("stop version metrics")
}

func (m *VersionMetrics) doStat() {
	m.setVersionMetrics()
}

func (m *VersionMetrics) setVersionMetrics() {
	labels := proto.GetVersion(m.moduleName).ToMap()
	delete(labels, "commit")
	delete(labels, "role")
	m.Metric.AddWithLabels(1, labels)
}
