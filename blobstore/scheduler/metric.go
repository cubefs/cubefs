package scheduler

import (
	"strconv"

	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	_metricDegradeStatsItemBlob   = "blob"
	_metricDegradeStatsItemVolume = "volume"
)

var dgradeStatsMetric = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Namespace: "blobstore",
		Subsystem: "scheduler",
		Name:      "degrade_stats",
		Help:      "scheduler degrade stats",
	},
	[]string{"cluster_id", "item", "codename", "degrade_level"},
)

func init() {
	prometheus.MustRegister(dgradeStatsMetric)
}

func reportDegradeStats(
	clusterID string,
	item string,
	aggregated map[codemode.CodeMode]map[int]float64,
) {
	for cm, levelMap := range aggregated {
		for level, total := range levelMap {
			dgradeStatsMetric.WithLabelValues(
				clusterID,
				item,
				cm.String(),
				strconv.Itoa(level),
			).Set(total)
		}
	}
}
