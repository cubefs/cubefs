package catalog

import (
	"strconv"

	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/prometheus/client_golang/prometheus"
)

var suidEpochNotConsistentMetric = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Namespace: "blobstore",
		Subsystem: "clusterMgr",
		Name:      "suid_epoch_not_consistent",
		Help:      "suid epoch not consistent",
	},
	[]string{"region", "cluster", "cm_suid", "report_suid"},
)

func init() {
	prometheus.MustRegister(suidEpochNotConsistentMetric)
}

func reportSuidEpochNotConsistent(CMSuid, reportSuid proto.Suid, region string, clusterID proto.ClusterID) {
	suidEpochNotConsistentMetric.Reset()
	CMSuidStr := strconv.FormatUint(uint64(CMSuid), 10)
	reportSuidStr := strconv.FormatUint(uint64(reportSuid), 10)
	suidEpochNotConsistentMetric.WithLabelValues(region, clusterID.ToString(), CMSuidStr, reportSuidStr).Set(float64(1))
}
