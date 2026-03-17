package servicemgr

import (
	"context"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/prometheus/client_golang/prometheus"
)

var registeredServicesMetric = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Namespace: "blobstore",
		Subsystem: "clusterMgr",
		Name:      "registered_services",
		Help:      "registered services info",
	},
	[]string{"region", "cluster", "idc", "service", "host", "status"},
)

type serviceNodeInfo struct {
	region  string
	cluster string
	idc     string
	name    string
	host    string
	status  string
}

func init() {
	prometheus.MustRegister(registeredServicesMetric)
}

func (s *ServiceMgr) reportServicesMetric(region string, cluster string) {
	currentSnapshot := make([]serviceNodeInfo, 0)

	s.cache.Range(func(key, value interface{}) bool {
		sv := value.(*service)
		sv.RLock()
		for _, val := range sv.nodes {
			nodeStatus := "online"
			if time.Until(val.Expires) <= 0 {
				nodeStatus = "offline"
			}
			currentSnapshot = append(currentSnapshot, serviceNodeInfo{
				region:  region,
				cluster: cluster,
				idc:     val.Idc,
				name:    val.Name,
				host:    val.Host,
				status:  nodeStatus,
			})
		}
		sv.RUnlock()

		return true
	})

	registeredServicesMetric.Reset()
	for _, item := range currentSnapshot {
		registeredServicesMetric.WithLabelValues(
			item.region,
			item.cluster,
			item.idc,
			item.name,
			item.host,
			item.status,
		).Set(float64(1))
	}
}

func (s *ServiceMgr) Report(ctx context.Context, region string, clusterID proto.ClusterID) {
	s.reportServicesMetric(region, clusterID.ToString())
}
