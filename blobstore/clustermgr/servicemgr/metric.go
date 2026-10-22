package servicemgr

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/cubefs/cubefs/blobstore/common/proto"
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

func init() {
	prometheus.MustRegister(registeredServicesMetric)
}

func (s *ServiceMgr) Report(_ context.Context, region string, clusterID proto.ClusterID) {
	cluster := clusterID.ToString()
	services, _ := s.ListServiceInfo()

	registeredServicesMetric.Reset()
	for _, node := range services.Nodes {
		nodeStatus := "online"
		if node.ExpireAt > 0 {
			nodeStatus = "offline"
		}
		registeredServicesMetric.WithLabelValues(region, cluster,
			node.Idc, node.Name, node.Host, nodeStatus).Set(1)
	}
}
