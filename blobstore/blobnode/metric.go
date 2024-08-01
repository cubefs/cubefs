package blobnode

import (
	"github.com/prometheus/client_golang/prometheus"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
)

var (
	diskHealthMetric = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "blobstore",
			Subsystem: "blobnode",
			Name:      "lost_disk",
			Help:      "blobnode lost disk",
		},
		[]string{"cluster_id", "idc", "rack", "host", "disk"},
	)

	networkMetric = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "blobstore",
			Subsystem: "blobnode",
			Name:      "network",
			Help:      "blobnode network traffic",
		},
		[]string{"cluster_id", "idc", "host", "node", "api", "io_type"},
	)
)

func init() {
	prometheus.MustRegister(diskHealthMetric)
	prometheus.MustRegister(networkMetric)
}

// when find the lost disk, set value 1
func (s *Service) reportLostDisk(hostInfo *core.HostInfo, diskPath string) {
	diskHealthMetric.WithLabelValues(hostInfo.ClusterID.ToString(),
		hostInfo.IDC,
		hostInfo.Rack,
		hostInfo.Host,
		diskPath,
	).Set(1)
}

// 1. when startup ok, we set value 0(normal disk)
// 2. when the lost disk is report to cm(set broken), we know it is bad disk, set 0
func (s *Service) reportOnlineDisk(hostInfo *core.HostInfo, diskPath string) {
	diskHealthMetric.WithLabelValues(hostInfo.ClusterID.ToString(),
		hostInfo.IDC,
		hostInfo.Rack,
		hostInfo.Host,
		diskPath,
	).Set(0)
}

func (s *Service) reportGetTraffic(ioType bnapi.IOType, size int64) {
	node := s.Conf.HostInfo
	networkMetric.WithLabelValues(node.ClusterID.ToString(), node.IDC, node.Host, node.NodeID.ToString(),
		"get", ioType.String()).Add(float64(size))
}

func (s *Service) reportPutTraffic(ioType bnapi.IOType, size int64) {
	node := s.Conf.HostInfo
	networkMetric.WithLabelValues(node.ClusterID.ToString(), node.IDC, node.Host, node.NodeID.ToString(),
		"put", ioType.String()).Add(float64(size))
}
