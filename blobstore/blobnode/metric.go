package blobnode

import (
	"github.com/prometheus/client_golang/prometheus"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
)

var (
	// readBadShardVec counts bad shards discovered during foreground user reads (Get/Put)
	// It is just read counts, not unique bad shard counts
	readBadShardVec = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "blobstore",
			Subsystem: "blobnode",
			Name:      "read_bad_shard",
			Help:      "Number of bad shards discovered during foreground user reads",
		},
		[]string{"cluster_id", "disk_id"},
	)

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

	// pendingBrokenDiskMetric is the number of local disks whose data-file EIO is
	// not promoted to broken yet
	pendingBrokenDiskMetric = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "blobstore",
			Subsystem: "blobnode",
			Name:      "pending_broken_disk",
			Help:      "Number of local disks with data-file EIO waiting to be marked broken",
		},
		[]string{"cluster_id", "idc", "rack", "host"},
	)
)

func init() {
	prometheus.MustRegister(readBadShardVec)
	prometheus.MustRegister(diskHealthMetric)
	prometheus.MustRegister(networkMetric)
	prometheus.MustRegister(pendingBrokenDiskMetric)
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

func reportReadBadShard(info clustermgr.BlobNodeDiskInfo) {
	readBadShardVec.WithLabelValues(
		info.ClusterID.ToString(),
		info.DiskID.ToString(),
	).Inc()
}

func (s *Service) reportPendingBrokenDisks() {
	s.pendingBrokenDisksMu.RLock()
	pending := len(s.pendingBrokenDisks)
	s.pendingBrokenDisksMu.RUnlock()

	node := s.Conf.HostInfo
	pendingBrokenDiskMetric.WithLabelValues(
		node.ClusterID.ToString(),
		node.IDC,
		node.Rack,
		node.Host,
	).Set(float64(pending))
}
