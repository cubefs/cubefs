// Copyright 2022 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package disk

import "github.com/prometheus/client_golang/prometheus"

// buckets is default histogram buckets for compact tasks
var buckets = []float64{30, 60, 300, 600, 1800, 3600, 7200, 14400}

var (
	// compactCounterVec counts chunk compact executions, labeled by result={success,failed}.
	compactCounterVec = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "blobstore",
			Subsystem: "blobnode",
			Name:      "compact",
			Help:      "Number of chunk compact executions labeled by result (success or failed)",
		},
		[]string{"cluster_id", "host", "disk_id", "result"},
	)

	// compactDurationVec records per-chunk compact duration distribution (seconds).
	// only record success compact task
	compactDurationVec = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "blobstore",
			Subsystem: "blobnode",
			Name:      "compact_duration_seconds",
			Help:      "Duration in seconds for each chunk compact operation",
			Buckets:   buckets,
		},
		[]string{"cluster_id", "host", "disk_id"},
	)

	// compactCopyBytesVec counts total bytes migrated from old chunk to new chunk.
	compactCopyBytesVec = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "blobstore",
			Subsystem: "blobnode",
			Name:      "compact_copy_bytes",
			Help:      "Total bytes copied from old chunk to new chunk during compact",
		},
		[]string{"cluster_id", "host", "disk_id"},
	)

	// compactCopyShardsVec counts total shards successfully migrated.
	compactCopyShardsVec = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "blobstore",
			Subsystem: "blobnode",
			Name:      "compact_copy_shards",
			Help:      "Total number of shards successfully migrated during compact",
		},
		[]string{"cluster_id", "host", "disk_id"},
	)

	// compactReplicaStgVec gauges chunks currently in double-write (replica) state.
	compactReplicaStgVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "blobstore",
			Subsystem: "blobnode",
			Name:      "compact_replica_stg",
			Help:      "Number of chunks currently in double-write (replica) state during compact",
		},
		[]string{"cluster_id", "host", "disk_id"},
	)

	// compactReplicaBytesVec counts bytes written in double-write mode during compact.
	compactReplicaBytesVec = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "blobstore",
			Subsystem: "blobnode",
			Name:      "compact_replica_bytes",
			Help:      "Total bytes written in double-write mode during compact",
		},
		[]string{"cluster_id", "host", "disk_id"},
	)

	// compactReplicaCntVec counts double-write requests during compact.
	compactReplicaCntVec = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "blobstore",
			Subsystem: "blobnode",
			Name:      "compact_replica_cnt",
			Help:      "Total number of double-write requests during compact",
		},
		[]string{"cluster_id", "host", "disk_id"},
	)

	// compactPendingVec gauges chunks that satisfy compact trigger conditions but have not yet been executed.
	compactPendingVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "blobstore",
			Subsystem: "blobnode",
			Name:      "compact_pending",
			Help:      "Number of chunks satisfying compact trigger conditions but not yet executed on this disk",
		},
		[]string{"cluster_id", "host", "disk_id"},
	)
)

func init() {
	prometheus.MustRegister(
		compactCounterVec,
		compactDurationVec,
		compactCopyBytesVec,
		compactCopyShardsVec,
		compactReplicaStgVec,
		compactReplicaBytesVec,
		compactReplicaCntVec,
		compactPendingVec,
	)
}

// compactMetrics holds pre-bound prometheus observers for a single disk instance.
// All label values are bound at construction time to avoid per-call map allocation.
type compactMetrics struct {
	counterSuccess prometheus.Counter
	counterFailed  prometheus.Counter
	duration       prometheus.Observer
	copyBytes      prometheus.Counter
	copyShards     prometheus.Counter
	replicaStg     prometheus.Gauge
	replicaBytes   prometheus.Counter
	replicaCnt     prometheus.Counter
	pending        prometheus.Gauge
}

// newCompactMetrics creates a compactMetrics instance with labels pre-bound to the given disk.
func newCompactMetrics(ds *DiskStorage) *compactMetrics {
	baseLabels := prometheus.Labels{
		"cluster_id": ds.Conf.ClusterID.ToString(),
		"host":       ds.Conf.Host,
		"disk_id":    ds.DiskID.ToString(),
	}
	successLabels := prometheus.Labels{
		"cluster_id": ds.Conf.ClusterID.ToString(),
		"host":       ds.Conf.Host,
		"disk_id":    ds.DiskID.ToString(),
		"result":     "success",
	}
	failLabels := prometheus.Labels{
		"cluster_id": ds.Conf.ClusterID.ToString(),
		"host":       ds.Conf.Host,
		"disk_id":    ds.DiskID.ToString(),
		"result":     "failed",
	}
	return &compactMetrics{
		counterSuccess: compactCounterVec.With(successLabels),
		counterFailed:  compactCounterVec.With(failLabels),
		duration:       compactDurationVec.With(baseLabels),
		copyBytes:      compactCopyBytesVec.With(baseLabels),
		copyShards:     compactCopyShardsVec.With(baseLabels),
		replicaStg:     compactReplicaStgVec.With(baseLabels),
		replicaBytes:   compactReplicaBytesVec.With(baseLabels),
		replicaCnt:     compactReplicaCntVec.With(baseLabels),
		pending:        compactPendingVec.With(baseLabels),
	}
}
