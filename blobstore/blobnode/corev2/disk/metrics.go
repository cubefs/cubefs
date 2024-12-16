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

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/cubefs/cubefs/blobstore/blobnode/base"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

var DiskStatMetric = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Namespace: "blobstore",
		Subsystem: "blobnode",
		Name:      "disk_stat",
		Help:      "blobnode disk stat",
	},
	[]string{"cluster_id", "idc", "rack", "host", "disk_id", "item"},
)

func init() {
	prometheus.MustRegister(DiskStatMetric)
}

func (ds *DiskStorage) loopMetricReport() {
	span, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "", "loopMetricReport")

	ticker := time.NewTicker(time.Duration(ds.Conf.MetricReportIntervalS) * time.Second)
	defer ticker.Stop()

	span.Debugf("start metric report task.")

	for {
		select {
		case <-ds.closeCh:
			return
		case <-ticker.C:
			ds.metricReport(ctx)
		}
	}
}

func (ds *DiskStorage) metricReport(ctx context.Context) {
	span := trace.SpanFromContextSafe(ctx)

	stats := ds.Stats()
	statsMetrics, err := base.GenMetric(stats)
	if err != nil {
		span.Errorf("diskID:%v, err:%v", err)
		return
	}

	for item, value := range statsMetrics {
		DiskStatMetric.With(prometheus.Labels{
			"cluster_id": ds.Conf.ClusterID.ToString(),
			"idc":        ds.Conf.IDC,
			"rack":       ds.Conf.Rack,
			"host":       ds.Conf.Host,
			"disk_id":    ds.DiskID.ToString(),
			"item":       item,
		}).Set(value)
	}
}
