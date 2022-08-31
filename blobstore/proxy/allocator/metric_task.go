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

package allocator

import (
	"context"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/cubefs/cubefs/blobstore/common/trace"
)

var proxyStatusMetric = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Namespace: "blobstore",
		Subsystem: "proxy",
		Name:      "volume_status",
		Help:      "proxy status about allocator",
	},
	[]string{"service", "cluster", "idc", "codemode", "type"},
)

func init() {
	prometheus.MustRegister(proxyStatusMetric)
}

func (v *volumeMgr) metricReportTask() {
	ticker := time.NewTicker(time.Duration(v.MetricReportIntervalS) * time.Second)
	defer ticker.Stop()
	span, ctx := trace.StartSpanFromContext(context.Background(), "")
	span.Debugf("start metric report task.")
	for {
		select {
		case <-ticker.C:
			v.metricReport(ctx)
		case <-v.closeCh:
			span.Debugf("loop metric report task done.")
			return
		}
	}
}

func (v *volumeMgr) metricReport(ctx context.Context) {
	span := trace.SpanFromContextSafe(ctx)
	for codeMode, modeInfo := range v.modeInfos {
		vols := modeInfo.volumes.List()
		volNums := len(vols)
		proxyStatusMetric.With(
			prometheus.Labels{
				"service":  "PROXY",
				"cluster":  strconv.FormatUint(uint64(v.ClusterID), 10),
				"idc":      v.Idc,
				"codemode": codeMode.String(),
				"type":     "volume_nums",
			}).Set(float64(volNums))
		proxyStatusMetric.With(
			prometheus.Labels{
				"service":  "PROXY",
				"cluster":  strconv.FormatUint(uint64(v.ClusterID), 10),
				"idc":      v.Idc,
				"codemode": codeMode.String(),
				"type":     "total_free_size",
			}).Set(float64(modeInfo.totalFree))
		span.Debugf("metric report total_free_size: %v, idc: %v, codemode: %v,", float64(modeInfo.totalFree),
			v.Idc, codeMode.String())
	}
}
