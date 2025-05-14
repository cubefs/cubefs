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

package stream

import (
	"os"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc/auditlog"
)

var unhealthMetric = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "blobstore",
		Subsystem: "access",
		Name:      "unhealth",
		Help:      "unhealth action on access",
	},
	[]string{"cluster", "action", "module", "host", "reason"},
)

var downloadMetric = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "blobstore",
		Subsystem: "access",
		Name:      "download",
		Help:      "download way on access",
	},
	[]string{"cluster", "way", "reason"},
)

var readwriteMetric *prometheus.HistogramVec

var SteamReportDownload = reportDownload

func init() {
	prometheus.MustRegister(unhealthMetric)
	prometheus.MustRegister(downloadMetric)

	hostname, _ := os.Hostname()
	readwriteMetric = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace:   "blobstore",
			Subsystem:   "access",
			Name:        "read_write_duration_ms",
			Help:        "read write duration ms",
			Buckets:     auditlog.Buckets,
			ConstLabels: map[string]string{"host": hostname},
		},
		[]string{"cluster", "idc", "api"},
	)
	prometheus.MustRegister(readwriteMetric)
}

func reportUnhealth(cid proto.ClusterID, action, module, host, reason string) {
	unhealthMetric.WithLabelValues(cid.ToString(), action, module, host, reason).Inc()
}

func reportDownload(cid proto.ClusterID, way, reason string) {
	downloadMetric.WithLabelValues(cid.ToString(), way, reason).Inc()
}

// upload_read, upload_write, download_read, download_write
func reportReadwrite(cid, idc, api string, ms int64) {
	readwriteMetric.WithLabelValues(cid, idc, api).Observe(float64(ms))
}
