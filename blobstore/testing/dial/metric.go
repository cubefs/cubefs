// Copyright 2023 The CubeFS Authors.
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

package dial

import (
	"os"
	"strconv"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	namespace = "dialtest"
	subsystem = "blobstore"
)

var (
	labels  = []string{"region", "cluster", "idc", "consul", "method"}
	buckets = []float64{1, 5, 10, 50, 200, 1000, 5000}

	hostname    = getHostname()
	constLabels = map[string]string{"host": hostname}
)

var (
	httpcodeMetric = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "httpcode",
			Help:        "http code counter",
			ConstLabels: constLabels,
		}, append(labels, "code"),
	)
	throughputMetric = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "throughput",
			Help:        "http bytes throughput",
			ConstLabels: constLabels,
		}, labels,
	)
	latencyMetric = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "latency",
			Help:        "http latency duration ms",
			Buckets:     buckets[:],
			ConstLabels: constLabels,
		}, labels,
	)
)

func init() {
	prometheus.MustRegister(httpcodeMetric)
	prometheus.MustRegister(throughputMetric)
	prometheus.MustRegister(latencyMetric)
}

func getHostname() string {
	hostname, _ := os.Hostname()
	return hostname
}

func runTimer(conn Connection, method string, size int, f func() error) {
	st := time.Now()
	err := f()
	duration := time.Since(st)

	code := rpc.DetectStatusCode(err)
	region, cluster, idc, consul := conn.Region, conn.Cluster, conn.IDC, conn.ConsulAddr
	httpcodeMetric.WithLabelValues(region, cluster, idc, consul, method, strconv.Itoa(code)).Inc()

	d := duration.Milliseconds()
	latencyMetric.WithLabelValues(region, cluster, idc, consul, method).Observe(float64(d))
	if code < 300 && size > 0 && d > 0 {
		throughputMetric.WithLabelValues(region, cluster, idc, consul, method).Add(float64(size) / (float64(d) / 1000))
	}
}
