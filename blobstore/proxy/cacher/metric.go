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

package cacher

import (
	"github.com/prometheus/client_golang/prometheus"
)

var cacheMetric = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "blobstore",
		Subsystem: "proxy",
		Name:      "cache",
		Help:      "cache statuts on proxy",
	},
	[]string{"cluster", "service", "name", "action"},
)

func init() {
	prometheus.MustRegister(cacheMetric)
}

func (c *cacher) metricReport(service, name, action string) {
	cacheMetric.WithLabelValues(c.clusterID.ToString(), service, name, action).Inc()
}

func (c *cacher) volumeReport(name, action string) {
	c.metricReport("volume", name, action)
}

func (c *cacher) diskReport(name, action string) {
	c.metricReport("disk", name, action)
}
