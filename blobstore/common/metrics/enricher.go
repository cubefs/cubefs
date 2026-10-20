// Copyright 2026 The CubeFS Authors.
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

package metrics

import (
	"fmt"
	"net/http"
	"os"
	"sort"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/model"

	"github.com/cubefs/cubefs/blobstore/util"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

// Enricher gathers the default Prometheus registry and adds missing public labels per sample.
type Enricher struct {
	gatherer prometheus.Gatherer
	labels   []*dto.LabelPair
}

// NewEnricher builds enricher state. Values __auto_host__ / __auto_ip__ are replaced with the
// local hostname / first non-loopback IPv4; lookup failure logs a warning and still keeps the label.
func NewEnricher(cfg *Config) (*Enricher, error) {
	var configured map[string]string
	if cfg != nil {
		configured = cfg.Labels
	}

	var auto autoValues
	labels := make([]*dto.LabelPair, 0, len(configured))
	for name, raw := range configured {
		if !model.LabelName(name).IsValid() {
			log.Warnf("metrics enricher: skip invalid label key %q", name)
			continue
		}
		value, err := auto.resolve(raw)
		if err != nil {
			log.Warnf("metrics enricher: resolve label %q: %v", name, err)
		}
		labels = append(labels, newLabelPair(name, value))
	}
	sort.Slice(labels, func(i, j int) bool {
		return labels[i].GetName() < labels[j].GetName()
	})

	return &Enricher{gatherer: prometheus.DefaultGatherer, labels: labels}, nil
}

// autoValues caches placeholder lookups so each is probed at most once.
type autoValues struct {
	host, ip string
}

func (a *autoValues) resolve(value string) (string, error) {
	var err error
	switch value {
	case autoHostValue:
		if a.host == "" {
			a.host, err = os.Hostname()
		}
		return a.host, err
	case autoIPValue:
		if a.ip == "" {
			a.ip, err = util.GetLocalIP()
		}
		return a.ip, err
	}
	return value, nil
}

func newLabelPair(name, value string) *dto.LabelPair {
	return &dto.LabelPair{Name: &name, Value: &value}
}

// Gather implements prometheus.Gatherer.
// Label pairs are shared across samples; the gathered snapshot is only encoded, never mutated.
func (e *Enricher) Gather() ([]*dto.MetricFamily, error) {
	mfs, err := e.gatherer.Gather()
	if err != nil {
		return nil, err
	}
	for _, mf := range mfs {
		for _, m := range mf.Metric {
			for _, lp := range e.labels {
				if !hasLabel(m, lp.GetName()) {
					m.Label = append(m.Label, lp)
				}
			}
		}
	}
	return mfs, nil
}

func hasLabel(m *dto.Metric, name string) bool {
	for _, lp := range m.Label {
		if lp.GetName() == name {
			return true
		}
	}
	return false
}

// NewHandler builds GET /metrics/prometheus handler mux.
func NewHandler(cfg *Config) (http.Handler, error) {
	enricher, err := NewEnricher(cfg)
	if err != nil {
		return nil, fmt.Errorf("metrics: %w", err)
	}
	inner := promhttp.HandlerFor(enricher, promhttp.HandlerOpts{})
	mux := http.NewServeMux()
	mux.HandleFunc(PathPrometheus, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		inner.ServeHTTP(w, r)
	})
	return mux, nil
}
