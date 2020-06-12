// Copyright 2018 The Chubao Authors.
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

package exporter

import (
	"fmt"
)

// gauge metric
type Gauge struct {
	name   string
	labels map[string]string
	val    float64
}

func NewGauge(name string) (g *Gauge) {
	if !IsEnabled() {
		return
	}
	g = new(Gauge)
	g.name = name
	return
}

func (g *Gauge) Key() string {
	return stringMD5(fmt.Sprintf("{%s: %s}", g.name, stringMapToString(g.labels)))
}

func (g *Gauge) Name() string {
	return g.name
}

func (g *Gauge) Labels() map[string]string {
	return g.labels
}

func (g *Gauge) Val() float64 {
	return g.val
}

func (g *Gauge) String() string {
	return fmt.Sprintf("{name: %s, labels: %s, val: %v}", g.name, stringMapToString(g.labels), g.val)
}

// set gauge val
func (g *Gauge) Set(val int64) {
	if !IsEnabled() {
		return
	}
	g.val = float64(val)
	CollectorInstance().Collect(g)
}

// set gauge val with labels
func (g *Gauge) SetWithLabels(val int64, labels map[string]string) {
	if !IsEnabled() {
		return
	}
	g.labels = labels
	g.Set(val)
}
