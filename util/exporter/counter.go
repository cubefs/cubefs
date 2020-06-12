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

import "sync"

const (
	CounterValChSize = 32 * 1024
)

var (
	counterValMap map[string]float64
	counterMu     sync.RWMutex
	counterValCh  chan *Counter
	counterStopCh chan NULL
)

func startCounter() {
	counterValCh = make(chan *Counter, CounterValChSize)
	counterValMap = make(map[string]float64)
	counterStopCh = make(chan NULL, 1)
	go runCountValRoutine()
}

func stopCounter() {
	counterStopCh <- null
}

func runCountValRoutine() {
	for {
		select {
		case <-counterStopCh:
			return
		case c := <-counterValCh:
			key := c.Key()
			val := c.val
			counterMu.Lock()
			if v, ok := counterValMap[key]; ok {
				v += val
				counterValMap[key] = v
			} else {
				counterValMap[key] = val
			}
			counterMu.Unlock()
		}
	}
}

//
type Counter struct {
	Gauge
}

func addVal(c *Counter) {
	select {
	case counterValCh <- c:
	default:
	}
}

func NewCounter(name string) (c *Counter) {
	if !IsEnabled() {
		return
	}
	c = new(Counter)
	c.name = name
	return
}

func (c *Counter) Val() (val float64) {
	counterMu.RLock()
	if v, ok := counterValMap[c.Key()]; ok {
		val = v
	}
	counterMu.RUnlock()

	return
}

func (c *Counter) Add(val int64) {
	if !IsEnabled() {
		return
	}
	c.val = float64(val)
	addVal(c)
	CollectorInstance().Collect(c)
}

func (c *Counter) AddWithLabels(val int64, labels map[string]string) {
	if !IsEnabled() {
		return
	}
	c.labels = labels
	c.Add(val)
}
