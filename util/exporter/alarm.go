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
	"github.com/chubaofs/chubaofs/util/ump"
	"sync"
)

var (
	AlarmPool = &sync.Pool{New: func() interface{} {
		return new(Alarm)
	}}
	//AlarmGroup  sync.Map
	AlarmCh chan *Alarm
)

func collectAlarm() {
	AlarmCh = make(chan *Alarm, ChSize)
	for {
		m := <-AlarmCh
		AlarmPool.Put(m)
	}
}

type Alarm struct {
	Counter
}

func NewAlarm(key string) (a *Alarm) {
	if !enabled {
		ump.Alarm(fmt.Sprintf("%v_%v_warning", clustername, modulename), key)
		return
	}
	a = AlarmPool.Get().(*Alarm)
	a.name = metricsName(fmt.Sprintf("%s_alarm", key))
	a.Add(1)
	return
}

func (c *Alarm) publish() {
	select {
	case AlarmCh <- c:
	default:
	}
}
