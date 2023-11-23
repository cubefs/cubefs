// Copyright 2018 The CubeFS Authors.
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
	"sync"

	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/ump"
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

func Warning(detail string) (a *Alarm) {
	key := fmt.Sprintf("%v_%v_warning", clustername, modulename)
	ump.Alarm(key, detail)
	log.LogCritical(key, detail)
	if !enabledPrometheus {
		return
	}
	a = AlarmPool.Get().(*Alarm)
	a.name = metricsName(key)
	a.Add(1)
	return
}
