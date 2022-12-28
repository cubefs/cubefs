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
	"sync"

	"github.com/chubaofs/chubaofs/util/log"
	"github.com/chubaofs/chubaofs/util/ump"
)

var (
	AlarmPool = &sync.Pool{New: func() interface{} {
		return new(Alarm)
	}}
	//AlarmGroup  sync.Map
	AlarmCh    chan *Alarm
	warningKey string
)

func collectAlarm() {
	defer wg.Done()
	AlarmCh = make(chan *Alarm, ChSize)
	for {
		select {
		case <-stopC:
			AlarmPool = nil
			return
		case m := <-AlarmCh:
			AlarmPool.Put(m)
		}
	}
}

type Alarm struct {
	Counter
}

func Warning(detail string) (a *Alarm) {
	if warningKey == "" {
		warningKey = fmt.Sprintf("%v_%v_warning", clustername, modulename)
	}
	ump.Alarm(warningKey, detail)
	log.LogCritical(warningKey, detail)
	return
}

func WarningCritical(detail string) (a *Alarm) {
	if warningKey == "" {
		warningKey = fmt.Sprintf("%v_%v_critical", clustername, modulename)
	}
	ump.Alarm(warningKey, detail)
	log.LogCritical(warningKey, detail)
	return
}

func WarningPanic(detail string) (a *Alarm) {
	if warningKey == "" {
		warningKey = fmt.Sprintf("%v_%v_panic", clustername, modulename)
	}
	ump.Alarm(warningKey, detail)
	log.LogCritical(warningKey, detail)
	return
}

func (c *Alarm) publish() {
	select {
	case AlarmCh <- c:
	default:
	}
}

func WarningRocksdbError(detail string) (a *Alarm) {
	key := fmt.Sprintf("%v_metanode_rocksdb_error_warning", clustername)
	ump.Alarm(key, detail)
	log.LogCritical(key, detail)
	return
}
