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
	"strings"
	"time"

	"github.com/cubefs/cubefs/util/exporter/backend/prom"
	"github.com/cubefs/cubefs/util/exporter/backend/ump"
)

type UMPTPPrecision int8

const (
	PrecisionMs UMPTPPrecision = iota
	PrecisionUs
)

var (
	unspecifiedTime = time.Time{}
	promKeyReplacer = strings.NewReplacer("-", "_", ".", "_", " ", "_", ",", "_", ":", "_")
)

type TP interface {
	Set(err error)
}

type promTP struct {
	tp      prom.Summary
	failure prom.Counter
	start   time.Time
}

func (tp *promTP) Set(err error) {
	if tp == nil {
		return
	}
	if tp.tp != nil {
		tp.tp.Observe(float64(time.Since(tp.start).Nanoseconds()))
	}
	if tp.failure != nil {
		if err != nil {
			tp.failure.Add(1)
		} else {
			tp.failure.Add(0)
		}
	}
}

func newPromTP(name string, start time.Time, lvs ...prom.LabelValue) TP {
	name = promKeyReplacer.Replace(name)
	var tp = &promTP{
		tp:      prom.GetSummary(name, lvs...),
		failure: prom.GetCounter(fmt.Sprintf("%s_failure", name), lvs...),
		start:   time.Now(),
	}
	if start == unspecifiedTime {
		tp.start = time.Now()
		return tp
	}
	tp.start = start
	return tp
}

type umpTP struct {
	to        *ump.TpObject
	precision UMPTPPrecision
}

func (tp *umpTP) Set(err error) {
	if tp == nil {
		return
	}
	if tp.to != nil {
		if tp.precision == PrecisionUs {
			ump.AfterTPUs(tp.to, err)
			return
		}
		ump.AfterTP(tp.to, err)
	}
}

func newUmpTP(key string, start time.Time, precision UMPTPPrecision) TP {
	var tp = &umpTP{
		precision: precision,
	}
	if start == unspecifiedTime {
		tp.to = ump.BeforeTP(key)
		return tp
	}
	tp.to = ump.BeforeTPWithStartTime(key, start)
	return tp
}

type noonTP struct{}

func (tp *noonTP) Set(_ error) {
	return
}

var singletonNoonTP = &noonTP{}

type multipleTP []TP

func (tp multipleTP) Set(err error) {
	for _, recorder := range tp {
		recorder.Set(err)
	}
}

func newTP(key string, start time.Time, precision UMPTPPrecision) (tp TP) {
	var umpTP TP = singletonNoonTP
	if umpEnabled {
		umpTP = newUmpTP(key, start, precision)
	}
	var promTP TP = singletonNoonTP
	if promEnabled {
		promTP = newPromTP(key, start)
	}
	tp = multipleTP{umpTP, promTP}
	return
}

func newModuleTP(op string, precision UMPTPPrecision) (tp TP) {
	if len(zoneName) > 0 {
		return multipleTP{
			newTP(fmt.Sprintf("%s_%s_%s", clusterName, moduleName, op), unspecifiedTime, precision),
			newTP(fmt.Sprintf("%s_%s_%s_%s", clusterName, zoneName, moduleName, op), unspecifiedTime, precision),
		}
	}
	return newTP(fmt.Sprintf("%s_%s_%s", clusterName, moduleName, op), unspecifiedTime, precision)
}

func newVolumeTP(op string, volume string, precision UMPTPPrecision) (tp TP) {
	return newTP(fmt.Sprintf("%s_%s_%s", clusterName, volume, op), unspecifiedTime, precision)
}

func NewModuleTP(op string) TP {
	return newModuleTP(op, PrecisionMs)
}

func NewModuleTPUs(op string) TP {
	return newModuleTP(op, PrecisionUs)
}

func NewVolumeTP(op string, volume string) TP {
	return newVolumeTP(op, volume, PrecisionMs)
}

func NewVolumeTPUs(op string, volume string) TP {
	return newVolumeTP(op, volume, PrecisionUs)
}

func NewCustomKeyTP(key string) TP {
	return newTP(key, unspecifiedTime, PrecisionMs)
}

func NewCustomKeyTPUs(key string) TP {
	return newTP(key, unspecifiedTime, PrecisionUs)
}

func NewCustomKeyTPWithStartTime(key string, start time.Time) TP {
	return newTP(key, start, PrecisionMs)
}

func NewCustomKeyTPUsWithStartTime(key string, start time.Time) TP {
	return newTP(key, start, PrecisionUs)
}
