// Copyright 2024 The CubeFS Authors.
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

package trace

import (
	"time"
)

// TracerOption is a function that sets some option on the tracer
type SpanOption func(spanOptions) spanOptions

type spanOptionDuration uint8

const (
	durationAny spanOptionDuration = iota
	durationNone
	durationNs
	durationUs
	durationMs // default
	durationSecond
	durationMinute
	durationHour
)

func (d spanOptionDuration) Value(duration time.Duration) int64 {
	var v int64
	switch d {
	case durationNone:
	case durationNs:
		v = duration.Nanoseconds()
	case durationUs:
		v = duration.Microseconds()
	case durationMs:
		v = duration.Milliseconds()
	case durationSecond:
		v = int64(duration / time.Second)
	case durationMinute:
		v = int64(duration / time.Minute)
	case durationHour:
		v = int64(duration / time.Hour)
	default:
	}
	return v
}

func (d spanOptionDuration) Unit(duration time.Duration) string {
	switch d {
	case durationNs:
		return "ns"
	case durationUs:
		return "us"
	case durationMs:
		return "ms"
	case durationSecond:
		return "s"
	case durationMinute:
		return "m"
	case durationHour:
		return "h"
	default:
		return ""
	}
}

type spanOptions struct {
	duration     spanOptionDuration
	durationUnit bool
	errorLength  uint16
}

var (
	_durationAny    = durTemplate(durationAny)
	_durationNone   = durTemplate(durationNone)
	_durationNs     = durTemplate(durationNs)
	_durationUs     = durTemplate(durationUs)
	_durationMs     = durTemplate(durationMs)
	_durationSecond = durTemplate(durationSecond)
	_durationMinute = durTemplate(durationMinute)
	_durationHour   = durTemplate(durationHour)
	_durationUnit   = func(o spanOptions) spanOptions { o.durationUnit = true; return o }

	_mapErrorLength = func() map[int]SpanOption { // pre-allocation
		m := make(map[int]SpanOption, maxErrorLen+2)
		for ii := 0; ii <= maxErrorLen; ii++ {
			length := ii
			m[ii] = func(o spanOptions) spanOptions { o.errorLength = uint16(length); return o }
		}
		m[-1] = func(o spanOptions) spanOptions { o.errorLength = maxErrorLen; return o }
		return m
	}()
)

func durTemplate(od spanOptionDuration) SpanOption {
	return func(o spanOptions) spanOptions { o.duration = od; return o }
}
func OptSpanDurationAny() SpanOption    { return _durationAny }
func OptSpanDurationNone() SpanOption   { return _durationNone }
func OptSpanDurationNs() SpanOption     { return _durationNs }
func OptSpanDurationUs() SpanOption     { return _durationUs }
func OptSpanDurationMs() SpanOption     { return _durationMs }
func OptSpanDurationSecond() SpanOption { return _durationSecond }
func OptSpanDurationMinute() SpanOption { return _durationMinute }
func OptSpanDurationHour() SpanOption   { return _durationHour }
func OptSpanDurationUnit() SpanOption   { return _durationUnit }
func OptSpanErrorLength(l int) SpanOption {
	if l < 0 {
		return _mapErrorLength[-1]
	}
	if l <= maxErrorLen {
		return _mapErrorLength[l]
	}
	heapl := l
	return func(o spanOptions) spanOptions {
		o.errorLength = uint16(heapl)
		return o
	}
}
