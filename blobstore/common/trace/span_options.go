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
type SpanOption func(*spanOptions)

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
	}
	return ""
}

type spanOptions struct {
	duration     spanOptionDuration
	durationUnit bool
	errorLength  int
}

func OptSpanDurationAny() SpanOption    { return func(o *spanOptions) { o.duration = durationAny } }
func OptSpanDurationNone() SpanOption   { return func(o *spanOptions) { o.duration = durationNone } }
func OptSpanDurationNs() SpanOption     { return func(o *spanOptions) { o.duration = durationNs } }
func OptSpanDurationUs() SpanOption     { return func(o *spanOptions) { o.duration = durationUs } }
func OptSpanDurationMs() SpanOption     { return func(o *spanOptions) { o.duration = durationMs } }
func OptSpanDurationSecond() SpanOption { return func(o *spanOptions) { o.duration = durationSecond } }
func OptSpanDurationMinute() SpanOption { return func(o *spanOptions) { o.duration = durationMinute } }
func OptSpanDurationHour() SpanOption   { return func(o *spanOptions) { o.duration = durationHour } }
func OptSpanDurationUnit() SpanOption   { return func(o *spanOptions) { o.durationUnit = true } }

func OptSpanErrorLength(l int) SpanOption {
	return func(o *spanOptions) {
		if l >= 0 {
			o.errorLength = l
		}
	}
}
