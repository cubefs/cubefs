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

package kvstat

import (
	"reflect"
)

// RangeTagValue iterates over the fields of Stats via reflection and invokes
// the closure f with each field's metric name and its value.
//
// Only fields carrying a "metric" tag are reported, the tag value is the item
// name used by prometheus; metric:"-" keeps a field out of the metrics, and a
// nested struct field (MemoryUsage) is flattened unless it carries metric:"-".
// The "json" tag only describes the /debug/meta/stats payload and plays no part
// here. bool fields are passed as 1/0, other unsupported kinds (strings, slices
// such as ColumnFamilies) are skipped.
func (s *Stats) RangeTagValue(f func(tag string, value uint64)) {
	rangeTagValue(reflect.ValueOf(*s), f)
}

// RangeCFs iterates over ColumnFamilies, invoking f with the CF name and the
// per-CF stats, whose RangeTagValue can be used to range over the fields.
func (s *Stats) RangeCFs(f func(name string, cf *CFStats)) {
	for i := range s.ColumnFamilies {
		f(s.ColumnFamilies[i].Name, &s.ColumnFamilies[i])
	}
}

// RangeTagValue iterates over the fields of CFStats, same as Stats.
func (c *CFStats) RangeTagValue(f func(tag string, value uint64)) {
	rangeTagValue(reflect.ValueOf(*c), f)
}

func rangeTagValue(rv reflect.Value, f func(tag string, value uint64)) {
	rt := rv.Type()
	for i := 0; i < rv.NumField(); i++ {
		fv := rv.Field(i)
		if !fv.CanInterface() || metricExcluded(rt.Field(i)) {
			continue
		}
		if fv.Kind() == reflect.Struct {
			rangeTagValue(fv, f)
			continue
		}
		tag, ok := tagName(rt.Field(i))
		if !ok {
			continue
		}
		if v, ok := tagValue(fv); ok {
			f(tag, v)
		}
	}
}

// metricExcluded reports whether the field is opted out of metrics, which also
// excludes the whole subtree of a nested struct field.
func metricExcluded(sf reflect.StructField) bool {
	return sf.Tag.Get("metric") == "-"
}

// tagValue converts a scalar field to the uint64 representation reported by
// RangeTagValue. It reports false for kinds that are not reported.
func tagValue(fv reflect.Value) (uint64, bool) {
	switch fv.Kind() {
	case reflect.Bool:
		if fv.Bool() {
			return 1, true
		}
		return 0, true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return fv.Uint(), true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return uint64(fv.Int()), true
	case reflect.Float32, reflect.Float64:
		return uint64(fv.Float()), true
	}
	return 0, false
}

// tagName returns the metric name of a field and whether the field is
// reported. The "metric" tag is the only source of the name: a field without
// it is not reported, so a metric is never collected by accident. The json tag
// is validated by TestStatsFieldsHaveJSONTags instead.
func tagName(sf reflect.StructField) (name string, ok bool) {
	switch metric := sf.Tag.Get("metric"); metric {
	case "", "-":
		return "", false
	default:
		return metric, true
	}
}
