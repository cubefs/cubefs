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
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func collectTags(t *testing.T, f func(func(string, uint64))) map[string]uint64 {
	out := make(map[string]uint64)
	f(func(tag string, value uint64) {
		require.NotContains(t, out, tag)
		out[tag] = value
	})
	return out
}

func TestStatsRangeTagValue(t *testing.T) {
	s := Stats{
		LiveSstFilesSize:       1024,
		NumFilesAtLevel0:       3,
		NumFilesAtLevel1:       4,
		ActualDelayedWriteRate: 65536,
		IsWriteStopped:         false,
		CompactionPending:      true,
		EstimateNumKeys:        99,
		MemoryUsage: MemoryUsage{
			BlockCacheUsage:         512,
			EstimateTableReadersMem: 256,
			CurSizeAllMemTables:     128,
			BlockCachePinnedUsage:   64,
			EstimatedMemoryUsage:    896,
		},
		ColumnFamilies: []CFStats{{Name: "default"}},
	}

	got := collectTags(t, s.RangeTagValue)

	// top-level fields, named after the rocksdb properties
	require.Equal(t, uint64(1024), got["live_sst_files_size"])
	require.Equal(t, uint64(3), got["num_files_at_level0"])
	require.Equal(t, uint64(4), got["num_files_at_level1"])
	require.Equal(t, uint64(99), got["estimate_num_keys"])
	require.Equal(t, uint64(65536), got["actual_delayed_write_rate"])
	// bool fields are passed as 1/0
	require.Equal(t, uint64(0), got["is_write_stopped"])
	require.Equal(t, uint64(1), got["compaction_pending"])
	// nested MemoryUsage is flattened
	require.Equal(t, uint64(512), got["block_cache_usage"])
	require.Equal(t, uint64(256), got["estimate_table_readers_mem"])
	require.Equal(t, uint64(128), got["cur_size_all_mem_tables"])
	require.Equal(t, uint64(64), got["block_cache_pinned_usage"])
	require.Equal(t, uint64(896), got["estimated_memory_usage"])
	// slice field is not reported
	require.NotContains(t, got, "column_families")
}

func TestCFStatsRangeTagValue(t *testing.T) {
	cf := CFStats{
		Name:                           "cf1",
		LiveSstFilesSize:               10,
		CurSizeAllMemTables:            20,
		MemTableFlushPending:           true,
		EstimatePendingCompactionBytes: 30,
	}

	got := collectTags(t, cf.RangeTagValue)

	require.Equal(t, uint64(10), got["live_sst_files_size"])
	require.Equal(t, uint64(20), got["cur_size_all_mem_tables"])
	require.Equal(t, uint64(1), got["mem_table_flush_pending"])
	require.Equal(t, uint64(30), got["estimate_pending_compaction_bytes"])
	// Name is not reported
	require.NotContains(t, got, "name")
}

// TestStatsTagNames pins the reported metric names: they are the rocksdb
// property names, aggregated over column families where applicable.
func TestStatsTagNames(t *testing.T) {
	var s Stats
	var tags []string
	s.RangeTagValue(func(tag string, _ uint64) {
		tags = append(tags, tag)
	})
	sort.Strings(tags)

	require.Equal(t, []string{
		"actual_delayed_write_rate",
		"background_errors",
		"block_cache_capacity",
		"block_cache_pinned_usage",
		"block_cache_usage",
		"compaction_pending",
		"cur_size_all_mem_tables",
		"estimate_live_data_size",
		"estimate_num_keys",
		"estimate_pending_compaction_bytes",
		"estimate_table_readers_mem",
		"estimated_memory_usage",
		"is_write_stopped",
		"live_sst_files_size",
		"mem_table_flush_pending",
		"num_files_at_level0",
		"num_files_at_level1",
		"num_immutable_mem_table",
		"num_running_compactions",
		"num_running_flushes",
		"num_snapshots",
		"oldest_snapshot_time",
	}, tags)
}

func TestStatsRangeCFs(t *testing.T) {
	s := Stats{
		ColumnFamilies: []CFStats{
			{Name: "default", EstimateNumKeys: 1},
			{Name: "cf1", EstimateNumKeys: 2},
		},
	}

	var names []string
	var keys []uint64
	s.RangeCFs(func(name string, cf *CFStats) {
		names = append(names, name)
		cf.RangeTagValue(func(tag string, value uint64) {
			if tag == "estimate_num_keys" {
				keys = append(keys, value)
			}
		})
	})

	require.Equal(t, []string{"default", "cf1"}, names)
	require.Equal(t, []uint64{1, 2}, keys)
}

// TestMetricTagRequired makes sure only fields carrying a metric tag are
// reported, so a metric never shows up because of a json tag alone.
func TestMetricTagRequired(t *testing.T) {
	type jsonOnly struct {
		Value uint64 `json:"value"`
	}
	type jsonDash struct {
		Value uint64 `json:"-"`
	}
	type noTags struct {
		Value uint64
	}
	type metricOnly struct {
		Value uint64 `metric:"value"`
	}
	type renamed struct {
		Value uint64 `json:"value" metric:"renamed_value"`
	}
	type excluded struct {
		Value uint64 `json:"value" metric:"-"`
	}
	type payloadExcluded struct {
		Value uint64 `json:"-" metric:"value"`
	}

	cases := map[string]struct {
		v    interface{}
		want map[string]uint64
	}{
		"json only":       {jsonOnly{Value: 1}, nil},
		"json dash":       {jsonDash{Value: 1}, nil},
		"no tags":         {noTags{Value: 1}, nil},
		"metric only":     {metricOnly{Value: 1}, map[string]uint64{"value": 1}},
		"json and metric": {renamed{Value: 1}, map[string]uint64{"renamed_value": 1}},
		"metric dash":     {excluded{Value: 1}, nil},
		"payload omitted": {payloadExcluded{Value: 1}, map[string]uint64{"value": 1}},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			got := make(map[string]uint64)
			require.NotPanics(t, func() {
				rangeTagValue(reflect.ValueOf(c.v), func(tag string, v uint64) {
					got[tag] = v
				})
			})
			if c.want == nil {
				require.Empty(t, got)
			} else {
				require.Equal(t, c.want, got)
			}
		})
	}
}

// TestMetricTagExcludesStructSubtree makes sure metric:"-" on a nested struct
// excludes all the fields of that struct.
func TestMetricTagExcludesStructSubtree(t *testing.T) {
	type nested struct {
		Inner uint64 `json:"inner" metric:"inner"`
	}
	type flattens struct {
		Kept     uint64 `json:"kept" metric:"kept"`
		Excluded nested `json:"nested" metric:"-"`
	}

	got := make(map[string]uint64)
	rangeTagValue(reflect.ValueOf(flattens{Kept: 1, Excluded: nested{Inner: 2}}), func(tag string, v uint64) {
		got[tag] = v
	})
	require.Equal(t, map[string]uint64{"kept": 1}, got)
}

// TestStatsFieldsHaveJSONTags makes sure the http payload stays complete: every
// exported field must carry a json tag.
func TestStatsFieldsHaveJSONTags(t *testing.T) {
	for _, rt := range statsTypes() {
		for i := 0; i < rt.NumField(); i++ {
			sf := rt.Field(i)
			tag := sf.Tag.Get("json")
			require.NotEmptyf(t, tag, "%s.%s has no json tag", rt.Name(), sf.Name)
			if tag == "-" {
				continue
			}
			name, _, _ := strings.Cut(tag, ",")
			require.NotEmptyf(t, name, "%s.%s json tag %q has no name", rt.Name(), sf.Name, tag)
		}
	}
}

// TestStatsFieldsHaveMetricTags makes sure no metric is dropped by accident:
// every scalar field must declare its metric name, or opt out with metric:"-".
// Nested struct fields are containers and are flattened instead.
func TestStatsFieldsHaveMetricTags(t *testing.T) {
	itemName := regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

	for _, rt := range statsTypes() {
		for i := 0; i < rt.NumField(); i++ {
			sf := rt.Field(i)
			if sf.Type.Kind() == reflect.Struct {
				continue
			}
			metric, exist := sf.Tag.Lookup("metric")
			require.Truef(t, exist, "%s.%s has no metric tag", rt.Name(), sf.Name)
			if metric == "-" {
				continue
			}
			require.Regexpf(t, itemName, metric, "%s.%s has an invalid metric name", rt.Name(), sf.Name)
		}
	}
}

func statsTypes() []reflect.Type {
	return []reflect.Type{
		reflect.TypeOf(Stats{}),
		reflect.TypeOf(MemoryUsage{}),
		reflect.TypeOf(CFStats{}),
	}
}
