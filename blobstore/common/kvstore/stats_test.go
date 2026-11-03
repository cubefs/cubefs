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

package kvstore

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/common/kvstat"
)

func TestStatsOpenDB(t *testing.T) {
	path, err := os.MkdirTemp("", "kvstore_stats_opendb")
	require.NoError(t, err)
	defer os.RemoveAll(path)

	db, err := OpenDB(path)
	require.NoError(t, err)
	defer db.Close()

	ins := db.(*instance)

	require.NoError(t, db.Put(KV{Key: []byte("k1"), Value: []byte("v1")}))
	require.NoError(t, db.Put(KV{Key: []byte("k2"), Value: []byte("v2")}))
	// flush default CF so the SST lands in L0
	require.NoError(t, ins.db.Flush(ins.fo))

	stats, err := db.Stats()
	require.NoError(t, err)

	// single default CF: no per-CF details
	require.Empty(t, stats.ColumnFamilies)
	require.Greater(t, stats.NumFilesAtLevel0, uint64(0))
	require.Greater(t, stats.MemoryUsage.BlockCacheCapacity, uint64(0))
	require.LessOrEqual(t, stats.MemoryUsage.BlockCacheUsage, stats.MemoryUsage.BlockCacheCapacity)
	// EstimatedMemoryUsage = block cache + table readers mem + memtables
	// (pinned is a subset of usage, not counted)
	require.Equal(t,
		stats.MemoryUsage.BlockCacheUsage+stats.MemoryUsage.EstimateTableReadersMem+stats.MemoryUsage.CurSizeAllMemTables,
		stats.MemoryUsage.EstimatedMemoryUsage)
}

func TestStatsOpenDBWithCF(t *testing.T) {
	path, err := os.MkdirTemp("", "kvstore_stats_openwithcf")
	require.NoError(t, err)
	defer os.RemoveAll(path)

	db, err := OpenDBWithCF(path, []string{"cf1", "cf2"})
	require.NoError(t, err)
	defer db.Close()

	ins := db.(*instance)
	t1 := db.Table("cf1")
	require.NotNil(t, t1)

	// write only into cf1
	require.NoError(t, t1.Put(KV{Key: []byte("k1"), Value: []byte("v1")}))
	require.NoError(t, t1.Put(KV{Key: []byte("k2"), Value: []byte("v2")}))

	byName := func(s kvstat.Stats) map[string]kvstat.CFStats {
		m := make(map[string]kvstat.CFStats, len(s.ColumnFamilies))
		for _, cf := range s.ColumnFamilies {
			m[cf.Name] = cf
		}
		return m
	}

	// before flush: cf1 memtable has data, default/cf2 are empty
	stats, err := db.Stats()
	require.NoError(t, err)
	require.Len(t, stats.ColumnFamilies, 3) // default + cf1 + cf2
	m := byName(stats)
	require.Contains(t, m, "default")
	require.Contains(t, m, "cf1")
	require.Contains(t, m, "cf2")
	require.Greater(t, m["cf1"].CurSizeAllMemTables, uint64(0))
	require.Greater(t, m["cf1"].CurSizeAllMemTables, m["default"].CurSizeAllMemTables)
	require.Greater(t, m["cf1"].CurSizeAllMemTables, m["cf2"].CurSizeAllMemTables)

	// flush cf1 only; its SST lands in L0 while default stays empty
	require.NoError(t, ins.db.FlushCF(ins.fo, t1.(*table).cf))
	stats, err = db.Stats()
	require.NoError(t, err)
	m = byName(stats)
	require.Greater(t, m["cf1"].NumFilesAtLevel0, uint64(0))
	require.Equal(t, uint64(0), m["default"].NumFilesAtLevel0)
	// DB-level L0 is the max over CFs
	require.Equal(t, m["cf1"].NumFilesAtLevel0, stats.NumFilesAtLevel0)
	// aggregate memtables size is the sum over CFs
	require.Equal(t,
		m["cf1"].CurSizeAllMemTables+m["default"].CurSizeAllMemTables+m["cf2"].CurSizeAllMemTables,
		stats.MemoryUsage.CurSizeAllMemTables)
}

func TestParseProps(t *testing.T) {
	require.Equal(t, uint64(0), parseUintProp(""))
	require.Equal(t, uint64(123), parseUintProp("123"))
	require.Equal(t, uint64(123), parseUintProp(" 123 "))
	require.Equal(t, uint64(0), parseUintProp("not-a-number"))
	require.Equal(t, uint64(0), parseUintProp("-1"))

	require.False(t, parseBoolProp(""))
	require.True(t, parseBoolProp("1"))
	require.False(t, parseBoolProp("0"))
	require.False(t, parseBoolProp("true"))
}
