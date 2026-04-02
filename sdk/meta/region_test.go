// Copyright 2025 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package meta

import (
	"testing"

	"github.com/cubefs/cubefs/proto"
)

func TestMetaWrapperGetRWPartitionsByRegion(t *testing.T) {
	mpEast1 := &MetaPartition{PartitionID: 1, Status: proto.ReadWrite, Region: "east"}
	mpEast2 := &MetaPartition{PartitionID: 2, Status: proto.ReadWrite, Region: "east"}
	mpWest := &MetaPartition{PartitionID: 3, Status: proto.ReadWrite, Region: "west"}
	mpEastRO := &MetaPartition{PartitionID: 4, Status: proto.ReadOnly, Region: "east"}

	t.Run("filters to default meta region when set", func(t *testing.T) {
		mw := &MetaWrapper{
			partitions: map[uint64]*MetaPartition{
				1: mpEast1, 2: mpEast2, 3: mpWest,
			},
			rwPartitions: []*MetaPartition{mpEast1, mpEast2, mpWest},
		}
		mw.defaultMetaRegion = "east"
		out := mw.getRWPartitions()
		if len(out) != 2 {
			t.Fatalf("want 2 east MPs, got %d", len(out))
		}
		for _, mp := range out {
			if mp.Region != "east" || mp.Status != proto.ReadWrite {
				t.Fatalf("unexpected mp: %+v", mp)
			}
		}
	})

	t.Run("read-only in region is excluded from filtered list", func(t *testing.T) {
		mw := &MetaWrapper{
			partitions: map[uint64]*MetaPartition{
				1: mpEast1, 4: mpEastRO,
			},
			rwPartitions: []*MetaPartition{mpEast1, mpEastRO},
		}
		mw.defaultMetaRegion = "east"
		out := mw.getRWPartitions()
		if len(out) != 1 || out[0] != mpEast1 {
			t.Fatalf("want only rw mpEast1, got %v", out)
		}
	})

	t.Run("no rw match in region falls back to full rwPartitions slice", func(t *testing.T) {
		mw := &MetaWrapper{
			partitions: map[uint64]*MetaPartition{
				1: mpEast1, 3: mpWest,
			},
			rwPartitions: []*MetaPartition{mpEast1, mpWest},
		}
		mw.defaultMetaRegion = "unknown-region"
		out := mw.getRWPartitions()
		if len(out) != 2 {
			t.Fatalf("fallback returns full cache, want len 2, got %d", len(out))
		}
		seen := map[uint64]bool{}
		for _, mp := range out {
			seen[mp.PartitionID] = true
		}
		if !seen[1] || !seen[3] {
			t.Fatalf("expected mp 1 and 3 in result, seen=%v", seen)
		}
	})

	t.Run("only read-only in target region falls back to full rwPartitions", func(t *testing.T) {
		mw := &MetaWrapper{
			partitions: map[uint64]*MetaPartition{
				4: mpEastRO, 3: mpWest,
			},
			rwPartitions: []*MetaPartition{mpEastRO, mpWest},
		}
		mw.defaultMetaRegion = "east"
		out := mw.getRWPartitions()
		if len(out) != 2 {
			t.Fatalf("want full cache len 2, got %d", len(out))
		}
	})

	t.Run("empty rwPartitions rebuilds from partitions then applies region filter", func(t *testing.T) {
		mw := &MetaWrapper{
			partitions: map[uint64]*MetaPartition{
				1: mpEast1, 2: mpEast2, 3: mpWest,
			},
			rwPartitions: nil,
		}
		mw.defaultMetaRegion = "east"
		out := mw.getRWPartitions()
		if len(out) != 2 {
			t.Fatalf("want 2 east RW from rebuilt list, got %d", len(out))
		}
	})

	t.Run("empty rwPartitions no region returns all partitions from map", func(t *testing.T) {
		mw := &MetaWrapper{
			partitions: map[uint64]*MetaPartition{
				1: mpEast1, 3: mpWest,
			},
			rwPartitions: nil,
		}
		out := mw.getRWPartitions()
		if len(out) != 2 {
			t.Fatalf("want 2 from partitions map, got %d", len(out))
		}
	})

	t.Run("no region filter returns rw cache as-is", func(t *testing.T) {
		mw := &MetaWrapper{
			rwPartitions: []*MetaPartition{mpEast1, mpWest},
		}
		mw.defaultMetaRegion = ""
		out := mw.getRWPartitions()
		if len(out) != 2 {
			t.Fatalf("got %d", len(out))
		}
	})
}

func TestMetaWrapperSetClientMetaRegion(t *testing.T) {
	mw := &MetaWrapper{}
	mw.SetClientMetaRegion("r-custom")
	if mw.clientMetaRegionCfg != "r-custom" || mw.defaultMetaRegion != "r-custom" {
		t.Fatalf("cfg=%q default=%q", mw.clientMetaRegionCfg, mw.defaultMetaRegion)
	}
}

func TestMetaWrapperNearReadEnabledByRegion(t *testing.T) {
	// nearReadEnabled: same region -> FR && NR; else -> (FR && NR) || RegionNearRead
	mw := &MetaWrapper{
		FollowerRead:      true,
		NearRead:          true,
		RegionNearRead:    false,
		defaultMetaRegion: "home",
	}
	if !mw.nearReadEnabled(&MetaPartition{Region: "home"}) {
		t.Fatal("home: FR+NR should enable")
	}
	if !mw.nearReadEnabled(&MetaPartition{Region: "other"}) {
		t.Fatal("other: FR+NR also enables (see nearReadEnabled boolean precedence)")
	}
	mw.NearRead = false
	if mw.nearReadEnabled(&MetaPartition{Region: "other"}) {
		t.Fatal("other: without NR and RegionNearRead should be off")
	}
	mw.RegionNearRead = true
	if !mw.nearReadEnabled(&MetaPartition{Region: "other"}) {
		t.Fatal("other: RegionNearRead alone should enable")
	}
}
