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

	t.Run("falls back when region has no rw partition", func(t *testing.T) {
		mw := &MetaWrapper{
			partitions: map[uint64]*MetaPartition{
				1: mpEast1, 3: mpWest,
			},
			rwPartitions: []*MetaPartition{mpEast1, mpWest},
		}
		mw.defaultMetaRegion = "unknown-region"
		out := mw.getRWPartitions()
		if len(out) != 2 {
			t.Fatalf("fallback want 2, got %d", len(out))
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
	// nearReadEnabled: same region -> FR && NR; else -> (FR && NR) || RegionReadCfg
	mw := &MetaWrapper{
		FollowerRead:      true,
		NearRead:          true,
		RegionReadCfg:     false,
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
		t.Fatal("other: without NR and RegionReadCfg should be off")
	}
	mw.RegionReadCfg = true
	if !mw.nearReadEnabled(&MetaPartition{Region: "other"}) {
		t.Fatal("other: RegionReadCfg alone should enable")
	}
}
