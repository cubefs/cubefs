// Copyright 2025 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package master

import (
	"sort"
	"testing"

	raftProto "github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	"github.com/cubefs/cubefs/proto"
)

func TestParseMpRegionPolicy(t *testing.T) {
	allowed := []string{"cn-east", "cn-west", "cn-south"}
	baseRegion := "cn-east"

	t.Run("valid single target", func(t *testing.T) {
		p, err := parseMpRegionPolicy("cn-west:rocksdb", allowed, baseRegion)
		if err != nil {
			t.Fatal(err)
		}
		if p == nil || len(p.Learner) != 1 {
			t.Fatalf("Learner map: %+v", p)
		}
		lp := p.Learner["cn-west"]
		if lp == nil || lp.Mode != proto.StoreModeRocksDb {
			t.Fatalf("mode: %+v", lp)
		}
	})

	t.Run("valid multiple targets", func(t *testing.T) {
		p, err := parseMpRegionPolicy("cn-west:memory; cn-south:rocksdb", allowed, baseRegion)
		if err != nil {
			t.Fatal(err)
		}
		if len(p.Learner) != 2 {
			t.Fatalf("want 2 learners, got %+v", p.Learner)
		}
		if p.Learner["cn-west"].Mode != proto.StoreModeMem {
			t.Fatal()
		}
		if p.Learner["cn-south"].Mode != proto.StoreModeRocksDb {
			t.Fatal()
		}
	})

	t.Run("whitespace and case insensitive mode", func(t *testing.T) {
		p, err := parseMpRegionPolicy("  cn-west : ROCKSDB  ; cn-south : Memory ", allowed, baseRegion)
		if err != nil {
			t.Fatal(err)
		}
		if p.Learner["cn-west"].Mode != proto.StoreModeRocksDb || p.Learner["cn-south"].Mode != proto.StoreModeMem {
			t.Fatalf("%+v", p.Learner)
		}
	})

	t.Run("reject duplicate target region", func(t *testing.T) {
		_, err := parseMpRegionPolicy("cn-west:memory;cn-west:rocksdb", allowed, baseRegion)
		if err == nil {
			t.Fatal("expected error for duplicate target region")
		}
	})

	t.Run("empty clears", func(t *testing.T) {
		p, err := parseMpRegionPolicy("empty", allowed, baseRegion)
		if err != nil || p != nil {
			t.Fatalf("p=%v err=%v", p, err)
		}
	})

	t.Run("case insensitive empty clears", func(t *testing.T) {
		for _, s := range []string{"EMPTY", " Empty "} {
			p, err := parseMpRegionPolicy(s, allowed, baseRegion)
			if err != nil || p != nil {
				t.Fatalf("policy %q p=%v err=%v", s, p, err)
			}
		}
	})

	t.Run("reject same region as base", func(t *testing.T) {
		_, err := parseMpRegionPolicy("cn-east:memory", allowed, baseRegion)
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("reject target not allowed", func(t *testing.T) {
		_, err := parseMpRegionPolicy("unknown:memory", allowed, baseRegion)
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("reject invalid mode", func(t *testing.T) {
		_, err := parseMpRegionPolicy("cn-west:invalid", allowed, baseRegion)
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("reject empty segment", func(t *testing.T) {
		_, err := parseMpRegionPolicy("cn-west:memory;;cn-south:memory", allowed, baseRegion)
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("reject missing colon in segment", func(t *testing.T) {
		_, err := parseMpRegionPolicy("cn-west-only", allowed, baseRegion)
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("reject too many colons", func(t *testing.T) {
		_, err := parseMpRegionPolicy("cn-west:rocks:db", allowed, baseRegion)
		if err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestVolIsRegionInAllowed(t *testing.T) {
	v := &Vol{allowedRegions: []string{"default", "r1", "r2"}}
	if !v.isRegionInAllowed("r1") || !v.isRegionInAllowed("default") {
		t.Fatal("expected allowed")
	}
	if v.isRegionInAllowed("r3") || v.isRegionInAllowed("R1") {
		t.Fatal("expected not allowed (case-sensitive)")
	}
	v2 := &Vol{allowedRegions: nil}
	if v2.isRegionInAllowed("default") {
		t.Fatal("nil slice should allow nothing")
	}
}

func TestClusterIsValidRegion(t *testing.T) {
	topo := newTopology()
	zCustom := newZone("zone-custom", proto.MediaType_Unspecified)
	zCustom.MetaRegion = "region-x"
	if err := topo.putZone(zCustom); err != nil {
		t.Fatal(err)
	}
	zDefault := newZone("zone-def", proto.MediaType_Unspecified)
	if err := topo.putZone(zDefault); err != nil {
		t.Fatal(err)
	}
	c := &Cluster{
		ClusterTopoSubItem: ClusterTopoSubItem{t: topo},
	}

	if !c.isValidRegion("region-x") {
		t.Fatal("custom MetaRegion should be valid")
	}
	if !c.isValidRegion(proto.DefaultRegion) {
		t.Fatal("default region from zone should be valid")
	}
	if !c.isValidRegion("") {
		t.Fatal("empty should normalize to default region")
	}
	if c.isValidRegion("no-such-region") {
		t.Fatal("unknown region should be invalid")
	}
}

func TestValidateVolZoneNamesForMetaRegion(t *testing.T) {
	topo := newTopology()
	zEast := newZone("z-east", proto.MediaType_Unspecified)
	zEast.MetaRegion = "east"
	if err := topo.putZone(zEast); err != nil {
		t.Fatal(err)
	}
	zWest := newZone("z-west", proto.MediaType_Unspecified)
	zWest.MetaRegion = "west"
	if err := topo.putZone(zWest); err != nil {
		t.Fatal(err)
	}
	c := &Cluster{ClusterTopoSubItem: ClusterTopoSubItem{t: topo}}

	t.Run("empty zoneName skips", func(t *testing.T) {
		if err := c.validateVolZoneNamesForMetaRegion("  ", "east"); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("single name matches MetaRegion", func(t *testing.T) {
		if err := c.validateVolZoneNamesForMetaRegion("z-east", "east"); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("reject when no listed zone has target MetaRegion", func(t *testing.T) {
		if err := c.validateVolZoneNamesForMetaRegion("z-east", "west"); err == nil {
			t.Fatal("expected error when no zone in list has MetaRegion west")
		}
	})

	t.Run("reject unknown zone name", func(t *testing.T) {
		if err := c.validateVolZoneNamesForMetaRegion("no-such-zone", "east"); err == nil {
			t.Fatal("expected error for unknown zone")
		}
	})

	t.Run("comma list succeeds if any zone matches", func(t *testing.T) {
		if err := c.validateVolZoneNamesForMetaRegion("z-west,z-east", "east"); err != nil {
			t.Fatal(err)
		}
		if err := c.validateVolZoneNamesForMetaRegion("z-east,z-west", "west"); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("comma list fails when none match target", func(t *testing.T) {
		if err := c.validateVolZoneNamesForMetaRegion("z-west,z-east", "north"); err == nil {
			t.Fatal("expected error when target region matches no zone in list")
		}
	})

	t.Run("unknown name in list fails before later match", func(t *testing.T) {
		if err := c.validateVolZoneNamesForMetaRegion("missing,z-east", "east"); err == nil {
			t.Fatal("expected error for unknown zone even if later name would match")
		}
	})

	t.Run("only commas after trim yields error", func(t *testing.T) {
		if err := c.validateVolZoneNamesForMetaRegion(",,", "east"); err == nil {
			t.Fatal("expected error when no resolvable zone names")
		}
	})

	t.Run("empty MetaRegion matches empty target only", func(t *testing.T) {
		topo2 := newTopology()
		z := newZone("z-empty-mr", proto.MediaType_Unspecified)
		z.MetaRegion = ""
		if err := topo2.putZone(z); err != nil {
			t.Fatal(err)
		}
		c2 := &Cluster{ClusterTopoSubItem: ClusterTopoSubItem{t: topo2}}
		if err := c2.validateVolZoneNamesForMetaRegion("z-empty-mr", ""); err != nil {
			t.Fatal(err)
		}
		if err := c2.validateVolZoneNamesForMetaRegion("z-empty-mr", proto.DefaultRegion); err == nil {
			t.Fatal("empty zone MetaRegion is not equal to default region string unless normalized by caller")
		}
	})
}

func TestClusterGetRegionFromMetaNodeAddr(t *testing.T) {
	c := &Cluster{}
	addr := "192.168.1.10:17210"
	c.metaNodes.Store(addr, &MetaNode{Addr: addr, Region: "az-1"})
	if got := c.getRegionFromMetaNodeAddr(addr); got != "az-1" {
		t.Fatalf("got %q", got)
	}
	if got := c.getRegionFromMetaNodeAddr("not-registered:17210"); got != proto.DefaultRegion {
		t.Fatalf("unknown addr should return default, got %q", got)
	}
}

func TestVolGetMpRegionPolicyStatus(t *testing.T) {
	const westAddr = "10.0.0.20:17210"
	const westProgressAddr = "10.0.0.21:17210"

	c := &Cluster{}
	c.metaNodes.Store(westAddr, &MetaNode{Addr: westAddr, Region: "west"})
	c.metaNodes.Store(westProgressAddr, &MetaNode{Addr: westProgressAddr, Region: "west"})

	vol := &Vol{
		Name:           "testvol",
		MetaPartitions: make(map[uint64]*MetaPartition),
		mpPolicy: map[string]*proto.VolMpPolicy{
			"east": {
				Name: "east",
				Learner: map[string]*proto.LearnerPolicy{
					"west": {Mode: proto.StoreModeMem},
				},
			},
		},
	}
	// mpsLock must be non-nil for getMpRegionPolicyStatus; avoid newMpsLockManager (background goroutine).
	vol.mpsLock = &mpsLockManager{}

	// MP 1: no learner toward west -> Remaining
	mp1 := newMetaPartition(1001, 0, 100, 3, vol.Name, 1, 0)
	mp1.Region = "east"
	mp1.Peers = []proto.Peer{
		{Addr: "10.0.0.1:17210", ID: 1},
	}
	vol.MetaPartitions[1001] = mp1

	// MP 2: manual-promote learner on west node -> Completed
	mp2 := newMetaPartition(1002, 101, 200, 3, vol.Name, 1, 0)
	mp2.Region = "east"
	mp2.Peers = []proto.Peer{
		{Addr: "10.0.0.1:17210", ID: 1},
		{Addr: westAddr, ID: 2, Type: raftProto.PeerLearner, ManualPromote: true},
	}
	vol.MetaPartitions[1002] = mp2

	// MP 3: in-progress learner (RecoverDst set, RecoverSrc empty) on west
	mp3 := newMetaPartition(1003, 201, 300, 3, vol.Name, 1, 0)
	mp3.Region = "east"
	mp3.Peers = []proto.Peer{{Addr: "10.0.0.1:17210", ID: 1}}
	mp3.RecoverLearners = []*proto.RecoverPair{
		{RecoverDst: westProgressAddr, RecoverSrc: ""},
	}
	vol.MetaPartitions[1003] = mp3

	statuses := vol.getMpRegionPolicyStatus(c)
	if len(statuses) != 1 {
		t.Fatalf("expected one status row for east, got %d", len(statuses))
	}
	st := statuses[0]
	if st.Region != "east" || st.TotalMp != 3 {
		t.Fatalf("region/total: %+v", st)
	}
	ws, ok := st.LearnerStatuses["west"]
	if !ok || ws == nil {
		t.Fatalf("missing west status: %+v", st.LearnerStatuses)
	}
	if ws.Completed != 1 || ws.InProgress != 1 || ws.Remaining != 1 {
		t.Fatalf("counts: completed=%d inProgress=%d remaining=%d", ws.Completed, ws.InProgress, ws.Remaining)
	}
	sort.Slice(ws.RemainingMpIds, func(i, j int) bool { return ws.RemainingMpIds[i] < ws.RemainingMpIds[j] })
	sort.Slice(ws.InProgressMpIds, func(i, j int) bool { return ws.InProgressMpIds[i] < ws.InProgressMpIds[j] })
	if len(ws.RemainingMpIds) != 1 || ws.RemainingMpIds[0] != 1001 {
		t.Fatalf("remaining ids: %v", ws.RemainingMpIds)
	}
	if len(ws.InProgressMpIds) != 1 || ws.InProgressMpIds[0] != 1003 {
		t.Fatalf("inProgress ids: %v", ws.InProgressMpIds)
	}

	// Region without policy: still reported with TotalMp, no per-target breakdown
	vol2 := &Vol{
		Name:           "v2",
		MetaPartitions: make(map[uint64]*MetaPartition),
		mpPolicy:       nil,
	}
	vol2.mpsLock = &mpsLockManager{}
	mpNorth := newMetaPartition(2001, 0, 50, 3, vol2.Name, 1, 0)
	mpNorth.Region = "north"
	vol2.MetaPartitions[2001] = mpNorth
	st2 := vol2.getMpRegionPolicyStatus(c)
	if len(st2) != 1 || st2[0].Region != "north" || st2[0].TotalMp != 1 {
		t.Fatalf("no-policy status: %+v", st2)
	}
	if st2[0].LearnerStatuses != nil && len(st2[0].LearnerStatuses) > 0 {
		t.Fatalf("expected no learner status map for region without policy")
	}
}
