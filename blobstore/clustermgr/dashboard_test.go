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

package clustermgr

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

func newTestDashboardMgr() *dashboardMgr {
	d := &dashboardMgr{
		freshCh: make(chan struct{}, 1),
	}
	d.snapshot.Store(&dashboardSnapshot{})
	return d
}

func TestDashboardRefresh_IdleTriggersSignal(t *testing.T) {
	d := newTestDashboardMgr()

	ch := d.Refresh()
	require.NotNil(t, ch)

	select {
	case <-d.freshCh:
	default:
		t.Fatal("expected freshCh to have a pending signal")
	}

	d.processLock.Lock()
	require.NotNil(t, d.processCh) // processCh is set
	d.processLock.Unlock()
}

func TestDashboardRefresh_InFlightSharesChannel(t *testing.T) {
	d := newTestDashboardMgr()

	ch1 := d.Refresh()
	ch2 := d.Refresh()
	require.Equal(t, ch1, ch2)

	// freshCh must have exactly one signal (not two)
	count := 0
	for {
		select {
		case <-d.freshCh:
			count++
		default:
			goto done
		}
	}
done:
	require.Equal(t, 1, count)
}

func TestDashboardRefresh_CooldownReturnsClosedChannel(t *testing.T) {
	d := newTestDashboardMgr()

	// Manually set a future cooldown and nil processCh
	d.processLock.Lock()
	d.cooldownUntil = time.Now().Add(10 * time.Second)
	d.processLock.Unlock()

	ch := d.Refresh()

	select {
	case <-ch:
	default:
		t.Fatal("expected a pre-closed channel during cooldown")
	}

	select {
	case <-d.freshCh:
		t.Fatal("freshCh should not be signaled during cooldown")
	default:
	}
}

func TestDashboardRefresh_AfterCooldownExpiry(t *testing.T) {
	d := newTestDashboardMgr()

	d.processLock.Lock()
	d.cooldownUntil = time.Now().Add(-1 * time.Second) // Expired cooldown
	d.processLock.Unlock()

	ch := d.Refresh()

	select {
	case <-d.freshCh:
	default:
		t.Fatal("expected freshCh signal after cooldown expired")
	}

	select {
	case <-ch:
		t.Fatal("channel should not be closed yet")
	default:
	}
}

func TestDashboardFresh_UpdatesSnapshotAndNotifiesWaiters(t *testing.T) {
	svc, cleanup := initTestService(t)
	defer cleanup()

	d := svc.dashboardMgr

	ch := d.Refresh()
	select {
	case <-d.freshCh:
	default:
	}

	t1 := time.Now()
	d.fresh(context.Background(), true)

	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("waiter not unblocked after fresh()")
	}

	// Snapshot GeneratedAt must be after t1
	snap := d.GetSnapshot()
	require.Greater(t, snap.dashboard.GeneratedAt, t1.UnixNano())

	d.processLock.Lock()
	require.Nil(t, d.processCh)
	d.processLock.Unlock()

	d.processLock.Lock()
	require.True(t, time.Now().Before(d.cooldownUntil))
	d.processLock.Unlock()
}

func TestDashboardFresh_NoPanicWhenNoWaiter(t *testing.T) {
	svc, cleanup := initTestService(t)
	defer cleanup()
	require.NotPanics(t, func() {
		svc.dashboardMgr.fresh(context.Background(), false)
	})
}

func TestDashboardRefresh_ConcurrentCallersShareResult(t *testing.T) {
	d := newTestDashboardMgr()

	const n = 50
	var wg sync.WaitGroup
	channels := make([](<-chan struct{}), n)

	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			channels[i] = d.Refresh()
		}()
	}
	wg.Wait()

	// All goroutines must have received a non-nil channel.
	for i, ch := range channels {
		require.NotNil(t, ch, "channel[%d] is nil", i)
	}

	// After fresh() closes processCh all channels must become readable.
	d.processLock.Lock()
	ch := d.processCh
	d.processCh = nil
	d.processLock.Unlock()
	if ch != nil {
		close(ch)
	}

	for i, ch := range channels {
		select {
		case <-ch:
		case <-time.After(time.Second):
			t.Fatalf("channel[%d] not readable after close", i)
		}
	}
}

// buildScope()

func TestDashboardBuildScope_EmptyScopes(t *testing.T) {
	svc, cleanup := initTestService(t)
	defer cleanup()
	result := buildScope(svc.ScopeMgr.Stat())
	require.Equal(t, clustermgr.DashboardScoreOK, result.Score.Score)
	require.Empty(t, result.Scopes)
}

func TestDashboardBuildScope_SortedAndCorrectMaxValue(t *testing.T) {
	fakeScopes := map[string]uint64{
		"vid":            10,
		"diskid":         5,
		BidScopeName:     100,
		SpaceIDScopeName: 200,
		"nodeid":         3,
	}
	result := buildScope(fakeScopes)

	for i := 1; i < len(result.Scopes); i++ {
		require.Less(t, result.Scopes[i-1].Name, result.Scopes[i].Name,
			"scopes not sorted at index %d", i)
	}

	for _, s := range result.Scopes {
		if s.Name == BidScopeName || s.Name == SpaceIDScopeName {
			require.Equal(t, uint64(math.MaxUint64), s.MaxValue, "scope %s", s.Name)
		} else {
			require.Equal(t, uint64(math.MaxUint32), s.MaxValue, "scope %s", s.Name)
		}
	}
}

func TestDashboardBuildScope_ScoreOKWhenBelowHalf(t *testing.T) {
	result := buildScope(map[string]uint64{
		"vid":    100, // MaxUint32/2 = 2147483647; 100 is far below half
		"diskid": 1000,
	})
	require.Equal(t, clustermgr.DashboardScoreOK, result.Score.Score)
}

func TestDashboardBuildScope_ScoreNoticeWhenAboveHalf(t *testing.T) {
	result := buildScope(map[string]uint64{
		"vid":    math.MaxUint32/2 + 1, // just above half → Notice
		"diskid": 100,
	})
	require.Equal(t, clustermgr.DashboardScoreNotice, result.Score.Score)
}

func TestDashboardBuildScope_BidScopeNoOverflow(t *testing.T) {
	require.Equal(t, clustermgr.DashboardScoreOK,
		buildScope(map[string]uint64{BidScopeName: math.MaxUint64 / 2}).Score.Score)
	require.Equal(t, clustermgr.DashboardScoreNotice,
		buildScope(map[string]uint64{BidScopeName: math.MaxUint64/2 + 1}).Score.Score)
}

// buildDisk

func genDisk(nodeID proto.NodeID, path string, diskID proto.DiskID,
	status proto.DiskStatus, idc string, hb clustermgr.DiskHeartBeatInfo,
) clustermgr.BlobNodeDiskInfo {
	hb.DiskID = diskID
	return clustermgr.BlobNodeDiskInfo{
		DiskInfo: clustermgr.DiskInfo{
			NodeID: nodeID,
			Path:   path,
			Idc:    idc,
			Status: status,
		},
		DiskHeartBeatInfo: hb,
	}
}

func TestBuildDisk_Empty(t *testing.T) {
	stat := buildDisk(nil, nil)
	require.Empty(t, stat.ByStatusIDC)
	require.Equal(t, clustermgr.DashboardScoreOK, stat.Score.Score)
}

func TestBuildDisk_NormalDisk(t *testing.T) {
	// A single normal disk: appears in "normal" and "__total__"; no other keys.
	snaps := []clustermgr.BlobNodeDiskInfo{
		genDisk(1, "/data0", 10, proto.DiskStatusNormal, "idc1",
			clustermgr.DiskHeartBeatInfo{Size: 1000, Used: 200, Free: 800, MaxChunkCnt: 10, FreeChunkCnt: 8, UsedChunkCnt: 2}),
	}
	stat := buildDisk(snaps, nil)

	require.Equal(t, 1, stat.ByStatusIDC["normal"]["idc1"].Count)
	require.Equal(t, int64(1000), stat.ByStatusIDC["normal"]["idc1"].TotalBytes)
	require.Equal(t, 1, stat.ByStatusIDC["__total__"]["idc1"].Count)
	require.Nil(t, stat.ByStatusIDC["dropped"])
	require.Nil(t, stat.ByStatusIDC["__replace__"])
}

func TestBuildDisk_DroppedExcludedFromTotal(t *testing.T) {
	// Dropped disks go into "dropped" bucket only — excluded from __total__.
	snaps := []clustermgr.BlobNodeDiskInfo{
		genDisk(1, "/data0", 10, proto.DiskStatusNormal, "idc1", clustermgr.DiskHeartBeatInfo{Size: 100}),
		genDisk(2, "/data0", 20, proto.DiskStatusDropped, "idc1", clustermgr.DiskHeartBeatInfo{Size: 100}),
	}
	stat := buildDisk(snaps, nil)

	require.Equal(t, 1, stat.ByStatusIDC["normal"]["idc1"].Count)
	require.Equal(t, 1, stat.ByStatusIDC["dropped"]["idc1"].Count)
	require.Equal(t, 1, stat.ByStatusIDC["__total__"]["idc1"].Count, "__total__ must not count dropped")
	require.Nil(t, stat.ByStatusIDC["repaired"])
}

func TestBuildDisk_RepairedBecomesReplace(t *testing.T) {
	// A repaired disk maps to "__replace__" (physical drive not yet swapped) and "__total__".
	// It must NOT appear under "repaired".
	snaps := []clustermgr.BlobNodeDiskInfo{
		genDisk(1, "/data0", 10, proto.DiskStatusRepaired, "idc1", clustermgr.DiskHeartBeatInfo{Size: 500}),
	}
	stat := buildDisk(snaps, nil)

	require.Equal(t, 1, stat.ByStatusIDC["__replace__"]["idc1"].Count)
	require.Equal(t, 1, stat.ByStatusIDC["__total__"]["idc1"].Count)
	require.Nil(t, stat.ByStatusIDC["repaired"], "repaired key must not exist")
}

func TestBuildDisk_SlotDedup_KeepsHighestDiskID(t *testing.T) {
	// Same (NodeID, Path): only the disk with the highest DiskID is "current".
	// Slot /data0 on node 1: disk 5 (broken) replaced by disk 10 (normal) — only disk 10 counts.
	snaps := []clustermgr.BlobNodeDiskInfo{
		genDisk(1, "/data0", 5, proto.DiskStatusBroken, "idc1", clustermgr.DiskHeartBeatInfo{Size: 100}),
		genDisk(1, "/data0", 10, proto.DiskStatusNormal, "idc1", clustermgr.DiskHeartBeatInfo{Size: 200}),
	}
	stat := buildDisk(snaps, nil)

	require.Equal(t, 1, stat.ByStatusIDC["normal"]["idc1"].Count)
	require.Equal(t, int64(200), stat.ByStatusIDC["normal"]["idc1"].TotalBytes)
	require.Nil(t, stat.ByStatusIDC["broken"], "old broken disk must be ignored")
	require.Equal(t, 1, stat.ByStatusIDC["__total__"]["idc1"].Count)
}

func TestBuildDisk_SlotDedup_OldNormalNewRepaired(t *testing.T) {
	// Physical drive still in bay after repair: disk 10 (normal) → disk 20 (repaired).
	// Slot is current=repaired → __replace__ + __total__.
	snaps := []clustermgr.BlobNodeDiskInfo{
		genDisk(1, "/data0", 10, proto.DiskStatusNormal, "idc1", clustermgr.DiskHeartBeatInfo{Size: 100}),
		genDisk(1, "/data0", 20, proto.DiskStatusRepaired, "idc1", clustermgr.DiskHeartBeatInfo{Size: 200}),
	}
	stat := buildDisk(snaps, nil)

	require.Equal(t, 1, stat.ByStatusIDC["__replace__"]["idc1"].Count)
	require.Equal(t, int64(200), stat.ByStatusIDC["__replace__"]["idc1"].TotalBytes)
	require.Equal(t, 1, stat.ByStatusIDC["__total__"]["idc1"].Count)
	require.Nil(t, stat.ByStatusIDC["normal"])
}

func TestBuildDisk_MultiStatusMultiIDC(t *testing.T) {
	// Mix of statuses across two IDCs; verify per-IDC counts.
	snaps := []clustermgr.BlobNodeDiskInfo{
		genDisk(1, "/d0", 1, proto.DiskStatusNormal, "idc1", clustermgr.DiskHeartBeatInfo{Size: 100}),
		genDisk(2, "/d0", 2, proto.DiskStatusBroken, "idc1", clustermgr.DiskHeartBeatInfo{Size: 100}),
		genDisk(3, "/d0", 3, proto.DiskStatusNormal, "idc2", clustermgr.DiskHeartBeatInfo{Size: 100}),
		genDisk(4, "/d0", 4, proto.DiskStatusDropped, "idc2", clustermgr.DiskHeartBeatInfo{Size: 100}),
	}
	stat := buildDisk(snaps, nil)

	require.Equal(t, 1, stat.ByStatusIDC["normal"]["idc1"].Count)
	require.Equal(t, 1, stat.ByStatusIDC["broken"]["idc1"].Count)
	require.Equal(t, 2, stat.ByStatusIDC["__total__"]["idc1"].Count)

	require.Equal(t, 1, stat.ByStatusIDC["normal"]["idc2"].Count)
	require.Equal(t, 1, stat.ByStatusIDC["dropped"]["idc2"].Count)
	require.Equal(t, 1, stat.ByStatusIDC["__total__"]["idc2"].Count, "dropped must not be in __total__")
}

func TestBuildDisk_ByteAggregation(t *testing.T) {
	// Verify that byte and chunk fields are summed correctly across multiple disks in same bucket.
	snaps := []clustermgr.BlobNodeDiskInfo{
		genDisk(1, "/d0", 1, proto.DiskStatusNormal, "idc1",
			clustermgr.DiskHeartBeatInfo{Size: 1000, Used: 300, Free: 700, MaxChunkCnt: 10, FreeChunkCnt: 7, UsedChunkCnt: 3, OversoldFreeChunkCnt: 1}),
		genDisk(2, "/d0", 2, proto.DiskStatusNormal, "idc1",
			clustermgr.DiskHeartBeatInfo{Size: 2000, Used: 500, Free: 1500, MaxChunkCnt: 20, FreeChunkCnt: 15, UsedChunkCnt: 5, OversoldFreeChunkCnt: 2}),
	}
	stat := buildDisk(snaps, nil)

	e := stat.ByStatusIDC["normal"]["idc1"]
	require.Equal(t, 2, e.Count)
	require.Equal(t, int64(3000), e.TotalBytes)
	require.Equal(t, int64(800), e.UsedBytes)
	require.Equal(t, int64(2200), e.FreeBytes)
	require.Equal(t, int64(30), e.MaxChunks)
	require.Equal(t, int64(22), e.FreeChunks)
	require.Equal(t, int64(8), e.UsedChunks)
	require.Equal(t, int64(3), e.OversoldChunks)
}

func TestBuildDisk_TotalEqualsNormalPlusBrokenPlusRepairingPlusReplace(t *testing.T) {
	// __total__ = normal + broken + repairing + __replace__ (dropped excluded)
	snaps := []clustermgr.BlobNodeDiskInfo{
		genDisk(1, "/d0", 1, proto.DiskStatusNormal, "idc1", clustermgr.DiskHeartBeatInfo{}),
		genDisk(2, "/d1", 2, proto.DiskStatusBroken, "idc1", clustermgr.DiskHeartBeatInfo{}),
		genDisk(3, "/d2", 3, proto.DiskStatusRepairing, "idc1", clustermgr.DiskHeartBeatInfo{}),
		genDisk(4, "/d3", 4, proto.DiskStatusRepaired, "idc1", clustermgr.DiskHeartBeatInfo{}),
		genDisk(5, "/d4", 5, proto.DiskStatusDropped, "idc1", clustermgr.DiskHeartBeatInfo{}),
	}
	stat := buildDisk(snaps, nil)

	total := stat.ByStatusIDC["__total__"]["idc1"].Count
	normal := stat.ByStatusIDC["normal"]["idc1"].Count
	broken := stat.ByStatusIDC["broken"]["idc1"].Count
	repairing := stat.ByStatusIDC["repairing"]["idc1"].Count
	replace := stat.ByStatusIDC["__replace__"]["idc1"].Count

	require.Equal(t, normal+broken+repairing+replace, total)
	require.Equal(t, 4, total)
}

func TestBuildDisk_NodeDroppedOverridesDiskStatus(t *testing.T) {
	// node 1 is Dropped; its disks should all be counted as "dropped"
	// regardless of their own status.
	droppedNodes := map[proto.NodeID]struct{}{proto.NodeID(1): {}}
	snaps := []clustermgr.BlobNodeDiskInfo{
		genDisk(1, "/d0", 1, proto.DiskStatusNormal, "idc1", clustermgr.DiskHeartBeatInfo{}), // node dropped → dropped
		genDisk(1, "/d1", 2, proto.DiskStatusBroken, "idc1", clustermgr.DiskHeartBeatInfo{}), // node dropped → dropped
		genDisk(2, "/d2", 3, proto.DiskStatusNormal, "idc1", clustermgr.DiskHeartBeatInfo{}), // normal node → normal
	}
	stat := buildDisk(snaps, droppedNodes)

	require.Equal(t, 2, stat.ByStatusIDC["dropped"]["idc1"].Count)
	require.Equal(t, 1, stat.ByStatusIDC["normal"]["idc1"].Count)
	// dropped disks are not added to __total__
	require.Equal(t, 1, stat.ByStatusIDC["__total__"]["idc1"].Count)
}

func TestDashboardDisk_EmptyOnFreshService(t *testing.T) {
	svc, cleanup := initTestService(t)
	defer cleanup()

	// Trigger a fresh snapshot and wait for it.
	ch := svc.dashboardMgr.Refresh()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("refresh timed out")
	}

	snap := svc.dashboardMgr.GetSnapshot()
	// No disks registered → Disk.Score is OK.
	require.Equal(t, clustermgr.DashboardScoreOK, snap.dashboard.Disk.Score.Score)
	// No service nodes registered → PROXY/WORKER/BLOBNODE all absent → Service.Score is Major.
	require.Equal(t, clustermgr.DashboardScoreMajor, snap.dashboard.Service.Score.Score)
	// No volumes at all → ActiveTotal==0 && IdleTotal==0 → Volume.Score is Critical.
	require.Equal(t, clustermgr.DashboardScoreCritical, snap.dashboard.Volume.Score.Score)
	// Overall score is dominated by Volume (Critical > Major).
	require.Equal(t, clustermgr.DashboardScoreCritical, snap.dashboard.Score.Score)
}

func TestDashboardGetSnapshot_NeverNil(t *testing.T) {
	d := newTestDashboardMgr()
	require.NotNil(t, d.GetSnapshot())
}

func TestDashboardLoopFresh_ForceRefresh(t *testing.T) {
	svc, cleanup := initTestService(t)
	defer cleanup()

	d := svc.dashboardMgr

	origAt := d.snapshot.Load().(*dashboardSnapshot).dashboard.GeneratedAt

	ch := d.Refresh()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("force refresh timed out")
	}

	snap := d.GetSnapshot()
	require.Greater(t, snap.dashboard.GeneratedAt, origAt)
}

// buildService

func svcNode(name, idc, host string, expireAt int64) clustermgr.ServiceNode {
	return clustermgr.ServiceNode{Name: name, Idc: idc, Host: host, ExpireAt: expireAt}
}

// noHost is a stub getHost for tests that don't involve disk expiry.
func noHost(proto.NodeID) string { return "" }

func TestBuildService_Empty(t *testing.T) {
	s := buildService(nil, nil, noHost)
	require.Nil(t, s.OfflineNodes)
	require.Empty(t, s.OnlineByTypeIDC)
	require.Equal(t, 0, s.ExpiredDisks)
	require.Nil(t, s.ExpiredByNode)
}

func TestBuildService_AllOnline(t *testing.T) {
	now := time.Now()
	services := []clustermgr.ServiceNode{
		svcNode(proto.ServiceNameProxy, "idc1", "h1", now.Add(60*time.Second).Unix()),
		svcNode(proto.ServiceNameProxy, "idc1", "h2", now.Add(60*time.Second).Unix()),
		svcNode(proto.ServiceNameScheduler, "idc1", "h1", now.Add(60*time.Second).Unix()),
		svcNode(proto.ServiceNameWorker, "idc1", "h1", now.Add(60*time.Second).Unix()),
		svcNode(proto.ServiceNameWorker, "idc1", "h2", now.Add(60*time.Second).Unix()),
		svcNode(proto.ServiceNameBlobNode, "idc1", "h1", now.Add(60*time.Second).Unix()),
		svcNode(proto.ServiceNameBlobNode, "idc1", "h2", now.Add(60*time.Second).Unix()),
	}
	s := buildService(services, nil, noHost)
	require.Equal(t, clustermgr.DashboardScoreOK, s.Score.Score)
	require.Nil(t, s.OfflineNodes)
}

func TestBuildService_SomeOffline(t *testing.T) {
	now := time.Now()
	services := []clustermgr.ServiceNode{
		svcNode(proto.ServiceNameProxy, "idc1", "h1", now.Add(60*time.Second).Unix()),
		svcNode(proto.ServiceNameProxy, "idc1", "h2", now.Add(-1*time.Second).Unix()), // offline
		svcNode(proto.ServiceNameScheduler, "idc1", "h1", now.Add(60*time.Second).Unix()),
		svcNode(proto.ServiceNameWorker, "idc1", "h1", now.Add(60*time.Second).Unix()),
		svcNode(proto.ServiceNameBlobNode, "idc1", "h1", now.Add(60*time.Second).Unix()),
	}
	s := buildService(services, nil, noHost)
	require.Equal(t, clustermgr.DashboardScoreWarning, s.Score.Score) // proxy idc1: 1 online → Warning
	require.Equal(t, 1, s.OnlineByTypeIDC[proto.ServiceNameProxy]["idc1"])
	require.Len(t, s.OfflineNodes, 1)
}

func TestBuildService_OfflineNodes(t *testing.T) {
	now := time.Now()
	services := []clustermgr.ServiceNode{
		svcNode("sched", "idc1", "h1", now.Add(30*time.Second).Unix()),
		svcNode("sched", "idc1", "h2", now.Add(-1*time.Second).Unix()), // offline
	}
	s := buildService(services, nil, noHost)
	require.Len(t, s.OfflineNodes, 1)
	require.Equal(t, "h2", s.OfflineNodes[0].Host)
}

func TestBuildService_ExpiredDisksGroupedByHost(t *testing.T) {
	hosts := map[proto.NodeID]string{1: "h1", 2: "h2"}
	getHost := func(id proto.NodeID) string { return hosts[id] }
	expired := []clustermgr.BlobNodeDiskInfo{
		{DiskInfo: clustermgr.DiskInfo{NodeID: 1}, DiskHeartBeatInfo: clustermgr.DiskHeartBeatInfo{DiskID: 1}}, // node 1 → h1
		{DiskInfo: clustermgr.DiskInfo{NodeID: 2}, DiskHeartBeatInfo: clustermgr.DiskHeartBeatInfo{DiskID: 3}}, // node 2 → h2
	}
	s := buildService(nil, expired, getHost)
	require.Equal(t, 2, s.ExpiredDisks)
	require.ElementsMatch(t, []proto.DiskID{1}, s.ExpiredByNode["h1"])
	require.ElementsMatch(t, []proto.DiskID{3}, s.ExpiredByNode["h2"])
}

func TestBuildService_NoExpiredDisks(t *testing.T) {
	s := buildService(nil, nil, noHost)
	require.Equal(t, 0, s.ExpiredDisks)
	require.Nil(t, s.ExpiredByNode)
}

// buildVolume

// volMap converts a VolumeBasic slice into the map expected by buildVolume.
func volMap(vols []clustermgr.VolumeBasic) map[proto.Vid]*clustermgr.VolumeBasic {
	m := make(map[proto.Vid]*clustermgr.VolumeBasic, len(vols))
	for i := range vols {
		m[proto.Vid(i+1)] = &vols[i]
	}
	return m
}

func TestBuildVolume_Empty(t *testing.T) {
	// No volumes → ActiveTotal==0 && IdleTotal==0 → Critical (cluster has no usable volumes).
	v := buildVolume(volMap(nil), 1<<30, 0, 0)
	require.Equal(t, clustermgr.DashboardScoreCritical, v.Score.Score)
	require.Equal(t, 0, v.Status.ActiveTotal)
	require.Equal(t, 0, v.Status.IdleTotal)
	require.Empty(t, v.ByScore)
	require.Empty(t, v.ByFree)
	require.Empty(t, v.AllocatableByScore)
	require.Empty(t, v.AllocatableByFree)
	require.Empty(t, v.TopDiskLoad)
}

func TestBuildVolume_ByScore(t *testing.T) {
	vols := []clustermgr.VolumeBasic{
		{CodeMode: codemode.EC6P6, Score: 0, Free: 100, Used: 200, Total: 300, Status: proto.VolumeStatusActive},
		{CodeMode: codemode.EC6P6, Score: -1, Free: 50, Used: 250, Total: 300, Status: proto.VolumeStatusActive},
		{CodeMode: codemode.EC6P6, Score: 0, Free: 80, Used: 120, Total: 200, Status: proto.VolumeStatusIdle},
	}
	v := buildVolume(volMap(vols), 1<<30, 0, 0)
	require.Equal(t, 2, v.Status.ActiveTotal)
	require.Equal(t, 1, v.Status.ActiveHealthy)
	require.Equal(t, 1, v.Status.ActiveUnhealthy)
	require.Equal(t, 1, v.Status.IdleTotal)
	// score is now disk-load based; threshold=0 → always OK
	require.Equal(t, clustermgr.DashboardScoreOK, v.Score.Score)

	ec6p6Name := codemode.EC6P6.String()
	require.Equal(t, 2, v.ByScore[ec6p6Name][0].Count)  // active score=0 + idle score=0
	require.Equal(t, 1, v.ByScore[ec6p6Name][-1].Count) // active score=-1
}

func TestBuildVolume_ScoreOKWhenThresholdZero(t *testing.T) {
	d1, d2 := proto.DiskID(1), proto.DiskID(2)
	vols := []clustermgr.VolumeBasic{
		{CodeMode: codemode.EC6P6, Score: 0, Status: proto.VolumeStatusActive, DiskIDs: []proto.DiskID{d1}},
		{CodeMode: codemode.EC6P6, Score: 0, Status: proto.VolumeStatusActive, DiskIDs: []proto.DiskID{d1}},
		// Must have at least one Idle volume; otherwise CalcScore → Critical.
		{CodeMode: codemode.EC6P6, Score: 0, Status: proto.VolumeStatusIdle, DiskIDs: []proto.DiskID{d2}},
	}
	// threshold=0 → CalcScore returns OK (cluster has both Active and Idle volumes).
	v := buildVolume(volMap(vols), 1<<30, 0, 0)
	require.Equal(t, clustermgr.DashboardScoreOK, v.Score.Score)
}

func TestBuildVolume_ScoreDiskLoad(t *testing.T) {
	d1, d2 := proto.DiskID(1), proto.DiskID(2)
	// disk1 hosts 5 active volume units (load=5); disk2 hosts one idle unit.
	// Both Active and Idle are required so CalcScore proceeds past the Critical guard.
	vols := make([]clustermgr.VolumeBasic, 5)
	for i := range vols {
		vols[i] = clustermgr.VolumeBasic{
			CodeMode: codemode.EC6P6,
			Score:    0,
			Status:   proto.VolumeStatusActive,
			DiskIDs:  []proto.DiskID{d1},
		}
	}
	vols = append(vols, clustermgr.VolumeBasic{
		CodeMode: codemode.EC6P6,
		Score:    0,
		Status:   proto.VolumeStatusIdle,
		DiskIDs:  []proto.DiskID{d2},
	})

	// threshold=10: load(5) ≤ 10 → OK
	v := buildVolume(volMap(vols), 1<<30, 0, 10)
	require.Equal(t, clustermgr.DashboardScoreOK, v.Score.Score)

	// threshold=4: load(5) > 4 → Warning
	v = buildVolume(volMap(vols), 1<<30, 0, 4)
	require.Equal(t, clustermgr.DashboardScoreWarning, v.Score.Score)

	// threshold=2: load(5) > 2×2=4 → Major
	v = buildVolume(volMap(vols), 1<<30, 0, 2)
	require.Equal(t, clustermgr.DashboardScoreMajor, v.Score.Score)
}

func TestBuildVolume_Allocatable(t *testing.T) {
	const threshold = uint64(1 << 30)
	vols := []clustermgr.VolumeBasic{
		{CodeMode: codemode.EC6P6, Score: 0, Free: threshold + 1, Status: proto.VolumeStatusIdle},   // ✓ allocatable
		{CodeMode: codemode.EC6P6, Score: -1, Free: threshold + 1, Status: proto.VolumeStatusIdle},  // ✗ score < retainThreshold(0)
		{CodeMode: codemode.EC6P6, Score: 0, Free: threshold - 1, Status: proto.VolumeStatusIdle},   // ✗ free too small
		{CodeMode: codemode.EC6P6, Score: 0, Free: threshold + 1, Status: proto.VolumeStatusActive}, // ✗ not idle
	}
	v := buildVolume(volMap(vols), threshold, 0, 0)
	ec6p6Name := codemode.EC6P6.String()
	require.Equal(t, 1, v.AllocatableByScore[ec6p6Name][0].Count)
}

func TestBuildVolume_FreeRatioLabel(t *testing.T) {
	cases := []struct {
		free, used uint64
		want       string
	}{
		{0, 0, "99"},   // empty volume → last bucket
		{0, 100, "10"}, // 0% free → idx=0 → "10"
		{100, 0, "99"}, // 100% free → idx=9 → "99"
		{50, 50, "60"}, // 50% → idx=5 → "60"
		{89, 11, "90"}, // 89% → idx=8 → "90"
		{90, 10, "99"}, // 90% → idx=9 → "99"
	}
	for _, c := range cases {
		got := volumeFreeRatioLabel(c.free, c.used)
		require.Equal(t, c.want, got, "free=%d used=%d", c.free, c.used)
	}
}

func TestBuildVolume_TopDiskLoad(t *testing.T) {
	d1, d2, d3 := proto.DiskID(1), proto.DiskID(2), proto.DiskID(3)
	vols := []clustermgr.VolumeBasic{
		// 3 active EC6P6 volumes; disk1 appears in all 3, disk2 and disk3 once each
		{CodeMode: codemode.EC6P6, Score: 0, Status: proto.VolumeStatusActive, DiskIDs: []proto.DiskID{d1, d2}},
		{CodeMode: codemode.EC6P6, Score: 0, Status: proto.VolumeStatusActive, DiskIDs: []proto.DiskID{d1, d3}},
		{CodeMode: codemode.EC6P6, Score: 0, Status: proto.VolumeStatusActive, DiskIDs: []proto.DiskID{d1}},
		{CodeMode: codemode.EC6P6, Score: 0, Status: proto.VolumeStatusIdle, DiskIDs: []proto.DiskID{d1}}, // idle: not counted
	}
	v := buildVolume(volMap(vols), 1<<30, 0, 0)
	require.Len(t, v.TopDiskLoad, 2) // one per-codemode + global summary

	perMode := v.TopDiskLoad[0]
	require.Equal(t, codemode.EC6P6.String(), perMode.CodeMode)
	require.Equal(t, 3, perMode.Total)
	require.Equal(t, d1, perMode.TopN[0].DiskID) // disk1 has load 3
	require.Equal(t, 3, perMode.TopN[0].Load)

	global := v.TopDiskLoad[len(v.TopDiskLoad)-1]
	require.Equal(t, "", global.CodeMode)
	require.Equal(t, 3, global.Total)
}

// makeUnsafeSet returns a set from a list of DiskIDs.
func makeUnsafeSet(ids ...proto.DiskID) map[proto.DiskID]struct{} {
	s := make(map[proto.DiskID]struct{}, len(ids))
	for _, id := range ids {
		s[id] = struct{}{}
	}
	return s
}

func TestBuildDataSafety_AllSafe(t *testing.T) {
	vols := []clustermgr.VolumeBasic{
		{CodeMode: codemode.EC6P6, DiskIDs: []proto.DiskID{1, 2, 3}},
		{CodeMode: codemode.EC6P6, DiskIDs: []proto.DiskID{4, 5, 6}},
	}
	stat := buildVolumeSafety(volMap(vols), nil)
	require.Equal(t, clustermgr.DashboardScoreOK, stat.Score.Score)
	require.Equal(t, 2, stat.SafeVolumes)
	require.Equal(t, 0, stat.DegradedVolumes)
	require.Equal(t, 0, stat.AtRiskVolumes)
	require.Equal(t, 0, stat.DataLossVolumes)
	require.Empty(t, stat.UnsafeDetails)
}

// TestBuildDataSafety_Degraded_Notice: unsafe <= M/2 → degraded, Notice score.
func TestBuildDataSafety_Degraded_Notice(t *testing.T) {
	// EC6P6: M = 6; M/2 = 3; unsafe=2 ≤ 3 → degraded, Notice
	vols := []clustermgr.VolumeBasic{
		{CodeMode: codemode.EC6P6, DiskIDs: []proto.DiskID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}},
	}
	// Make disks 1 and 2 unsafe (2 unsafe units, M/2 = 3)
	stat := buildVolumeSafety(volMap(vols), makeUnsafeSet(1, 2))
	require.Equal(t, clustermgr.DashboardScoreNotice, stat.Score.Score)
	require.Equal(t, 1, stat.DegradedVolumes)
	require.Equal(t, 0, stat.AtRiskVolumes)
	require.Equal(t, 0, stat.DataLossVolumes)
	require.Empty(t, stat.UnsafeDetails)
}

// TestBuildDataSafety_Degraded_Warning: unsafe > M/2 but < M-1 → Warning score.
func TestBuildDataSafety_Degraded_Warning(t *testing.T) {
	// EC6P6: M = 6; M/2 = 3; unsafe=4 > 3 and < 5 (M-1) → Warning
	vols := []clustermgr.VolumeBasic{
		{CodeMode: codemode.EC6P6, DiskIDs: []proto.DiskID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}},
	}
	stat := buildVolumeSafety(volMap(vols), makeUnsafeSet(1, 2, 3, 4))
	require.Equal(t, clustermgr.DashboardScoreWarning, stat.Score.Score)
	require.Equal(t, 1, stat.DegradedVolumes)
	require.Empty(t, stat.UnsafeDetails)
}

// TestBuildDataSafety_AtRisk: unsafe == M-1 → at_risk, Major score.
func TestBuildDataSafety_AtRisk(t *testing.T) {
	// EC6P6: M = 6; M-1 = 5
	vols := []clustermgr.VolumeBasic{
		{CodeMode: codemode.EC6P6, DiskIDs: []proto.DiskID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}},
	}
	stat := buildVolumeSafety(volMap(vols), makeUnsafeSet(1, 2, 3, 4, 5))
	require.Equal(t, clustermgr.DashboardScoreMajor, stat.Score.Score)
	require.Equal(t, 0, stat.DegradedVolumes)
	require.Equal(t, 1, stat.AtRiskVolumes)
	require.Equal(t, 0, stat.DataLossVolumes)
	require.Len(t, stat.UnsafeDetails, 1)
	require.Equal(t, "at_risk", stat.UnsafeDetails[0].Level)
	require.Equal(t, 5, stat.UnsafeDetails[0].UnsafeUnits)
}

// TestBuildDataSafety_AtRisk_EqualM: unsafe == M is also at_risk (Major), not data_loss.
func TestBuildDataSafety_AtRisk_EqualM(t *testing.T) {
	// EC6P6: M = 6; unsafe=6 == M → at_risk (Major)
	vols := []clustermgr.VolumeBasic{
		{CodeMode: codemode.EC6P6, DiskIDs: []proto.DiskID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}},
	}
	stat := buildVolumeSafety(volMap(vols), makeUnsafeSet(1, 2, 3, 4, 5, 6))
	require.Equal(t, clustermgr.DashboardScoreMajor, stat.Score.Score)
	require.Equal(t, 1, stat.AtRiskVolumes)
	require.Equal(t, 0, stat.DataLossVolumes)
	require.Len(t, stat.UnsafeDetails, 1)
	require.Equal(t, "at_risk", stat.UnsafeDetails[0].Level)
	require.Equal(t, 6, stat.UnsafeDetails[0].UnsafeUnits)
}

// TestBuildDataSafety_DataLoss: unsafe > M → data_loss, Critical score.
func TestBuildDataSafety_DataLoss(t *testing.T) {
	// EC6P6: M = 6; unsafe=7 > M → data_loss (Critical)
	vols := []clustermgr.VolumeBasic{
		{CodeMode: codemode.EC6P6, DiskIDs: []proto.DiskID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}},
	}
	stat := buildVolumeSafety(volMap(vols), makeUnsafeSet(1, 2, 3, 4, 5, 6, 7))
	require.Equal(t, clustermgr.DashboardScoreCritical, stat.Score.Score)
	require.Equal(t, 1, stat.DataLossVolumes)
	require.Len(t, stat.UnsafeDetails, 1)
	require.Equal(t, "data_loss", stat.UnsafeDetails[0].Level)
	require.Equal(t, 7, stat.UnsafeDetails[0].UnsafeUnits)
}

// TestBuildDataSafety_MixedLevels: worst score wins; details sorted.
func TestBuildDataSafety_MixedLevels(t *testing.T) {
	vols := []clustermgr.VolumeBasic{
		{CodeMode: codemode.EC6P6, DiskIDs: []proto.DiskID{1, 2, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19}},
		{CodeMode: codemode.EC6P6, DiskIDs: []proto.DiskID{20, 21, 22, 23, 24, 30, 31, 32, 33, 34, 35, 36}},
		{CodeMode: codemode.EC6P6, DiskIDs: []proto.DiskID{40, 41, 42, 43, 44, 45, 50, 51, 52, 53, 54, 55}},
		{CodeMode: codemode.EC6P6, DiskIDs: []proto.DiskID{60, 61, 62, 63, 64, 65, 66, 70, 71, 72, 73, 74}},
	}
	unsafe := makeUnsafeSet(
		1, 2, // vol1: 2 unsafe → degraded
		20, 21, 22, 23, 24, // vol2: 5 unsafe == M-1 → at_risk
		40, 41, 42, 43, 44, 45, // vol3: 6 unsafe == M → at_risk
		60, 61, 62, 63, 64, 65, 66, // vol4: 7 unsafe > M → data_loss
	)
	stat := buildVolumeSafety(volMap(vols), unsafe)
	require.Equal(t, clustermgr.DashboardScoreCritical, stat.Score.Score)
	require.Equal(t, 1, stat.DegradedVolumes)
	require.Equal(t, 2, stat.AtRiskVolumes)
	require.Equal(t, 1, stat.DataLossVolumes)
	// details: data_loss (7) before at_risk (6) before at_risk (5)
	require.Len(t, stat.UnsafeDetails, 3)
	require.Equal(t, "data_loss", stat.UnsafeDetails[0].Level)
	require.Equal(t, 7, stat.UnsafeDetails[0].UnsafeUnits)
	require.Equal(t, "at_risk", stat.UnsafeDetails[1].Level)
	require.Equal(t, 6, stat.UnsafeDetails[1].UnsafeUnits)
	require.Equal(t, "at_risk", stat.UnsafeDetails[2].Level)
	require.Equal(t, 5, stat.UnsafeDetails[2].UnsafeUnits)
}

func TestBuildDataSafety_CapAt(t *testing.T) {
	const n = 150
	// Use a globally unsafe set for disks 1-5
	unsafe := makeUnsafeSet(1, 2, 3, 4, 5)
	vols := make([]clustermgr.VolumeBasic, n)
	for i := range vols {
		vols[i] = clustermgr.VolumeBasic{
			CodeMode: codemode.EC6P6,
			DiskIDs:  []proto.DiskID{1, 2, 3, 4, 5, proto.DiskID(100 + i)},
		}
	}
	stat := buildVolumeSafety(volMap(vols), unsafe)
	require.Equal(t, n, stat.AtRiskVolumes)
	require.Len(t, stat.UnsafeDetails, maxUnsafeDetails)
}

// stubBlobnode implements dashboardBlobnode with fixed test data.
type stubBlobnode struct {
	nodes       []clustermgr.BlobNodeInfo
	allDisks    []clustermgr.BlobNodeDiskInfo
	expiredDisk []clustermgr.BlobNodeDiskInfo
}

func (s *stubBlobnode) ListNodes(_ context.Context, _ proto.NodeStatus) []clustermgr.BlobNodeInfo {
	return s.nodes
}

func (s *stubBlobnode) DisksSnapshot() ([]clustermgr.BlobNodeDiskInfo, []clustermgr.BlobNodeDiskInfo) {
	return s.allDisks, s.expiredDisk
}

// newSimulateDashboard builds a dashboardMgr backed by a real service
// (for ServiceMgr / VolumeMgr access) but with a fixed blobnode stub,
// a pre-stored snapshot and the given pre-populated volumes.
func newSimulateDashboard(
	t *testing.T,
	snapshotScore int,
	bn *stubBlobnode,
	vols []clustermgr.VolumeBasic,
) (*dashboardMgr, func()) {
	svc, cleanup := initTestService(t)
	d := svc.dashboardMgr
	d.blobnode = bn
	d.volumes = volMap(vols)
	d.snapshot.Store(&dashboardSnapshot{
		dashboard: clustermgr.ClusterDashboard{Score: clustermgr.DashboardScore{Score: snapshotScore}},
	})
	return d, cleanup
}

// simulateBlobnode returns a stubBlobnode with one node at the given host and
// one Normal disk per diskID, all on that node.
func simulateBlobnode(nodeID proto.NodeID, host string, diskIDs ...proto.DiskID) *stubBlobnode {
	disks := make([]clustermgr.BlobNodeDiskInfo, len(diskIDs))
	for i, id := range diskIDs {
		disks[i] = clustermgr.BlobNodeDiskInfo{
			DiskInfo:          clustermgr.DiskInfo{NodeID: nodeID, Status: proto.DiskStatusNormal},
			DiskHeartBeatInfo: clustermgr.DiskHeartBeatInfo{DiskID: id},
		}
	}
	return &stubBlobnode{
		nodes: []clustermgr.BlobNodeInfo{
			{NodeInfo: clustermgr.NodeInfo{NodeID: nodeID, Host: host}},
		},
		allDisks: disks,
	}
}

// TestSimulate_NoMatch: IP not present in allNodes or services →
// original snapshot score is returned unchanged.
func TestSimulate_NoMatch(t *testing.T) {
	bn := simulateBlobnode(1, "http://10.0.0.1:9998", 101)
	d, cleanup := newSimulateDashboard(t, clustermgr.DashboardScoreWarning, bn, nil)
	defer cleanup()

	result := d.Simulate([]string{"10.0.0.99"})
	require.Equal(t, clustermgr.DashboardScoreWarning, result.Score.Score)
}

// TestSimulate_BlobNodeMatch_Degraded: shutdown node's Normal disk becomes
// unsafe; one volume unit is degraded (0 < unsafe ≤ M/2 → Notice).
func TestSimulate_BlobNodeMatch_Degraded(t *testing.T) {
	const (
		nodeID = proto.NodeID(1)
		diskID = proto.DiskID(101)
	)
	bn := simulateBlobnode(nodeID, "http://10.0.0.1:9998", diskID)

	// EC6P6 M=6: diskID is 1 of 12 units → unsafeCount=1 ≤ 3 → degraded.
	vols := []clustermgr.VolumeBasic{
		{
			CodeMode: codemode.EC6P6, Status: proto.VolumeStatusActive,
			DiskIDs: []proto.DiskID{diskID, 200, 201, 202, 203, 204, 205, 206, 207, 208, 209, 210},
		},
		{
			CodeMode: codemode.EC6P6, Status: proto.VolumeStatusIdle,
			DiskIDs: []proto.DiskID{300, 301, 302, 303, 304, 305, 306, 307, 308, 309, 310, 311},
		},
	}
	d, cleanup := newSimulateDashboard(t, clustermgr.DashboardScoreOK, bn, vols)
	defer cleanup()

	result := d.Simulate([]string{"10.0.0.1"})

	require.Equal(t, 1, result.VolumeSafety.DegradedVolumes)
	require.Equal(t, 0, result.VolumeSafety.AtRiskVolumes)
	require.Equal(t, 0, result.VolumeSafety.DataLossVolumes)
	require.GreaterOrEqual(t, result.VolumeSafety.Score.Score, clustermgr.DashboardScoreNotice)
	require.GreaterOrEqual(t, result.Score.Score, clustermgr.DashboardScoreNotice)
}

// TestSimulate_BlobNodeMatch_AtRisk: M-1 disks on the shutdown node →
// volume becomes at_risk (Major).
func TestSimulate_BlobNodeMatch_AtRisk(t *testing.T) {
	const nodeID = proto.NodeID(2)
	// EC6P6 M=6, M-1=5: 5 disks on the shutdown node.
	unsafeIDs := []proto.DiskID{1, 2, 3, 4, 5}
	safeIDs := []proto.DiskID{10, 11, 12, 13, 14, 15, 16, 17, 18, 19}

	bn := simulateBlobnode(nodeID, "http://10.0.0.2:9998", unsafeIDs...)

	volDiskIDs := append(append([]proto.DiskID{}, unsafeIDs...), safeIDs[:7]...)
	vols := []clustermgr.VolumeBasic{
		{CodeMode: codemode.EC6P6, Status: proto.VolumeStatusActive, DiskIDs: volDiskIDs},
		{CodeMode: codemode.EC6P6, Status: proto.VolumeStatusIdle, DiskIDs: safeIDs},
	}
	d, cleanup := newSimulateDashboard(t, clustermgr.DashboardScoreOK, bn, vols)
	defer cleanup()

	result := d.Simulate([]string{"10.0.0.2"})

	require.Equal(t, 1, result.VolumeSafety.AtRiskVolumes)
	require.Equal(t, clustermgr.DashboardScoreMajor, result.VolumeSafety.Score.Score)
	require.Len(t, result.VolumeSafety.UnsafeDetails, 1)
	require.Equal(t, "at_risk", result.VolumeSafety.UnsafeDetails[0].Level)
	require.Equal(t, 5, result.VolumeSafety.UnsafeDetails[0].UnsafeUnits)
}

// TestSimulate_NonNormalDisksNotExpired: Dropped/Broken disks on a shutdown
// node must NOT be added to expiredDisks — only DiskStatusNormal disks are.
func TestSimulate_NonNormalDisksNotExpired(t *testing.T) {
	const nodeID = proto.NodeID(3)

	normalID := proto.DiskID(201)
	droppedID := proto.DiskID(202)
	brokenID := proto.DiskID(203)

	bn := &stubBlobnode{
		nodes: []clustermgr.BlobNodeInfo{
			{NodeInfo: clustermgr.NodeInfo{NodeID: nodeID, Host: "http://10.0.0.3:9998"}},
		},
		allDisks: []clustermgr.BlobNodeDiskInfo{
			{
				DiskInfo:          clustermgr.DiskInfo{NodeID: nodeID, Status: proto.DiskStatusNormal},
				DiskHeartBeatInfo: clustermgr.DiskHeartBeatInfo{DiskID: normalID},
			},
			{
				DiskInfo:          clustermgr.DiskInfo{NodeID: nodeID, Status: proto.DiskStatusDropped},
				DiskHeartBeatInfo: clustermgr.DiskHeartBeatInfo{DiskID: droppedID},
			},
			{
				DiskInfo:          clustermgr.DiskInfo{NodeID: nodeID, Status: proto.DiskStatusBroken},
				DiskHeartBeatInfo: clustermgr.DiskHeartBeatInfo{DiskID: brokenID},
			},
		},
	}

	d, cleanup := newSimulateDashboard(t, clustermgr.DashboardScoreOK, bn, nil)
	defer cleanup()

	result := d.Simulate([]string{"10.0.0.3"})

	// Only the Normal disk should appear as expired.
	require.Equal(t, 1, result.Service.ExpiredDisks)
	for _, ids := range result.Service.ExpiredByNode {
		require.Contains(t, ids, normalID)
		require.NotContains(t, ids, droppedID)
		require.NotContains(t, ids, brokenID)
	}
}

// TestSimulate_NoNodeMatch_SnapshotUnchanged: service IP matching requires
// ServiceMgr to have registered nodes. Since ServiceMgr is a concrete type
// backed by an empty DB in tests, ListServiceInfo returns nothing and the
// snapshot is returned as-is when no blob node matches either.
func TestSimulate_NoNodeMatch_SnapshotUnchanged(t *testing.T) {
	bn := &stubBlobnode{} // no blob nodes
	d, cleanup := newSimulateDashboard(t, clustermgr.DashboardScoreOK, bn, nil)
	defer cleanup()

	// Pre-store a recognisable snapshot.
	preService := buildService([]clustermgr.ServiceNode{
		svcNode(proto.ServiceNameProxy, "idc1", "http://10.0.1.10:9100", time.Now().Add(time.Minute).Unix()),
		svcNode(proto.ServiceNameProxy, "idc1", "http://10.0.1.20:9100", time.Now().Add(time.Minute).Unix()),
	}, nil, noHost)
	d.snapshot.Store(&dashboardSnapshot{
		dashboard: clustermgr.ClusterDashboard{Score: preService.Score, Service: preService},
	})

	// 10.0.1.20 is in the snapshot but NOT in ServiceMgr → no change.
	result := d.Simulate([]string{"10.0.1.20"})
	require.Equal(t, preService.Score, result.Service.Score)
}
