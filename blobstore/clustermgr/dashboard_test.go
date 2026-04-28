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
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
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
	d.fresh()

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
		svc.dashboardMgr.fresh()
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
	require.Equal(t, clustermgr.DashboardScoreOK, result.Score)
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
	require.Equal(t, clustermgr.DashboardScoreOK, result.Score)
}

func TestDashboardBuildScope_ScoreNoticeWhenAboveHalf(t *testing.T) {
	result := buildScope(map[string]uint64{
		"vid":    math.MaxUint32/2 + 1, // just above half → Notice
		"diskid": 100,
	})
	require.Equal(t, clustermgr.DashboardScoreNotice, result.Score)
}

func TestDashboardBuildScope_BidScopeNoOverflow(t *testing.T) {
	require.Equal(t, clustermgr.DashboardScoreOK,
		buildScope(map[string]uint64{BidScopeName: math.MaxUint64 / 2}).Score)
	require.Equal(t, clustermgr.DashboardScoreNotice,
		buildScope(map[string]uint64{BidScopeName: math.MaxUint64/2 + 1}).Score)
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
	stat := buildDisk(nil)
	require.Empty(t, stat.ByStatusIDC)
	require.Equal(t, clustermgr.DashboardScoreOK, stat.Score)
}

func TestBuildDisk_NormalDisk(t *testing.T) {
	// A single normal disk: appears in "normal" and "__total__"; no other keys.
	snaps := []clustermgr.BlobNodeDiskInfo{
		genDisk(1, "/data0", 10, proto.DiskStatusNormal, "idc1",
			clustermgr.DiskHeartBeatInfo{Size: 1000, Used: 200, Free: 800, MaxChunkCnt: 10, FreeChunkCnt: 8, UsedChunkCnt: 2}),
	}
	stat := buildDisk(snaps)

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
	stat := buildDisk(snaps)

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
	stat := buildDisk(snaps)

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
	stat := buildDisk(snaps)

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
	stat := buildDisk(snaps)

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
	stat := buildDisk(snaps)

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
	stat := buildDisk(snaps)

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
	stat := buildDisk(snaps)

	total := stat.ByStatusIDC["__total__"]["idc1"].Count
	normal := stat.ByStatusIDC["normal"]["idc1"].Count
	broken := stat.ByStatusIDC["broken"]["idc1"].Count
	repairing := stat.ByStatusIDC["repairing"]["idc1"].Count
	replace := stat.ByStatusIDC["__replace__"]["idc1"].Count

	require.Equal(t, normal+broken+repairing+replace, total)
	require.Equal(t, 4, total)
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
	// A freshly initialised service has no disks registered, so Disk.Score must be OK.
	require.Equal(t, clustermgr.DashboardScoreOK, snap.dashboard.Disk.Score)
	require.Equal(t, clustermgr.DashboardScoreOK, snap.dashboard.Score)
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
