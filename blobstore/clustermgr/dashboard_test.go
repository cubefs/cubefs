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
	result := svc.dashboardMgr.buildScope()
	require.Equal(t, clustermgr.DashboardScoreOK, result.Score)
	require.Empty(t, result.Scopes)
}

func TestDashboardBuildScope_SortedAndCorrectMaxValue(t *testing.T) {
	fakeScopes := map[string]uint64{
		"vid":        10,
		"diskid":     5,
		BidScopeName: 100,
		"space_id":   200,
		"nodeid":     3,
	}
	result := buildScopeFromMap(fakeScopes)

	for i := 1; i < len(result.Scopes); i++ {
		require.Less(t, result.Scopes[i-1].Name, result.Scopes[i].Name,
			"scopes not sorted at index %d", i)
	}

	for _, s := range result.Scopes {
		if s.Name == BidScopeName || s.Name == "space_id" {
			require.Equal(t, uint64(math.MaxUint64), s.MaxValue, "scope %s", s.Name)
		} else {
			require.Equal(t, uint64(math.MaxUint32), s.MaxValue, "scope %s", s.Name)
		}
	}
}

func TestDashboardBuildScope_ScoreOKWhenBelowHalf(t *testing.T) {
	fakeScopes := map[string]uint64{
		"vid":    100, // MaxUint32/2 = 2147483647; 100 is far below half
		"diskid": 1000,
	}
	result := buildScopeFromMap(fakeScopes)
	require.Equal(t, clustermgr.DashboardScoreOK, result.Score)
}

func TestDashboardBuildScope_ScoreNoticeWhenAboveHalf(t *testing.T) {
	fakeScopes := map[string]uint64{
		"vid":    math.MaxUint32/2 + 1, // just above half → Notice
		"diskid": 100,
	}
	result := buildScopeFromMap(fakeScopes)
	require.Equal(t, clustermgr.DashboardScoreNotice, result.Score)
}

func TestDashboardBuildScope_BidScopeNoOverflow(t *testing.T) {
	fakeScopes := map[string]uint64{
		BidScopeName: math.MaxUint64 / 2,
	}
	result := buildScopeFromMap(fakeScopes)
	require.Equal(t, clustermgr.DashboardScoreOK, result.Score)

	// current = MaxUint64/2+1 should be Notice
	fakeScopes[BidScopeName] = math.MaxUint64/2 + 1
	result = buildScopeFromMap(fakeScopes)
	require.Equal(t, clustermgr.DashboardScoreNotice, result.Score)
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
