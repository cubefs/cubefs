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
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/defaulter"
)

const rebuildCooldown = 5 * time.Second

func (s *Service) Dashboard(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)

	args := new(clustermgr.DashboardArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Infof("accept dashboard request, force=%v", args.Force)

	if args.Force {
		select {
		case <-s.dashboardMgr.Refresh():
		case <-ctx.Done():
			c.RespondError(ctx.Err())
			return
		}
	}

	snap := s.dashboardMgr.GetSnapshot()
	c.RespondJSON(snap.dashboard)
}

type dashboardSnapshot struct {
	dashboard clustermgr.ClusterDashboard
}

type dashboardMgr struct {
	service  *Service
	snapshot atomic.Value // stores *dashboardSnapshot

	freshCh chan struct{} // buffered(1); loop listens for force-fresh signals

	processLock   sync.Mutex
	processCh     chan struct{} // non-nil while a fresh is pending; closed on completion
	cooldownUntil time.Time     // Refresh() returns a pre-closed channel before this time
}

func newDashboardMgr(service *Service) *dashboardMgr {
	d := &dashboardMgr{
		service: service,
		freshCh: make(chan struct{}, 1),
	}
	d.snapshot.Store(&dashboardSnapshot{})
	return d
}

func (d *dashboardMgr) Refresh() <-chan struct{} {
	d.processLock.Lock()
	defer d.processLock.Unlock()

	if d.processCh != nil {
		return d.processCh
	}

	// within cooldown window: snapshot is fresh enough, return a pre-closed
	if time.Now().Before(d.cooldownUntil) {
		ch := make(chan struct{})
		close(ch)
		return ch
	}

	ch := make(chan struct{})
	d.processCh = ch
	select {
	case d.freshCh <- struct{}{}:
	default:
	}
	return ch
}

func (d *dashboardMgr) GetSnapshot() *dashboardSnapshot {
	return d.snapshot.Load().(*dashboardSnapshot)
}

func (d *dashboardMgr) loopFresh() {
	defaulter.LessOrEqual(&d.service.DashboardFreshIntervalS, 60)

	interval := time.Duration(d.service.DashboardFreshIntervalS) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			d.fresh()
			select {
			case <-d.freshCh:
			default:
			}

		case <-d.freshCh:
			d.fresh()
			ticker.Reset(interval)

		case <-d.service.closeCh:
			return
		}
	}
}

func (d *dashboardMgr) fresh() {
	d.snapshot.Store(&dashboardSnapshot{
		dashboard: clustermgr.ClusterDashboard{
			Scope:       d.buildScope(),
			GeneratedAt: time.Now().UnixNano(),
		},
	})

	d.processLock.Lock()
	ch := d.processCh
	d.processCh = nil
	d.cooldownUntil = time.Now().Add(rebuildCooldown)
	d.processLock.Unlock()

	if ch != nil {
		close(ch) // wake all force waiters
	}
}

func (d *dashboardMgr) buildScope() clustermgr.ScopeStat {
	return buildScopeFromMap(d.service.ScopeMgr.Stat())
}

func buildScopeFromMap(rawScopes map[string]uint64) clustermgr.ScopeStat {
	scopeMaxValue := func(name string) uint64 {
		switch name {
		case BidScopeName, "space_id":
			return math.MaxUint64
		default:
			return math.MaxUint32
		}
	}

	score := clustermgr.DashboardScoreOK
	scopes := make([]clustermgr.ScopeUsage, 0, len(rawScopes))
	for name, cur := range rawScopes {
		maxVal := scopeMaxValue(name)
		if cur > maxVal/2 {
			score = clustermgr.DashboardScoreNotice
		}
		scopes = append(scopes, clustermgr.ScopeUsage{
			Name:     name,
			Current:  cur,
			MaxValue: maxVal,
		})
	}
	sort.Slice(scopes, func(i, j int) bool {
		return scopes[i].Name < scopes[j].Name
	})
	return clustermgr.ScopeStat{Score: score, Scopes: scopes}
}
