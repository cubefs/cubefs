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
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/defaulter"
)

const (
	rebuildCooldown = 5 * time.Second

	SpaceIDScopeName = "space_id"
)

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
	now := time.Now()

	scope := buildScope(d.service.ScopeMgr.Stat())
	svcInfo, _ := d.service.ServiceMgr.ListServiceInfo()
	allDisks, expiredDisks := d.service.BlobNodeMgr.DisksSnapshot()
	disk := buildDisk(allDisks)
	service := buildService(svcInfo.Nodes, expiredDisks, func(nodeID proto.NodeID) string {
		info, err := d.service.BlobNodeMgr.GetNodeInfo(context.Background(), nodeID)
		if err != nil || info == nil {
			return ""
		}
		return info.Host
	})

	score := scope.Score.Max(disk.Score, service.Score)
	d.snapshot.Store(&dashboardSnapshot{
		dashboard: clustermgr.ClusterDashboard{
			Score:       score,
			Scope:       scope,
			Disk:        disk,
			Service:     service,
			GeneratedAt: now.UnixNano(),
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

func buildScope(rawScopes map[string]uint64) clustermgr.ScopeStat {
	scopeMaxValue := func(name string) uint64 {
		switch name {
		case BidScopeName, SpaceIDScopeName:
			return math.MaxUint64
		default:
			return math.MaxUint32
		}
	}
	scopes := make([]clustermgr.ScopeUsage, 0, len(rawScopes))
	for name, cur := range rawScopes {
		scopes = append(scopes, clustermgr.ScopeUsage{
			Name:     name,
			Current:  cur,
			MaxValue: scopeMaxValue(name),
		})
	}
	sort.Slice(scopes, func(i, j int) bool { return scopes[i].Name < scopes[j].Name })
	stat := clustermgr.ScopeStat{Scopes: scopes}
	stat.CalcScore()
	return stat
}

// buildDisk aggregates a flat disk snapshot list into DiskStat.
//
// For each physical slot (NodeID, Path), only the disk with the highest DiskID
// is considered "current". Older disks on the same slot are ignored.
//
// ByStatusIDC keys produced:
//   - proto.DiskStatus.String() — "normal" | "broken" | "repairing" | "dropped"
//   - "__replace__" — current disk is Repaired; physical drive not yet swapped
//   - "__total__"   — all active slots: normal + broken + repairing + __replace__
func buildDisk(snaps []clustermgr.BlobNodeDiskInfo) clustermgr.DiskStat {
	raw := make(map[string]map[string]clustermgr.DiskEntry)

	addEntry := func(key, idc string, hb *clustermgr.DiskHeartBeatInfo) {
		if raw[key] == nil {
			raw[key] = make(map[string]clustermgr.DiskEntry)
		}
		e := raw[key][idc]
		e.Count++
		e.UsedBytes += hb.Used
		e.FreeBytes += hb.Free
		e.TotalBytes += hb.Size
		e.MaxChunks += hb.MaxChunkCnt
		e.FreeChunks += hb.FreeChunkCnt
		e.UsedChunks += hb.UsedChunkCnt
		e.OversoldChunks += hb.OversoldFreeChunkCnt
		raw[key][idc] = e
	}

	type slotKey struct {
		nodeID proto.NodeID
		path   string
	}
	type slotEntry struct {
		diskID proto.DiskID
		snap   *clustermgr.BlobNodeDiskInfo
	}
	slotMax := make(map[slotKey]slotEntry)
	for i := range snaps {
		s := &snaps[i]
		k := slotKey{s.NodeID, s.Path}
		if cur, ok := slotMax[k]; !ok || s.DiskID > cur.diskID {
			slotMax[k] = slotEntry{s.DiskID, s}
		}
	}

	for _, e := range slotMax {
		s := e.snap
		if s.Status == proto.DiskStatusDropped {
			addEntry(s.Status.String(), s.Idc, &s.DiskHeartBeatInfo)
			continue
		}
		if s.Status == proto.DiskStatusRepaired {
			addEntry("__replace__", s.Idc, &s.DiskHeartBeatInfo)
		} else {
			addEntry(s.Status.String(), s.Idc, &s.DiskHeartBeatInfo)
		}
		addEntry("__total__", s.Idc, &s.DiskHeartBeatInfo)
	}

	stat := clustermgr.DiskStat{ByStatusIDC: raw}
	stat.CalcScore()
	return stat
}

func buildService(services []clustermgr.ServiceNode,
	expired []clustermgr.BlobNodeDiskInfo, getHost func(proto.NodeID) string,
) clustermgr.ServiceStat {
	var offlineNodes []clustermgr.ServiceNode
	onlineByTypeIDC := make(map[string]map[string]int)
	now := time.Now().Unix()
	for _, n := range services {
		if onlineByTypeIDC[n.Name] == nil {
			onlineByTypeIDC[n.Name] = make(map[string]int)
		}
		if _, ok := onlineByTypeIDC[n.Name][n.Idc]; !ok {
			onlineByTypeIDC[n.Name][n.Idc] = 0
		}
		if n.ExpireAt < now {
			offlineNodes = append(offlineNodes, n)
		} else {
			onlineByTypeIDC[n.Name][n.Idc]++
		}
	}

	byNodeID := make(map[proto.NodeID][]proto.DiskID)
	for _, disk := range expired {
		byNodeID[disk.NodeID] = append(byNodeID[disk.NodeID], disk.DiskID)
	}
	var expiredByNode map[string][]proto.DiskID
	if len(byNodeID) > 0 {
		expiredByNode = make(map[string][]proto.DiskID, len(byNodeID))
		for nodeID, ids := range byNodeID {
			host := getHost(nodeID)
			expiredByNode[host] = append(expiredByNode[host], ids...)
		}
	}

	stat := clustermgr.ServiceStat{
		OfflineNodes:    offlineNodes,
		OnlineByTypeIDC: onlineByTypeIDC,
		ExpiredDisks:    len(expired),
		ExpiredByNode:   expiredByNode,
	}
	stat.CalcScore()
	return stat
}
