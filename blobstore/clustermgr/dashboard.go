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
	"fmt"
	"math"
	"net"
	"net/url"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util"
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

func (s *Service) Simulate(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)

	args := new(clustermgr.SimulateAgrs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	span.Infof("accept simulate request, nodes=%v", args.Nodes)
	for _, node := range args.Nodes {
		if net.ParseIP(node) == nil {
			c.RespondError(errcode.ErrIllegalArguments)
			return
		}
	}

	dashboard := s.dashboardMgr.Simulate(args.Nodes)
	c.RespondJSON(dashboard)
}

type dashboardSnapshot struct {
	dashboard clustermgr.ClusterDashboard
}

type dashboardBlobnode interface {
	ListNodes(ctx context.Context, status proto.NodeStatus) []clustermgr.BlobNodeInfo
	DisksSnapshot() ([]clustermgr.BlobNodeDiskInfo, []clustermgr.BlobNodeDiskInfo)
}

type dashboardMgr struct {
	service  *Service
	blobnode dashboardBlobnode
	snapshot atomic.Value // stores *dashboardSnapshot

	// volumes is a persistent cache of VolumeBasic keyed by Vid.
	// RangeUpdateVolume updates entries in-place
	// only called DashboardFreshVolumeTick reached
	volumes    map[proto.Vid]*clustermgr.VolumeBasic
	volMu      sync.RWMutex
	freshTick  uint64
	lastVolume clustermgr.VolumeStat

	freshCh chan struct{} // buffered(1); loop listens for force-fresh signals

	processLock   sync.Mutex
	processCh     chan struct{} // non-nil while a fresh is pending; closed on completion
	cooldownUntil time.Time     // Refresh() returns a pre-closed channel before this time
}

func newDashboardMgr(service *Service) *dashboardMgr {
	d := &dashboardMgr{
		service:  service,
		blobnode: service.BlobNodeMgr,
		volumes:  make(map[proto.Vid]*clustermgr.VolumeBasic),
		freshCh:  make(chan struct{}, 1),
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

func (d *dashboardMgr) Simulate(nodes []string) clustermgr.ClusterDashboard {
	dashboard := d.GetSnapshot().dashboard
	dashboard.GeneratedAt = time.Now().UnixNano()

	shutdownIPs := make(map[string]struct{}, len(nodes))
	for _, n := range nodes {
		shutdownIPs[n] = struct{}{}
	}

	allNodes := d.blobnode.ListNodes(context.Background(), proto.NodeStatusInvalid)
	nodeHost := make(map[proto.NodeID]string, len(allNodes))
	shutdownNodeIDs := make(map[proto.NodeID]struct{})
	for _, n := range allNodes {
		nodeHost[n.NodeID] = n.Host
		if _, hit := shutdownIPs[hostIP(n.Host)]; hit {
			shutdownNodeIDs[n.NodeID] = struct{}{}
		}
	}

	allDisks, expiredDisks := d.blobnode.DisksSnapshot()
	for _, di := range allDisks {
		if _, hit := shutdownNodeIDs[di.NodeID]; hit && di.Status == proto.DiskStatusNormal {
			expiredDisks = append(expiredDisks, di)
		}
	}

	services, _ := d.service.ServiceMgr.ListServiceInfo()
	serviceChanged := false
	for idx, node := range services.Nodes {
		if _, hit := shutdownIPs[hostIP(node.Host)]; hit {
			serviceChanged = true
			node.ExpireAt = time.Now().Unix() - 1 // mark as just-expired for simulation
			services.Nodes[idx] = node
		}
	}

	if !serviceChanged && len(shutdownNodeIDs) == 0 {
		return dashboard
	}

	service := buildService(services.Nodes, expiredDisks, func(nodeID proto.NodeID) string {
		return nodeHost[nodeID]
	})
	dashboard.Service = service
	dashboard.Score = dashboard.Score.Max(service.Score)
	if len(shutdownNodeIDs) == 0 {
		return dashboard
	}

	unsafeDiskSet := make(map[proto.DiskID]struct{}, len(allDisks))
	for _, di := range allDisks {
		if di.Status != proto.DiskStatusNormal {
			unsafeDiskSet[di.DiskID] = struct{}{}
		}
	}
	for _, di := range expiredDisks {
		unsafeDiskSet[di.DiskID] = struct{}{}
	}

	d.volMu.RLock()
	volume := buildVolume(d.volumes,
		d.service.VolumeMgr.AllocatableSize,
		d.service.VolumeMgr.RetainThreshold,
		d.service.VolumeMgr.AllocatableDiskLoadThreshold)
	safety := buildVolumeSafety(d.volumes, unsafeDiskSet)
	d.volMu.RUnlock()

	volume.Usage = buildUsage(volume)
	dashboard.Volume = volume
	dashboard.VolumeSafety = safety
	dashboard.Score = dashboard.Score.Max(volume.Score, safety.Score)
	return dashboard
}

func (d *dashboardMgr) loopFresh() {
	defaulter.LessOrEqual(&d.service.DashboardFreshIntervalS, 60)
	defaulter.IntegerLessOrEqual(&d.service.DashboardFreshVolumeTick, 5)

	interval := time.Duration(d.service.DashboardFreshIntervalS) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		_, ctx := trace.StartSpanFromContext(context.Background(), "")
		select {
		case <-ticker.C:
			d.fresh(ctx, false)
			select {
			case <-d.freshCh:
			default:
			}

		case <-d.freshCh:
			d.fresh(ctx, true)
			ticker.Reset(interval)

		case <-d.service.closeCh:
			return
		}
	}
}

func (d *dashboardMgr) fresh(ctx context.Context, force bool) {
	now := time.Now()

	scope := buildScope(d.service.ScopeMgr.Stat())
	services, _ := d.service.ServiceMgr.ListServiceInfo()
	allDisks, expiredDisks := d.blobnode.DisksSnapshot()
	allNodes := d.blobnode.ListNodes(ctx, proto.NodeStatusInvalid)
	droppedNodes := make(map[proto.NodeID]struct{})
	nodeHost := make(map[proto.NodeID]string, len(allNodes))
	for _, n := range allNodes {
		nodeHost[n.NodeID] = n.Host
		if n.Status == proto.NodeStatusDropped {
			droppedNodes[n.NodeID] = struct{}{}
		}
	}
	disk := buildDisk(allDisks, droppedNodes)
	service := buildService(services.Nodes, expiredDisks, func(nodeID proto.NodeID) string {
		return nodeHost[nodeID]
	})

	unsafeDiskSet := make(map[proto.DiskID]struct{}, len(allDisks))
	for _, di := range allDisks {
		if di.Status != proto.DiskStatusNormal {
			unsafeDiskSet[di.DiskID] = struct{}{}
		}
	}
	for _, di := range expiredDisks {
		unsafeDiskSet[di.DiskID] = struct{}{}
	}

	d.volMu.Lock()
	if force || d.freshTick%uint64(util.Max(1, d.service.DashboardFreshVolumeTick)) == 0 {
		d.service.VolumeMgr.RangeUpdateVolume(ctx, d.volumes)
		d.lastVolume = buildVolume(d.volumes,
			d.service.VolumeMgr.AllocatableSize,
			d.service.VolumeMgr.RetainThreshold,
			d.service.VolumeMgr.AllocatableDiskLoadThreshold)
	}
	d.freshTick++
	volume := d.lastVolume
	safety := buildVolumeSafety(d.volumes, unsafeDiskSet)
	d.volMu.Unlock()

	volume.Usage = buildUsage(volume)
	score := scope.Score.Max(disk.Score, service.Score, volume.Score, safety.Score)
	snapshot := clustermgr.ClusterDashboard{
		Score:        score,
		Scope:        scope,
		Disk:         disk,
		Service:      service,
		Volume:       volume,
		VolumeSafety: safety,
		GeneratedAt:  now.UnixNano(),
	}
	d.snapshot.Store(&dashboardSnapshot{dashboard: snapshot})
	d.service.reportDashboard(snapshot)

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
// droppedNodes is the set of NodeIDs whose status is NodeStatusDropped.
// Any disk whose NodeID appears in droppedNodes is categorised as "dropped"
// regardless of the disk's own status.
//
// ByStatusIDC keys produced:
//   - proto.DiskStatus.String() — "normal" | "broken" | "repairing" | "dropped"
//   - "__replace__" — current disk is Repaired; physical drive not yet swapped
//   - "__total__"   — all active slots: normal + broken + repairing + __replace__
func buildDisk(snaps []clustermgr.BlobNodeDiskInfo, droppedNodes map[proto.NodeID]struct{}) clustermgr.DiskStat {
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
		// Legacy disks from before node-id tracking: NodeID==0 with Repaired status means
		// the node was decommissioned before node tracking existed; count as repaired but
		// exclude from __total__ (no physical slot to track).
		if s.NodeID == 0 && s.Status == proto.DiskStatusRepaired {
			addEntry(proto.DiskStatusRepaired.String(), s.Idc, &s.DiskHeartBeatInfo)
			continue
		}
		// Node is Dropped → treat all its disks as dropped regardless of disk status.
		if _, nodeDropped := droppedNodes[s.NodeID]; nodeDropped || s.Status == proto.DiskStatusDropped {
			addEntry(proto.DiskStatusDropped.String(), s.Idc, &s.DiskHeartBeatInfo)
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
	for _, n := range services {
		if onlineByTypeIDC[n.Name] == nil {
			onlineByTypeIDC[n.Name] = make(map[string]int)
		}
		if _, ok := onlineByTypeIDC[n.Name][n.Idc]; !ok {
			onlineByTypeIDC[n.Name][n.Idc] = 0
		}
		if n.ExpireAt != 0 {
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

const topDiskLoadN = 50

// buildVolume aggregates volumes into VolumeStat.
//   - allocatableSize:     minimum free bytes for a volume to be allocatable
//   - retainThreshold:     minimum HealthScore for a volume to be allocatable
//   - diskLoadThreshold:   AllocatableDiskLoadThreshold from VolumeMgrConfig
func buildVolume(volumes map[proto.Vid]*clustermgr.VolumeBasic,
	allocatableSize uint64, retainThreshold int, diskLoadThreshold int,
) clustermgr.VolumeStat {
	byScore := make(clustermgr.VolumeScoreStat)
	byFree := make(clustermgr.VolumeFreeStat)
	allocatableByScore := make(clustermgr.VolumeScoreStat)
	allocatableByFree := make(clustermgr.VolumeFreeStat)

	type diskKey struct {
		code   codemode.CodeMode
		diskID proto.DiskID
	}
	perModeDiskLoad := make(map[diskKey]int)
	globalDiskLoad := make(map[proto.DiskID]int)
	perModeActiveTotal := make(map[codemode.CodeMode]int)

	activeTotal, activeHealthy, activeUnhealthy := 0, 0, 0
	idleTotal, otherTotal, activeGlobalTotal := 0, 0, 0

	for _, vp := range volumes {
		v := *vp
		codeName := v.CodeMode.String()
		if codeName == "" {
			codeName = fmt.Sprintf("unknown(%d)", int(v.CodeMode))
		}

		// ByScore: CodeMode → health_score → entry
		if byScore[codeName] == nil {
			byScore[codeName] = make(map[int]clustermgr.VolumeStatEntry)
		}
		se := byScore[codeName][v.Score]
		se.Count++
		se.FreeBytes += int64(v.Free)
		se.UsedBytes += int64(v.Used)
		se.TotalBytes += int64(v.Total)
		byScore[codeName][v.Score] = se

		// ByFree: CodeMode → free-ratio bucket → entry
		label := volumeFreeRatioLabel(v.Free, v.Used)
		if byFree[codeName] == nil {
			byFree[codeName] = make(map[string]clustermgr.VolumeStatEntry)
		}
		fe := byFree[codeName][label]
		fe.Count++
		fe.FreeBytes += int64(v.Free)
		fe.UsedBytes += int64(v.Used)
		fe.TotalBytes += int64(v.Total)
		byFree[codeName][label] = fe

		switch v.Status {
		case proto.VolumeStatusActive:
			activeTotal++
			if v.Score >= 0 {
				activeHealthy++
			} else {
				activeUnhealthy++
			}
			// top-disk-load: only active volumes
			perModeActiveTotal[v.CodeMode]++
			activeGlobalTotal++
			for _, diskID := range v.DiskIDs {
				perModeDiskLoad[diskKey{v.CodeMode, diskID}]++
				globalDiskLoad[diskID]++
			}

		case proto.VolumeStatusIdle:
			idleTotal++
			// allocatable: Idle + free > threshold + score >= retainThreshold
			if v.Free > allocatableSize && v.Score >= retainThreshold {
				// AllocatableByScore
				if allocatableByScore[codeName] == nil {
					allocatableByScore[codeName] = make(map[int]clustermgr.VolumeStatEntry)
				}
				ase := allocatableByScore[codeName][v.Score]
				ase.Count++
				ase.FreeBytes += int64(v.Free)
				ase.UsedBytes += int64(v.Used)
				ase.TotalBytes += int64(v.Total)
				allocatableByScore[codeName][v.Score] = ase

				// AllocatableByFree
				if allocatableByFree[codeName] == nil {
					allocatableByFree[codeName] = make(map[string]clustermgr.VolumeStatEntry)
				}
				afl := volumeFreeRatioLabel(v.Free, v.Used)
				afe := allocatableByFree[codeName][afl]
				afe.Count++
				afe.FreeBytes += int64(v.Free)
				afe.UsedBytes += int64(v.Used)
				afe.TotalBytes += int64(v.Total)
				allocatableByFree[codeName][afl] = afe
			}

		default:
			otherTotal++
		}
	}

	status := clustermgr.VolumeStatusStat{
		Total:           activeTotal + idleTotal + otherTotal,
		ActiveTotal:     activeTotal,
		ActiveHealthy:   activeHealthy,
		ActiveUnhealthy: activeUnhealthy,
		IdleTotal:       idleTotal,
		OtherTotal:      otherTotal,
	}

	// Build per-codemode top-disk-load lists then append the global summary.
	codeModeDiskLoad := make(map[codemode.CodeMode]map[proto.DiskID]int)
	for k, load := range perModeDiskLoad {
		if codeModeDiskLoad[k.code] == nil {
			codeModeDiskLoad[k.code] = make(map[proto.DiskID]int)
		}
		codeModeDiskLoad[k.code][k.diskID] = load
	}
	topLoads := make([]clustermgr.TopDiskLoad, 0, len(codeModeDiskLoad)+1)
	for mode, loads := range codeModeDiskLoad {
		topLoads = append(topLoads, clustermgr.TopDiskLoad{
			CodeMode: mode.String(),
			Total:    perModeActiveTotal[mode],
			TopN:     topKDiskLoad(loads, topDiskLoadN),
		})
	}
	sort.Slice(topLoads, func(i, j int) bool {
		return topLoads[i].CodeMode < topLoads[j].CodeMode
	})
	if len(globalDiskLoad) > 0 {
		topLoads = append(topLoads, clustermgr.TopDiskLoad{
			CodeMode: "",
			Total:    activeGlobalTotal,
			TopN:     topKDiskLoad(globalDiskLoad, topDiskLoadN),
		})
	}

	stat := clustermgr.VolumeStat{
		Status:             status,
		ByScore:            byScore,
		ByFree:             byFree,
		AllocatableByScore: allocatableByScore,
		AllocatableByFree:  allocatableByFree,
		DiskLoadThreshold:  diskLoadThreshold,
		TopDiskLoad:        topLoads,
	}
	stat.CalcScore()
	return stat
}

// volumeFreeRatioLabel maps (free, used) to a bucket label string.
//
//	ratio = free / (free + used)
//	idx   = int(ratio × 10)  [integer division: free*10/(free+used)]
//	label = "99" if idx ≥ 9, else strconv.Itoa((idx+1)×10) → "10"…"90"
func volumeFreeRatioLabel(free, used uint64) string {
	total := free + used
	if total == 0 {
		return "99"
	}
	idx := free * 10 / total
	if idx >= 9 {
		return "99"
	}
	return strconv.Itoa(int((idx + 1) * 10))
}

// topKDiskLoad returns at most k DiskLoadEntry items sorted by Load descending.
func topKDiskLoad(loads map[proto.DiskID]int, k int) []clustermgr.DiskLoadEntry {
	entries := make([]clustermgr.DiskLoadEntry, 0, len(loads))
	for diskID, load := range loads {
		entries = append(entries, clustermgr.DiskLoadEntry{DiskID: diskID, Load: load})
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Load > entries[j].Load
	})
	if len(entries) > k {
		entries = entries[:k]
	}
	return entries
}

const maxUnsafeDetails = 100

// buildVolumeSafety cross-references each volume's DiskIDs against unsafeDiskSet
// to determine real-time data-safety levels
//
// Scoring per volume (M = CodeMode parity / fault-tolerance count):
//
//	unsafe == 0           → safe          (Score OK)
//	0 < unsafe ≤ M/2      → degraded      (Score Notice)
//	M/2 < unsafe < M-1    → degraded      (Score Warning)
//	unsafe == M-1 or M    → at_risk       (Score Major)
//	unsafe > M            → data_loss     (Score Critical)
//
// The overall score is the worst level observed across all volumes.
func buildVolumeSafety(
	volumes map[proto.Vid]*clustermgr.VolumeBasic,
	unsafeDiskSet map[proto.DiskID]struct{},
) clustermgr.VolumeSafetyStat {
	var (
		safe, degraded, atRisk, dataLoss int

		details []clustermgr.VolumeSafetyEntry
		score   clustermgr.DashboardScore
	)

	for vid, vp := range volumes {
		v := vp
		tolerance := v.CodeMode.Tactic().M
		if tolerance <= 0 {
			continue
		}

		var unsafeDiskIDs []proto.DiskID
		for _, diskID := range v.DiskIDs {
			if _, bad := unsafeDiskSet[diskID]; bad {
				unsafeDiskIDs = append(unsafeDiskIDs, diskID)
			}
		}
		unsafeCount := len(unsafeDiskIDs)
		if unsafeCount == 0 {
			safe++
			continue
		}

		var level string
		var volScore int
		switch {
		case unsafeCount > tolerance:
			level = "data_loss"
			volScore = clustermgr.DashboardScoreCritical
			dataLoss++
		case unsafeCount >= tolerance-1:
			level = "at_risk"
			volScore = clustermgr.DashboardScoreMajor
			atRisk++
		case unsafeCount*2 > tolerance:
			level = "degraded"
			volScore = clustermgr.DashboardScoreWarning
			degraded++
		default:
			level = "degraded"
			volScore = clustermgr.DashboardScoreNotice
			degraded++
		}
		score = score.Max(clustermgr.DashboardScore{Score: volScore, Reason: level})

		if level == "at_risk" || level == "data_loss" {
			details = append(details, clustermgr.VolumeSafetyEntry{
				Vid:           vid,
				CodeMode:      v.CodeMode,
				UnsafeUnits:   unsafeCount,
				Level:         level,
				UnsafeDiskIDs: unsafeDiskIDs,
			})
		}
	}

	sort.Slice(details, func(i, j int) bool {
		return details[i].UnsafeUnits > details[j].UnsafeUnits
	})
	if len(details) > maxUnsafeDetails {
		details = details[:maxUnsafeDetails]
	}

	switch {
	case dataLoss > 0:
		score.Reason = fmt.Sprintf("data_loss:%d", dataLoss)
	case atRisk > 0:
		score.Reason = fmt.Sprintf("at_risk:%d", atRisk)
	case degraded > 0:
		score.Reason = fmt.Sprintf("degraded:%d", degraded)
	}

	return clustermgr.VolumeSafetyStat{
		Score:           score,
		SafeVolumes:     safe,
		DegradedVolumes: degraded,
		AtRiskVolumes:   atRisk,
		DataLossVolumes: dataLoss,
		UnsafeDetails:   details,
	}
}

func buildUsage(v clustermgr.VolumeStat) clustermgr.UsageStat {
	stat := make(clustermgr.UsageStat, len(v.ByScore)+1)
	var allLogic, allPhys int64
	for codeName, byScore := range v.ByScore {
		tactic := codemode.CodeModeName(codeName).GetCodeMode().Tactic()
		total := tactic.N + tactic.M + tactic.L
		if total == 0 {
			continue
		}
		var phys int64
		for _, e := range byScore {
			phys += e.UsedBytes
		}
		logic := phys / int64(total) * int64(tactic.N)
		rate := float64(0)
		if phys > 0 {
			rate = float64(logic) / float64(phys)
		}
		stat[codeName] = clustermgr.UsageEntry{Logic: logic, Physical: phys, Rate: rate}
		allLogic += logic
		allPhys += phys
	}
	allRate := float64(0)
	if allPhys > 0 {
		allRate = float64(allLogic) / float64(allPhys)
	}
	stat["ALL"] = clustermgr.UsageEntry{Logic: allLogic, Physical: allPhys, Rate: allRate}
	return stat
}

func hostIP(host string) string {
	u, err := url.Parse(host)
	if err != nil {
		return host
	}
	ip, _, err := net.SplitHostPort(u.Host)
	if err != nil {
		return u.Host
	}
	return ip
}
