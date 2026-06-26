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

	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/util"
)

type DashboardArgs struct {
	// Force to fresh an immediate snapshot of dashboard
	Force bool `json:"force,omitempty"`
}

type SimulateAgrs struct {
	Nodes []string `json:"nodes"`
}

type DashboardScore struct {
	Score  int    `json:"score"`
	Reason string `json:"reason,omitempty"`
}

const (
	DashboardScoreOK       = 0
	DashboardScoreNotice   = 1
	DashboardScoreWarning  = 2
	DashboardScoreMajor    = 3
	DashboardScoreCritical = 4
)

func (a DashboardScore) String() string {
	switch a.Score {
	case DashboardScoreOK:
		return "OK"
	case DashboardScoreNotice:
		return "NOTICE"
	case DashboardScoreWarning:
		return "WARNING"
	case DashboardScoreMajor:
		return "MAJOR"
	case DashboardScoreCritical:
		return "CRITICAL"
	default:
		return "UNKNOWN"
	}
}

func (a DashboardScore) Max(others ...DashboardScore) DashboardScore {
	m := a
	for _, b := range others {
		if b.Score > m.Score {
			m = b
		}
	}
	return m
}

// VolumeBasic is a lightweight in-memory copy.
type VolumeBasic struct {
	CodeMode codemode.CodeMode
	Score    int
	Free     uint64
	Used     uint64
	Total    uint64
	Status   proto.VolumeStatus
	DiskIDs  []proto.DiskID // one DiskID per volume unit, for top-disk-load
}

type ClusterDashboard struct {
	Score       DashboardScore `json:"score"`
	GeneratedAt int64          `json:"generated_at"` // Unix nanoseconds

	Scope   ScopeStat   `json:"scope"`
	Disk    DiskStat    `json:"disk"`
	Service ServiceStat `json:"service"`

	Volume       VolumeStat       `json:"volume"`
	VolumeSafety VolumeSafetyStat `json:"volume_safety"`
}

// VolumeSafetyEntry records the safety level of one non-safe volume.
type VolumeSafetyEntry struct {
	Vid           proto.Vid         `json:"vid"`
	CodeMode      codemode.CodeMode `json:"code_mode"`
	Level         string            `json:"level"` // "degraded" | "at_risk" | "data_loss"
	UnsafeUnits   int               `json:"unsafe_units"`
	UnsafeDiskIDs []proto.DiskID    `json:"unsafe_disks"`
}

type VolumeSafetyStat struct {
	Score           DashboardScore `json:"score"`
	SafeVolumes     int            `json:"safe_volumes"`
	DegradedVolumes int            `json:"degraded_volumes"`
	AtRiskVolumes   int            `json:"at_risk_volumes"`
	DataLossVolumes int            `json:"data_loss_volumes"`
	// UnsafeDetails lists at_risk and data_loss volumes sorted by unsafe_units
	// descending; capped at 100 entries.
	UnsafeDetails []VolumeSafetyEntry `json:"unsafe_details"`
}

// VolumeStatEntry holds aggregated capacity metrics for one bucket.
type VolumeStatEntry struct {
	Count      int   `json:"count"`
	FreeBytes  int64 `json:"free_bytes"`
	UsedBytes  int64 `json:"used_bytes"`
	TotalBytes int64 `json:"total_bytes"`
}

// VolumeScoreStat aggregates volumes by CodeMode → HealthScore → entry.
// key1: CodeMode name (e.g. "EC6P10L2"); key2: health_score integer value.
type VolumeScoreStat map[string]map[int]VolumeStatEntry

// VolumeFreeStat aggregates volumes by CodeMode → free-ratio bucket label → entry.
// key1: CodeMode name; key2: one of "10","20","30","40","50","60","70","80","90","99"
// where the label is the upper-bound percentage of free/(free+used).
type VolumeFreeStat map[string]map[string]VolumeStatEntry

// VolumeStatusStat is a fast status count across all volumes.
type VolumeStatusStat struct {
	Total           int `json:"total"`
	ActiveTotal     int `json:"active_total"`
	ActiveHealthy   int `json:"active_healthy"`
	ActiveUnhealthy int `json:"active_unhealthy"`
	IdleTotal       int `json:"idle_total"`
	OtherTotal      int `json:"other_total"`
}

// DiskLoadEntry holds one disk's load (active volume unit count).
type DiskLoadEntry struct {
	DiskID proto.DiskID `json:"disk_id"`
	Load   int          `json:"load"`
}

// TopDiskLoad ranks disks by active-volume-unit count for one CodeMode.
// CodeMode == "" represents the global (all codemodes) summary.
type TopDiskLoad struct {
	CodeMode string          `json:"code_mode"`
	Total    int             `json:"total"`
	TopN     []DiskLoadEntry `json:"top_n"`
}

// VolumeStat is the volume health snapshot included in ClusterDashboard.
type VolumeStat struct {
	Score              DashboardScore   `json:"score"`
	Status             VolumeStatusStat `json:"status"`
	ByScore            VolumeScoreStat  `json:"by_score"`
	ByFree             VolumeFreeStat   `json:"by_free"`
	AllocatableByScore VolumeScoreStat  `json:"allocatable_by_score"`
	AllocatableByFree  VolumeFreeStat   `json:"allocatable_by_free"`
	DiskLoadThreshold  int              `json:"disk_load_threshold"`
	TopDiskLoad        []TopDiskLoad    `json:"top_disk_load"`
}

// CalcScore derives VolumeStat.Score from the maximum disk load observed in
// TopDiskLoad relative to diskLoadThreshold (AllocatableDiskLoadThreshold).
//
//	OK:      threshold ≤ 0, or no disks recorded, or maxLoad ≤ threshold
//	Warning: maxLoad > threshold
//	Major:   maxLoad > 2 × threshold
func (v *VolumeStat) CalcScore() {
	diskLoadThreshold := v.DiskLoadThreshold
	if v.Status.ActiveTotal == 0 {
		v.Score = DashboardScore{
			Score:  DashboardScoreCritical,
			Reason: "no active volume",
		}
		return
	}
	if v.Status.IdleTotal == 0 {
		v.Score = DashboardScore{
			Score:  DashboardScoreCritical,
			Reason: "no idle volume",
		}
		return
	}

	v.Score = DashboardScore{}
	if diskLoadThreshold <= 0 || len(v.TopDiskLoad) == 0 {
		return
	}
	maxLoad := 0
	for _, tl := range v.TopDiskLoad {
		if len(tl.TopN) > 0 && tl.TopN[0].Load > maxLoad {
			maxLoad = tl.TopN[0].Load
		}
	}
	switch {
	case maxLoad > diskLoadThreshold && maxLoad-diskLoadThreshold > diskLoadThreshold:
		reason := fmt.Sprintf("disk_overload: maxLoad=%d > threshold=%d", maxLoad, diskLoadThreshold)
		v.Score = DashboardScore{Score: DashboardScoreMajor, Reason: reason}
	case maxLoad > diskLoadThreshold:
		reason := fmt.Sprintf("disk_overload: maxLoad=%d > threshold=%d", maxLoad, diskLoadThreshold)
		v.Score = DashboardScore{Score: DashboardScoreWarning, Reason: reason}
	default:
	}
}

// DiskStat holds disk metrics grouped by status key × IDC.
//
// For each physical slot (NodeID+Path), only the disk with the highest DiskID
// is tracked. ByStatusIDC keys:
//   - "normal" | "broken" | "repairing" | "dropped" — from proto.DiskStatus
//   - "__replace__" — current disk is Repaired; physical drive not yet swapped
//   - "__total__"   — active slots: normal + broken + repairing + __replace__
type DiskStat struct {
	Score       DashboardScore                  `json:"score"`
	ByStatusIDC map[string]map[string]DiskEntry `json:"by_status_idc"`
}

// DiskEntry holds aggregated metrics for one (status, idc) bucket.
type DiskEntry struct {
	Count          int   `json:"count"`
	UsedBytes      int64 `json:"used_bytes"`
	FreeBytes      int64 `json:"free_bytes"`
	TotalBytes     int64 `json:"total_bytes"`
	MaxChunks      int64 `json:"max_chunks"`
	FreeChunks     int64 `json:"free_chunks"`
	UsedChunks     int64 `json:"used_chunks"`
	OversoldChunks int64 `json:"oversold_chunks"`
}

func (s *DiskStat) SumIDC(status string) int {
	if len(s.ByStatusIDC) == 0 {
		return 0
	}
	n := 0
	for _, e := range s.ByStatusIDC[status] {
		n += e.Count
	}
	return n
}

// CalcScore unavailable disks = broken + repairing.
// The Notice threshold (t) is tiered by total disk count:
//
//	total ≤ 100  → t = 10%, ≤ 1000 → t = 5%, > 1000 → t = 1%
//
// Score levels:
//
//	OK      unavailable/total < t
//	Notice  >= t
//	Warning >= t×2
//	Major   >= t×3
func (s *DiskStat) CalcScore() {
	total := s.SumIDC("__total__")
	broken := s.SumIDC(proto.DiskStatusBroken.String())
	repairing := s.SumIDC(proto.DiskStatusRepairing.String())
	unavailable := broken + repairing
	if total == 0 || unavailable == 0 {
		s.Score = DashboardScore{}
		return
	}

	var noticePct int
	switch {
	case total > 1000:
		noticePct = 1
	case total > 100:
		noticePct = 5
	default:
		noticePct = 10
	}

	u100 := unavailable * 100
	switch {
	case u100 >= total*noticePct*3:
		s.Score.Score = DashboardScoreMajor
	case u100 >= total*noticePct*2:
		s.Score.Score = DashboardScoreWarning
	case u100 >= total*noticePct:
		s.Score.Score = DashboardScoreNotice
	default:
		s.Score.Score = DashboardScoreOK
	}
	if s.Score.Score > DashboardScoreOK {
		s.Score.Reason = fmt.Sprintf("broken_disk:%d + repairing_disk:%d / total:%d",
			broken, repairing, total)
	}
}

type ScopeStat struct {
	Score  DashboardScore `json:"score"`
	Scopes []ScopeUsage   `json:"scopes"`
}

func (su *ScopeStat) CalcScore() {
	su.Score = DashboardScore{}
	for _, s := range su.Scopes {
		if s.Current > s.MaxValue/2 {
			su.Score = DashboardScore{
				Score: DashboardScoreNotice,
				Reason: fmt.Sprintf("scope_near_limit: %s=%.4f%%",
					s.Name, float64(s.Current)*100/float64(s.MaxValue)),
			}
			return
		}
	}
}

// ScopeUsage records the current allocation progress of a single scope counter.
type ScopeUsage struct {
	Name     string `json:"name"`
	Current  uint64 `json:"current"`
	MaxValue uint64 `json:"max_value"`
}

// ServiceStat is the online/offline summary for all registered service nodes,
// and includes blobnode disk heartbeat expiry as part of service health.
type ServiceStat struct {
	Score DashboardScore `json:"score"`

	OfflineNodes    []ServiceNode             `json:"offline_nodes,omitempty"`
	OnlineByTypeIDC map[string]map[string]int `json:"online_by_type_idc,omitempty"`

	// Expired blobnode disks: Normal disks whose heartbeat has timed out, grouped by node host.
	ExpiredDisks  int                       `json:"expired_disks,omitempty"`
	ExpiredByNode map[string][]proto.DiskID `json:"expired_by_node,omitempty"`
}

func (s *ServiceStat) CalcScore() {
	score := DashboardScore{}

	idcScore := func(svc string, byIDC map[string]int) DashboardScore {
		sc := DashboardScore{}
		for idc, cnt := range byIDC {
			switch cnt {
			case 0:
				sc = sc.Max(DashboardScore{
					Score:  DashboardScoreMajor,
					Reason: fmt.Sprintf("%s has no service in idc %s", svc, idc),
				})
			case 1:
				sc = sc.Max(DashboardScore{
					Score:  DashboardScoreWarning,
					Reason: fmt.Sprintf("%s has only one service in idc %s", svc, idc),
				})
			}
		}
		return sc
	}
	for _, svc := range []string{
		proto.ServiceNameProxy,
		proto.ServiceNameWorker,
		proto.ServiceNameBlobNode,
	} {
		byIDC, ok := s.OnlineByTypeIDC[svc]
		if !ok {
			score = score.Max(DashboardScore{
				Score:  DashboardScoreMajor,
				Reason: svc + " has no service",
			})
		} else {
			score = score.Max(idcScore(svc, byIDC))
		}
	}

	total := 0
	if byIDC, ok := s.OnlineByTypeIDC[proto.ServiceNameScheduler]; ok {
		for _, cnt := range byIDC {
			total += cnt
		}
	}
	if total == 0 {
		score = score.Max(DashboardScore{
			Score:  DashboardScoreWarning,
			Reason: proto.ServiceNameScheduler + " has no service",
		})
	}

	if s.ExpiredDisks > 0 {
		score = score.Max(DashboardScore{
			Score:  DashboardScoreNotice,
			Reason: fmt.Sprintf("expired_disk:%d", s.ExpiredDisks),
		})
	}
	if s.ExpiredDisks > 120 {
		score = score.Max(DashboardScore{
			Score:  DashboardScoreWarning,
			Reason: fmt.Sprintf("expired_disk:%d", s.ExpiredDisks),
		})
	}

	if len(s.OfflineNodes) > 0 {
		score = score.Max(DashboardScore{
			Score:  DashboardScoreNotice,
			Reason: fmt.Sprintf("offline_node:%d", len(s.OfflineNodes)),
		})
	}

	s.Score = score
}

func (c *Client) Dashboard(ctx context.Context, args *DashboardArgs) (ret ClusterDashboard, err error) {
	err = c.GetWith(ctx, "/cluster/dashboard?force="+util.Any2String(args.Force), &ret)
	return
}

func (c *Client) Simulate(ctx context.Context, args *SimulateAgrs) (ret ClusterDashboard, err error) {
	err = c.PostWith(ctx, "/cluster/simulate", &ret, args)
	return
}
