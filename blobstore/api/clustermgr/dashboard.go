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

	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/util"
)

type DashboardArgs struct {
	// Force to fresh an immediate snapshot of dashboard
	Force bool `json:"force,omitempty"`
}

type DashboardScore int

const (
	DashboardScoreOK       DashboardScore = 0
	DashboardScoreNotice   DashboardScore = 1
	DashboardScoreWarning  DashboardScore = 2
	DashboardScoreMajor    DashboardScore = 3
	DashboardScoreCritical DashboardScore = 4
)

func (a DashboardScore) Max(others ...DashboardScore) DashboardScore {
	m := a
	for _, b := range others {
		m = util.Max(m, b)
	}
	return m
}

type ClusterDashboard struct {
	Score       DashboardScore `json:"score"`
	GeneratedAt int64          `json:"generated_at"` // Unix nanoseconds

	Scope   ScopeStat   `json:"scope"`
	Disk    DiskStat    `json:"disk"`
	Service ServiceStat `json:"service"`
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
	unavailable := s.SumIDC(proto.DiskStatusBroken.String()) +
		s.SumIDC(proto.DiskStatusRepairing.String())
	if total == 0 || unavailable == 0 {
		s.Score = DashboardScoreOK
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
		s.Score = DashboardScoreMajor
	case u100 >= total*noticePct*2:
		s.Score = DashboardScoreWarning
	case u100 >= total*noticePct:
		s.Score = DashboardScoreNotice
	default:
		s.Score = DashboardScoreOK
	}
}

type ScopeStat struct {
	Score  DashboardScore `json:"score"`
	Scopes []ScopeUsage   `json:"scopes"`
}

// CalcScore any scope whose Current > MaxValue/2 sets Notice; otherwise OK.
func (su *ScopeStat) CalcScore() {
	for _, s := range su.Scopes {
		if s.Current > s.MaxValue/2 {
			su.Score = DashboardScoreNotice
			return
		}
	}
	su.Score = DashboardScoreOK
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
	score := DashboardScoreOK

	idcScore := func(byIDC map[string]int) DashboardScore {
		sc := DashboardScoreOK
		for _, cnt := range byIDC {
			switch cnt {
			case 0:
				sc = sc.Max(DashboardScoreMajor)
			case 1:
				sc = sc.Max(DashboardScoreWarning)
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
			score = score.Max(DashboardScoreMajor)
		} else {
			score = score.Max(idcScore(byIDC))
		}
	}

	if byIDC, ok := s.OnlineByTypeIDC[proto.ServiceNameScheduler]; ok {
		total := 0
		for _, cnt := range byIDC {
			total += cnt
		}
		if total == 0 {
			score = score.Max(DashboardScoreWarning)
		}
	} else {
		score = score.Max(DashboardScoreWarning)
	}

	if s.ExpiredDisks > 0 {
		score = score.Max(DashboardScoreNotice)
	}
	if s.ExpiredDisks > 120 {
		score = score.Max(DashboardScoreWarning)
	}

	s.Score = score
}

func (c *Client) Dashboard(ctx context.Context, args *DashboardArgs) (ret ClusterDashboard, err error) {
	err = c.GetWith(ctx, "/cluster/dashboard?force="+util.Any2String(args.Force), &ret)
	return
}
