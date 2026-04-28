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

type ClusterDashboard struct {
	Score       DashboardScore `json:"score"`
	GeneratedAt int64          `json:"generated_at"` // Unix nanoseconds

	Scope ScopeStat `json:"scope"`
}

type ScopeStat struct {
	Score  DashboardScore `json:"score"`
	Scopes []ScopeUsage   `json:"scopes"`
}

// ScopeUsage records the current allocation progress of a single scope counter.
type ScopeUsage struct {
	Name     string `json:"name"`
	Current  uint64 `json:"current"`
	MaxValue uint64 `json:"max_value"`
}

func (c *Client) Dashboard(ctx context.Context, args *DashboardArgs) (ret ClusterDashboard, err error) {
	err = c.GetWith(ctx, "/cluster/dashboard?force="+util.Any2String(args.Force), &ret)
	return
}
