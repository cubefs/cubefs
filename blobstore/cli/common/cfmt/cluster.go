// Copyright 2022 The CubeFS Authors.
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

package cfmt

import (
	"fmt"

	"github.com/dustin/go-humanize"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/cli/common"
)

// VolumeInfoJoin volume info
func VolumeInfoJoin(vol *clustermgr.VolumeInfo, prefix string) string {
	return joinWithPrefix(prefix, VolumeInfoF(vol))
}

// VolumeInfoF volume info
func VolumeInfoF(vol *clustermgr.VolumeInfo) []string {
	if vol == nil {
		return []string{" <nil> "}
	}
	usedC := common.ColorizeUint64(vol.Used, vol.Total)
	freeC := common.ColorizeUint64Free(vol.Free, vol.Total)
	vals := make([]string, 0, 32)
	vals = append(vals, []string{
		fmt.Sprintf("Vid        : %d", vol.Vid),
		fmt.Sprintf("CodeMode   : %-12d (%s)", vol.CodeMode, vol.CodeMode.String()),
		fmt.Sprintf("Status     : %-12d (%s)", vol.Status, vol.Status.String()),
		fmt.Sprintf("HealthScore: %d", vol.HealthScore),
		fmt.Sprintf("Total      : %-16d (%s)", vol.Total, humanize.IBytes(vol.Total)),
		fmt.Sprintf("Free       : %-16d (%s)", vol.Free, freeC.Sprint(humanize.IBytes(vol.Free))),
		fmt.Sprintf("Used       : %-16d (%s)", vol.Used, usedC.Sprint(humanize.IBytes(vol.Used))),
		fmt.Sprintf("Uints: (%d) [", len(vol.Units)),
	}...)
	alterColor := common.NewAlternateColor(3)
	for idx, unit := range vol.Units {
		vals = append(vals, alterColor.Next().Sprintf(" >:%3d| Vuid: %s | DiskID: %-10d | Host: %s",
			idx, VuidF(unit.Vuid), unit.DiskID, unit.Host))
	}
	vals = append(vals, "]")
	return vals
}

// ClusterInfoJoin cluster info
func ClusterInfoJoin(info *clustermgr.ClusterInfo, prefix string) string {
	return joinWithPrefix(prefix, ClusterInfoF(info))
}

// ClusterInfoF cluster info
func ClusterInfoF(info *clustermgr.ClusterInfo) []string {
	if info == nil {
		return []string{" <nil> "}
	}
	avaiC := common.ColorizeInt64(-info.Available, info.Capacity)
	vals := make([]string, 0, 8)
	vals = append(vals, []string{
		fmt.Sprintf("Region    : %s", info.Region),
		fmt.Sprintf("ClusterID : %d", info.ClusterID),
		fmt.Sprintf("Readonly  : %v", info.Readonly),
		fmt.Sprintf("Capacity  : %-16d (%s)", info.Capacity, humanize.IBytes(uint64(info.Capacity))),
		fmt.Sprintf("Available : %-16d (%s)", info.Available, avaiC.Sprint(humanize.IBytes(uint64(info.Available)))),
		fmt.Sprintf("Nodes: (%d) [", len(info.Nodes)),
	}...)
	for _, node := range info.Nodes {
		vals = append(vals, fmt.Sprintf("  - %s", node))
	}
	vals = append(vals, "]")
	return vals
}
