// Copyright 2023 The CubeFS Authors.
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

package cmd

import (
	"fmt"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
)

func TestFmtFlashNode(t *testing.T) {
	fnView := proto.FlashNodeViewInfo{
		ID:           0xff,
		Addr:         "a.b.c.c:80",
		ReportTime:   time.Now(),
		IsActive:     true,
		Version:      "",
		ZoneName:     "zoneName",
		FlashGroupID: 0xee,
		IsEnable:     false,
	}
	stdoutln(formatFlashNodeView(&fnView))

	zoneNodes := make(map[string][]*proto.FlashNodeViewInfo)
	for idxZ, zone := range []string{"z1", "z2", "z3"} {
		for idxI := range [5]struct{}{} {
			fn := fnView
			fn.ID = uint64(idxZ * idxI)
			fn.ZoneName = zone
			fn.IsEnable = (fn.ID % 2) == 0
			zoneNodes[zone] = append(zoneNodes[zone], &fn)
		}
	}
	stdoutln()
	stdoutln("[Flash Nodes]")
	stdoutln(formatFlashNodeViewTableHeader())
	for _, nodes := range zoneNodes {
		for _, fn := range nodes {
			hitRate, evicts, limit := "N/A", "N/A", "N/A"
			if fn.IsActive && fn.IsEnable {
				hitRate = fmt.Sprintf("%.2f%%", 0.66666*100)
				evicts = "100"
				limit = "1024000"
			}
			stdoutlnf(flashNodeViewTableRowPattern, fn.ZoneName, fn.ID, fn.Addr,
				formatYesNo(fn.IsActive), formatYesNo(fn.IsEnable),
				fn.FlashGroupID, formatTime(fn.ReportTime.Unix()), hitRate, evicts, limit)
		}
	}

	stdoutln()
	for zone, nodes := range zoneNodes {
		stdoutln("[Flash Nodes]", zone)
		stdoutln(formatFlashNodeSimpleViewTableHeader())
		showFlashNodesView(nodes, false)
	}
}
