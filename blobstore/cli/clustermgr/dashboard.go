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
	"sort"
	"strings"
	"time"

	"github.com/desertbit/grumble"
	"github.com/fatih/color"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/util"
	"github.com/cubefs/cubefs/blobstore/util/tablefmt"
)

var dashboardSections = []string{"scope", "service", "disk", "volume", "safety"}

func addCmdDashboard(cmd *grumble.Command) {
	command := &grumble.Command{
		Name: "dashboard",
		Help: "show cluster dashboard",
		LongHelp: "cluster health overview: scope, service, disk, volume, safety\n" +
			"  sections: " + strings.Join(dashboardSections, ",") + "\n" +
			"  --nodes  simulate node shutdown instead of fetching the live snapshot",
		Run: cmdDashboard,
		Flags: func(f *grumble.Flags) {
			clusterFlags(f)
			f.BoolL("force", false, "force an immediate refresh")
			f.IntL("topn", 10, "top N disk load entries to show per codemode")
			f.StringL("section", "", "comma-separated sections to show")
			f.StringL("nodes", "", "comma-separated node IPs to simulate shutdown")
		},
	}
	cmd.AddCommand(command)
}

func cmdDashboard(c *grumble.Context) error {
	ctx := common.CmdContext()
	cmClient := newCMClient(c.Flags)

	var (
		d   clustermgr.ClusterDashboard
		err error
	)

	if raw := strings.TrimSpace(c.Flags.String("nodes")); raw != "" {
		nodes := strings.Split(raw, ",")
		for i, n := range nodes {
			nodes[i] = strings.TrimSpace(n)
		}
		fmt.Printf("Simulating shutdown of nodes: %v\n", nodes)
		d, err = cmClient.Simulate(ctx, &clustermgr.SimulateAgrs{Nodes: nodes})
	} else {
		d, err = cmClient.Dashboard(ctx, &clustermgr.DashboardArgs{Force: c.Flags.Bool("force")})
	}
	if err != nil {
		return err
	}
	printDashboard(c, d)
	return nil
}

func printDashboard(c *grumble.Context, d clustermgr.ClusterDashboard) {
	topn := c.Flags.Int("topn")

	// parse --section into a set; empty means show all
	showSet := make(map[string]bool)
	if raw := strings.TrimSpace(c.Flags.String("section")); raw != "" {
		for _, part := range strings.Split(raw, ",") {
			s := strings.TrimSpace(strings.ToLower(part))
			if s != "" {
				showSet[s] = true
			}
		}
	}
	showAll := len(showSet) == 0
	show := func(sec string) bool { return showAll || showSet[sec] }

	fmt.Println()
	if show("scope") {
		printDashboardScope(d.Scope)
	}
	if show("service") {
		printDashboardService(d.Service)
	}
	if show("disk") {
		printDashboardDisk(d.Disk)
	}
	if show("volume") {
		printDashboardVolume(d.Volume, topn)
	}
	if show("safety") {
		printDashboardSafety(d.VolumeSafety)
	}

	genAt := time.Unix(0, d.GeneratedAt).Format("2006-01-02 15:04:05")
	fmt.Printf("\nCluster Dashboard  score=%s  generated_at=[%s]\n", scoreStr(d.Score), genAt)
}

func printDashboardScope(s clustermgr.ScopeStat) {
	fmt.Printf("[Scope]  score=%s\n", scoreStr(s.Score))

	if len(s.Scopes) == 0 {
		fmt.Println()
		return
	}
	rows := tablefmt.Table{tablefmt.NewRow("Name", "Current", "MaxValue", "Usage%")}
	for _, sc := range s.Scopes {
		pct := ""
		if sc.MaxValue > 0 {
			pct = fmt.Sprintf("%.4f%%", float64(sc.Current)*100/float64(sc.MaxValue))
		}
		rows = rows.Append(tablefmt.NewRow(sc.Name, sc.Current, sc.MaxValue, pct))
	}
	for _, line := range tablefmt.AlignWith([]tablefmt.Alignment{tablefmt.AlignRight}, rows...) {
		fmt.Println("  " + line)
	}
	fmt.Println()
}

func printDashboardService(s clustermgr.ServiceStat) {
	fmt.Printf("[Service]  score=%s\n", scoreStr(s.Score))

	// Online by type × IDC
	if len(s.OnlineByTypeIDC) > 0 {
		fmt.Println("  Online nodes:")
		svcOrder := []string{
			proto.ServiceNameBlobNode,
			proto.ServiceNameProxy,
			proto.ServiceNameScheduler,
			proto.ServiceNameWorker,
		}
		// collect any extra service types not in the fixed order
		knownSet := make(map[string]bool)
		for _, svc := range svcOrder {
			knownSet[svc] = true
		}
		for svc := range s.OnlineByTypeIDC {
			if !knownSet[svc] {
				svcOrder = append(svcOrder, svc)
				knownSet[svc] = true
			}
		}

		idcSet := make(map[string]bool)
		for _, byIDC := range s.OnlineByTypeIDC {
			for idc := range byIDC {
				idcSet[idc] = true
			}
		}
		idcs := sortedKeys(idcSet)

		header := append([]any{"Service"}, strs2Any(idcs)...)
		rows := tablefmt.Table{tablefmt.NewRow(header...)}
		for _, svc := range svcOrder {
			byIDC, ok := s.OnlineByTypeIDC[svc]
			if !ok {
				continue
			}
			row := []any{svc}
			for _, idc := range idcs {
				row = append(row, byIDC[idc])
			}
			rows = rows.Append(tablefmt.NewRow(row...))
		}
		for _, line := range tablefmt.AlignWith([]tablefmt.Alignment{tablefmt.AlignRight}, rows...) {
			fmt.Println("  " + line)
		}
	}

	// Offline nodes
	if len(s.OfflineNodes) > 0 {
		fmt.Printf("  Offline nodes (%d):\n", len(s.OfflineNodes))
		rows := tablefmt.Table{tablefmt.NewRow("Name", "IDC", "Host")}
		for _, n := range s.OfflineNodes {
			rows = rows.Append(tablefmt.NewRow(n.Name, n.Idc, n.Host))
		}
		for _, line := range tablefmt.AlignWith(nil, rows...) {
			fmt.Println("    " + line)
		}
	}

	// Expired blobnode disks
	if s.ExpiredDisks > 0 {
		fmt.Printf("  Expired disks: %d\n", s.ExpiredDisks)
		if len(s.ExpiredByNode) > 0 {
			nodes := sortedKeys(s.ExpiredByNode)
			rows := tablefmt.Table{tablefmt.NewRow("Node", "ExpiredDiskIDs")}
			for _, node := range nodes {
				rows = rows.Append(tablefmt.NewRow(node, s.ExpiredByNode[node]))
			}
			for _, line := range tablefmt.AlignWith(nil, rows...) {
				fmt.Println("    " + line)
			}
		}
	}
	fmt.Println()
}

func printDashboardDisk(d clustermgr.DiskStat) {
	fmt.Printf("[Disk]  score=%s\n", scoreStr(d.Score))

	human := func(b int64) string { return util.HumanIBytes(b, 3) }

	var statusOrder []string
	for st := proto.DiskStatusNormal; st < proto.DiskStatusMax; st++ {
		statusOrder = append(statusOrder, st.String())
	}
	statusOrder = append(statusOrder, "__replace__", "__total__")

	for _, status := range statusOrder {
		byIDC, ok := d.ByStatusIDC[status]
		if !ok || len(byIDC) == 0 {
			continue
		}

		idcs := sortedKeys(byIDC)
		fmt.Printf("  [%s]\n", status)
		rows := tablefmt.Table{tablefmt.NewRow(
			"IDC", "Count", "Used", "Free", "Total",
			"MaxChunk", "FreeChunk", "UsedChunk", "OversoldChunk",
		)}
		tot := clustermgr.DiskEntry{}
		for _, idc := range idcs {
			e := byIDC[idc]
			rows = rows.Append(tablefmt.NewRow(
				idc, e.Count, human(e.UsedBytes), human(e.FreeBytes), human(e.TotalBytes),
				e.MaxChunks, e.FreeChunks, e.UsedChunks, e.OversoldChunks,
			))
			tot.Count += e.Count
			tot.UsedBytes += e.UsedBytes
			tot.FreeBytes += e.FreeBytes
			tot.TotalBytes += e.TotalBytes
			tot.MaxChunks += e.MaxChunks
			tot.FreeChunks += e.FreeChunks
			tot.UsedChunks += e.UsedChunks
			tot.OversoldChunks += e.OversoldChunks
		}
		rows = rows.Append(tablefmt.NewRow(
			"TOTAL", tot.Count, human(tot.UsedBytes), human(tot.FreeBytes), human(tot.TotalBytes),
			tot.MaxChunks, tot.FreeChunks, tot.UsedChunks, tot.OversoldChunks,
		))
		lines := tablefmt.AlignWith([]tablefmt.Alignment{tablefmt.AlignRight}, rows...)
		for _, line := range tablefmt.Summary(lines) {
			fmt.Println("    " + line)
		}
	}
	fmt.Println()
}

func printDashboardVolume(v clustermgr.VolumeStat, topn int) {
	fmt.Printf("[Volume]  score=%s\n", scoreStr(v.Score))

	human := func(b int64) string { return util.HumanIBytes(b, 3) }
	st := v.Status

	// Status summary
	rows := tablefmt.Table{tablefmt.NewRow("Total", "Active", "Active-Healthy", "Active-Unhealthy", "Idle", "Other")}
	rows = rows.Append(tablefmt.NewRow(
		st.Total, st.ActiveTotal, st.ActiveHealthy, st.ActiveUnhealthy, st.IdleTotal, st.OtherTotal,
	))
	for _, line := range tablefmt.AlignWith([]tablefmt.Alignment{tablefmt.AlignRight}, rows...) {
		fmt.Println("  " + line)
	}
	fmt.Println()

	if len(v.ByScore) > 0 {
		fmt.Println("  [All by Score]")
		printVolumeScoreStat(v.ByScore, human)
	}
	if len(v.AllocatableByScore) > 0 {
		fmt.Println("  [Allocatable by Score]")
		printVolumeScoreStat(v.AllocatableByScore, human)
	}

	if len(v.ByFree) > 0 {
		fmt.Println("  [All by Free%]")
		printVolumeFreeStat(v.ByFree, human)
	}
	if len(v.AllocatableByFree) > 0 {
		fmt.Println("  [Allocatable by Free%]")
		printVolumeFreeStat(v.AllocatableByFree, human)
	}

	if len(v.TopDiskLoad) > 0 && topn > 0 {
		fmt.Println("  [Top Disk Load]")
		for _, tl := range v.TopDiskLoad {
			label := tl.CodeMode
			if label == "" {
				label = "ALL"
			}
			fmt.Printf("    CodeMode=%s  total_active_units=%d\n", label, tl.Total)
			n := util.Min(topn, len(tl.TopN))
			if n == 0 {
				continue
			}
			rows := tablefmt.Table{tablefmt.NewRow("Rank", "DiskID", "Load")}
			for i := 0; i < n; i++ {
				e := tl.TopN[i]
				rows = rows.Append(tablefmt.NewRow(i+1, e.DiskID, e.Load))
			}
			for _, line := range tablefmt.AlignWith([]tablefmt.Alignment{tablefmt.AlignRight}, rows...) {
				fmt.Println("      " + line)
			}
		}
	}
	fmt.Println()
}

func printVolumeScoreStat(stat clustermgr.VolumeScoreStat, human func(int64) string) {
	cms := sortedKeys(stat)
	for _, cm := range cms {
		scoreMap := stat[cm]
		fmt.Printf("    [%s]\n", cm)

		scoreList := make([]int, 0, len(scoreMap))
		for score := range scoreMap {
			scoreList = append(scoreList, score)
		}
		sort.Ints(scoreList)

		rows := tablefmt.Table{tablefmt.NewRow("Score", "Count", "Free", "Used", "Total")}
		sub := clustermgr.VolumeStatEntry{}
		for _, score := range scoreList {
			e := scoreMap[score]
			rows = rows.Append(tablefmt.NewRow(
				score, e.Count, human(e.FreeBytes), human(e.UsedBytes), human(e.TotalBytes),
			))
			sub.Count += e.Count
			sub.FreeBytes += e.FreeBytes
			sub.UsedBytes += e.UsedBytes
			sub.TotalBytes += e.TotalBytes
		}
		rows = rows.Append(tablefmt.NewRow(
			"TOTAL", sub.Count, human(sub.FreeBytes), human(sub.UsedBytes), human(sub.TotalBytes),
		))
		lines := tablefmt.AlignWith([]tablefmt.Alignment{tablefmt.AlignRight}, rows...)
		for _, line := range tablefmt.Summary(lines) {
			fmt.Println("      " + line)
		}
	}
}

func printVolumeFreeStat(stat clustermgr.VolumeFreeStat, human func(int64) string) {
	cms := sortedKeys(stat)
	for _, cm := range cms {
		bucketMap := stat[cm]
		fmt.Printf("    [%s]\n", cm)

		buckets := sortedKeys(bucketMap)
		rows := tablefmt.Table{tablefmt.NewRow("Free%≤", "Count", "Free", "Used", "Total")}
		sub := clustermgr.VolumeStatEntry{}
		for _, bucket := range buckets {
			e := bucketMap[bucket]
			rows = rows.Append(tablefmt.NewRow(
				bucket+"%", e.Count, human(e.FreeBytes), human(e.UsedBytes), human(e.TotalBytes),
			))
			sub.Count += e.Count
			sub.FreeBytes += e.FreeBytes
			sub.UsedBytes += e.UsedBytes
			sub.TotalBytes += e.TotalBytes
		}
		rows = rows.Append(tablefmt.NewRow(
			"TOTAL", sub.Count, human(sub.FreeBytes), human(sub.UsedBytes), human(sub.TotalBytes),
		))
		lines := tablefmt.AlignWith([]tablefmt.Alignment{tablefmt.AlignRight}, rows...)
		for _, line := range tablefmt.Summary(lines) {
			fmt.Println("      " + line)
		}
	}
}

func printDashboardSafety(s clustermgr.VolumeSafetyStat) {
	fmt.Printf("[Volume Safety]  score=%s\n", scoreStr(s.Score))

	rows := tablefmt.Table{tablefmt.NewRow("Safe", "Degraded", "AtRisk", "DataLoss")}
	rows = rows.Append(tablefmt.NewRow(
		s.SafeVolumes, s.DegradedVolumes, s.AtRiskVolumes, s.DataLossVolumes,
	))
	for _, line := range tablefmt.AlignWith([]tablefmt.Alignment{tablefmt.AlignRight}, rows...) {
		fmt.Println("  " + line)
	}

	if len(s.UnsafeDetails) > 0 {
		fmt.Printf("  Unsafe volumes (%d):\n", len(s.UnsafeDetails))
		rows := tablefmt.Table{tablefmt.NewRow("Vid", "CodeMode", "Level", "UnsafeUnits", "UnsafeDiskIDs")}
		for _, e := range s.UnsafeDetails {
			rows = rows.Append(tablefmt.NewRow(
				e.Vid, e.CodeMode.Name(), e.Level, e.UnsafeUnits, e.UnsafeDiskIDs,
			))
		}
		for _, line := range tablefmt.AlignWith([]tablefmt.Alignment{tablefmt.AlignRight}, rows...) {
			fmt.Println("    " + line)
		}
	}
	fmt.Println()
}

func strs2Any(ss []string) []any {
	r := make([]any, len(ss))
	for i, s := range ss {
		r[i] = s
	}
	return r
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func scoreStr(score clustermgr.DashboardScore) string {
	colors := [...]*color.Color{
		common.Optimal,
		common.Normal,
		common.Warn,
		common.Danger,
		common.Dead,
	}
	label := score.String()
	if score.Score < len(colors) {
		label = colors[score.Score].Sprint(label)
	}
	if score.Reason != "" {
		label += "  (" + score.Reason + ")"
	}
	return label
}
