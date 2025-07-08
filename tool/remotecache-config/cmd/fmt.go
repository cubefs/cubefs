package cmd

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/cubefs/cubefs/proto"
)

var (
	formatFlashNodeSimpleViewTableTitle = arow("Zone", "ID", "Address", "Active", "Enable", "FlashGroupID", "ReportTime")
	formatFlashNodeViewTableTitle       = append(formatFlashNodeSimpleViewTableTitle[:], "DataPath", "HitRate", "Evicts", "Limit", "MaxAlloc", "HasAlloc", "Num", "Status")
	formatFlashGroupViewTile            = arow("ID", "Weight", "Slots", "ReservedSlots", "Status", "SlotStatus", "PendingSlots", "Step", "FlashNodeCount", "ReducingSlots")
)

func formatClusterView(cv *proto.ClusterView) string {
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("  Cluster name       : %v\n", cv.Name))
	sb.WriteString(fmt.Sprintf("  Master leader      : %v\n", cv.LeaderAddr))
	for _, master := range cv.MasterNodes {
		sb.WriteString(fmt.Sprintf("  Master-%d           : %v\n", master.ID, master.Addr))
	}
	sb.WriteString(fmt.Sprintf("  FlashNodeHandleReadTimeout       : %v ms\n", cv.FlashNodeHandleReadTimeout))
	sb.WriteString(fmt.Sprintf("  FlashNodeReadDataNodeTimeout     : %v ms\n", cv.FlashNodeReadDataNodeTimeout))
	sb.WriteString(fmt.Sprintf("  RemoteCacheTTL                   : %v s\n", cv.RemoteCacheTTL))
	sb.WriteString(fmt.Sprintf("  RemoteCacheReadTimeout           : %v ms\n", cv.RemoteCacheReadTimeout))
	sb.WriteString(fmt.Sprintf("  RemoteCacheMultiRead             : %v\n", cv.RemoteCacheMultiRead))
	sb.WriteString(fmt.Sprintf("  FlashNodeTimeoutCount            : %v\n", cv.FlashNodeTimeoutCount))
	sb.WriteString(fmt.Sprintf("  RemoteCacheSameZoneTimeout       : %v microsecond\n", cv.RemoteCacheSameZoneTimeout))
	sb.WriteString(fmt.Sprintf("  RemoteCacheSameRegionTimeout     : %v millisecond\n", cv.RemoteCacheSameRegionTimeout))
	sb.WriteString(fmt.Sprintf("  FlashHotKeyMissCount             : %v\n", cv.FlashHotKeyMissCount))
	return sb.String()
}

func formatFlashNodeView(fn *proto.FlashNodeViewInfo) string {
	return "[FlashNode]\n" + alignColumn(
		arow("  ID", fn.ID),
		arow("  Address", fn.Addr),
		arow("  Version", fn.Version),
		arow("  ZoneName", fn.ZoneName),
		arow("  FlashGroupID", fn.FlashGroupID),
		arow("  ReportTime", formatTimeToString(fn.ReportTime)),
		arow("  IsActive", fn.IsActive),
		arow("  IsEnable", fn.IsEnable),
	)
}

func formatTimeToString(t time.Time) string {
	return t.Format("2006-01-02 15:04:05")
}

func formatIndent(v interface{}) string {
	b, _ := json.MarshalIndent(v, "", "  ")
	return string(b)
}

var (
	flashnodeSlotStatTablePattern = "%-12v    %-12v    %-12v    %-12v    %-12v\n"
	flashnodeSlotStatTableHeader  = fmt.Sprintf(flashnodeSlotStatTablePattern,
		"SlotId", "OwnerSlotId", "HitCount", "HitRate", "RecentTime")
)

func formatFlashNodeSlotStat(stat *proto.FlashNodeSlotStat) string {
	sb := strings.Builder{}
	sb.WriteString(flashnodeSlotStatTableHeader)
	for _, slot := range stat.SlotStat {
		hitrate := fmt.Sprintf("%.2f%%", slot.HitRate*100)
		recentTime := formatTimeToString(slot.RecentTime)
		sb.WriteString(fmt.Sprintf(flashnodeSlotStatTablePattern, slot.SlotId, slot.OwnerSlotId, slot.HitCount, hitrate, recentTime))
	}

	return sb.String()
}

func formatYesNo(b bool) string {
	if b {
		return "Yes"
	}
	return "No"
}

func formatFlashGroupView(fg *proto.FlashGroupAdminView) string {
	return "[FlashGroup]\n" +
		fmt.Sprintf("  ID:%v\n", fg.ID) +
		fmt.Sprintf("  Weight:%v\n", fg.Weight) +
		fmt.Sprintf("  Slots:%v\n", fg.Slots) +
		fmt.Sprintf("  ReservedSlots:%v\n", fg.ReservedSlots) +
		fmt.Sprintf("  IsReducingSlots:%v\n", fg.IsReducingSlots) +
		fmt.Sprintf("  Status:%v\n", fg.Status) +
		fmt.Sprintf("  SlotStatus:%v\n", fg.SlotStatus) +
		fmt.Sprintf("  PedningSlots:%v\n", fg.PendingSlots) +
		fmt.Sprintf("  Step:%v\n", fg.Step) +
		fmt.Sprintf("  FlashNodeCount:%v\n", fg.FlashNodeCount)
}
