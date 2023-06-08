// Copyright 2018 The CubeFS Authors.
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
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cubefs/cubefs/proto"
)

func formatClusterView(cv *proto.ClusterView) string {
	var sb = strings.Builder{}
	sb.WriteString(fmt.Sprintf("  Cluster name     : %v\n", cv.Name))
	sb.WriteString(fmt.Sprintf("  Master leader    : %v\n", cv.LeaderAddr))
	sb.WriteString(fmt.Sprintf("  Auto allocate    : %v\n", formatEnabledDisabled(!cv.DisableAutoAlloc)))
	sb.WriteString(fmt.Sprintf("  MetaNode count   : %v\n", len(cv.MetaNodes)))
	if cv.MetaNodeStatInfo != nil {
		sb.WriteString(fmt.Sprintf("  MetaNode used    : %v GB\n", cv.MetaNodeStatInfo.UsedGB))
		sb.WriteString(fmt.Sprintf("  MetaNode total   : %v GB\n", cv.MetaNodeStatInfo.TotalGB))
	}
	sb.WriteString(fmt.Sprintf("  DataNode count   : %v\n", len(cv.DataNodes)))
	if cv.DataNodeStatInfo != nil {
		sb.WriteString(fmt.Sprintf("  DataNode used    : %v GB\n", cv.DataNodeStatInfo.UsedGB))
		sb.WriteString(fmt.Sprintf("  DataNode total   : %v GB\n", cv.DataNodeStatInfo.TotalGB))
	}
	sb.WriteString(fmt.Sprintf("  Volume count     : %v\n", cv.VolCount))
	sb.WriteString(fmt.Sprintf("  Dp recover pool  : %v\n", cv.DpRecoverPool))
	sb.WriteString(fmt.Sprintf("  Mp recover pool  : %v\n", cv.MpRecoverPool))
	sb.WriteString(fmt.Sprintf("  Client pkg addr  : %v\n", cv.ClientPkgAddr))
	sb.WriteString(fmt.Sprintf("  UMP jmtp addr    : %v\n", cv.UmpJmtpAddr))
	sb.WriteString(fmt.Sprintf("  UMP jmtp batch   : %v\n", cv.UmpJmtpBatch))
	sb.WriteString(fmt.Sprintf("  EcNode count     : %v\n", len(cv.EcNodes)))
	if cv.EcNodeStatInfo != nil {
		sb.WriteString(fmt.Sprintf("  EcNode used      : %v GB\n", cv.EcNodeStatInfo.UsedGB))
		sb.WriteString(fmt.Sprintf("  EcNode total     : %v GB\n", cv.EcNodeStatInfo.TotalGB))
	}
	sb.WriteString(fmt.Sprintf("  Ec scrub enable  : %v\n", cv.EcScrubEnable))
	sb.WriteString(fmt.Sprintf("  Ec scrub period  : %v min\n", cv.EcScrubPeriod))
	sb.WriteString(fmt.Sprintf("  Ec scrub extents : %v\n", cv.EcMaxScrubExtents))
	sb.WriteString(fmt.Sprintf("  codec concurrent : %v\n", cv.MaxCodecConcurrent))
	sb.WriteString(fmt.Sprintf("  Strict vol zone  : %v\n", formatEnabledDisabled(!cv.DisableStrictVolZone)))
	sb.WriteString(fmt.Sprintf("  Auto Update Partition Replica Num  : %v\n", formatEnabledDisabled(cv.AutoUpdatePartitionReplicaNum)))
	sb.WriteString(fmt.Sprintf("  DelVolAfterMarkDel   : %v(%v sec)\n", formatTimeInterval(cv.DeleteMarkDelVolInterval), cv.DeleteMarkDelVolInterval))
	if cv.EcScrubEnable {
		startScrubTime := time.Unix(cv.EcScrubStartTime, 0).Format(time.RFC1123)
		sb.WriteString(fmt.Sprintf("  Ec start scrub   : %v \n", startScrubTime))
	}

	return sb.String()
}

var (
	statInfoTablePattern = "    %-15v    %-15v    %-15v    %-15v\n"
	statInfoTableHeader  = fmt.Sprintf(statInfoTablePattern,
		"TOTAL/GB", "USED/GB", "INCREASED/GB", "USED RATIO")
	zoneStatInfoTablePattern = "    %-10v   %-10v  %-15v    %-15v    %-15v    %-15v    %-10v    %-10v\n"
	zoneStatInfoTableHeader  = fmt.Sprintf(zoneStatInfoTablePattern,
		"ZONE NAME", "ROLE", "TOTAL/GB", "USED/GB", "AVAILABLE/GB ", "USED RATIO", "TOTAL NODES", "WRITEBLE NODES")
)

func formatClusterStat(cs *proto.ClusterStatInfo) string {
	var sb = strings.Builder{}
	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("DataNode Status:\n"))
	sb.WriteString(statInfoTableHeader)
	sb.WriteString(fmt.Sprintf(statInfoTablePattern, cs.DataNodeStatInfo.TotalGB, cs.DataNodeStatInfo.UsedGB, cs.DataNodeStatInfo.IncreasedGB, cs.DataNodeStatInfo.UsedRatio))
	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("MetaNode Status:\n"))
	sb.WriteString(statInfoTableHeader)
	sb.WriteString(fmt.Sprintf(statInfoTablePattern, cs.MetaNodeStatInfo.TotalGB, cs.MetaNodeStatInfo.UsedGB, cs.MetaNodeStatInfo.IncreasedGB, cs.MetaNodeStatInfo.UsedRatio))
	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("Zone List:\n"))
	sb.WriteString(zoneStatInfoTableHeader)
	for zoneName, zoneStat := range cs.ZoneStatInfo {
		sb.WriteString(fmt.Sprintf(zoneStatInfoTablePattern, zoneName, "DATANODE", zoneStat.DataNodeStat.Total, zoneStat.DataNodeStat.Used, zoneStat.DataNodeStat.Avail, zoneStat.DataNodeStat.UsedRatio, zoneStat.DataNodeStat.TotalNodes, zoneStat.DataNodeStat.WritableNodes))
		sb.WriteString(fmt.Sprintf(zoneStatInfoTablePattern, "", "METANODE", zoneStat.MetaNodeStat.Total, zoneStat.MetaNodeStat.Used, zoneStat.MetaNodeStat.Avail, zoneStat.MetaNodeStat.UsedRatio, zoneStat.MetaNodeStat.TotalNodes, zoneStat.MetaNodeStat.WritableNodes))
	}
	return sb.String()
}

var nodeViewTableRowPattern = "%-6v    %-18v    %-8v    %-8v    %-8v"
var dataNodeDetailViewTableRowPattern = "%-6v    %-18v    %-8v    %-8v    %-8v    %-8v    %-8v    %-8v    %-8v"
var metaNodeDetailViewTableRowPattern = "%-6v    %-18v    %-8v    %-8v    %-8v    %-8v    %-8v    %-8v    %-8v"

func formatNodeViewTableHeader() string {
	return fmt.Sprintf(nodeViewTableRowPattern, "ID", "ADDRESS", "VERSION", "WRITABLE", "STATUS")
}

func formatDataNodeViewTableHeader() string {
	return fmt.Sprintf(dataNodeDetailViewTableRowPattern, "ID", "ADDRESS", "VERSION", "WRITABLE", "STATUS", "USED", "RATIO", "ZONE", "DP COUNT")
}

func formatMetaNodeViewTableHeader() string {
	return fmt.Sprintf(metaNodeDetailViewTableRowPattern, "ID", "ADDRESS", "VERSION", "WRITABLE", "STATUS", "USED", "RATIO", "ZONE", "MP COUNT")
}

func formatNodeView(view *proto.NodeView, tableRow bool) string {
	if tableRow {
		return fmt.Sprintf(nodeViewTableRowPattern, view.ID, view.Addr, view.Version,
			formatYesNo(view.IsWritable), formatNodeStatus(view.Status))
	}
	var sb = strings.Builder{}
	sb.WriteString(fmt.Sprintf("  ID      : %v\n", view.ID))
	sb.WriteString(fmt.Sprintf("  Address : %v\n", view.Addr))
	sb.WriteString(fmt.Sprintf("  Version : %v\n", view.Version))
	sb.WriteString(fmt.Sprintf("  Writable: %v\n", formatYesNo(view.IsWritable)))
	sb.WriteString(fmt.Sprintf("  Status  : %v", formatNodeStatus(view.Status)))
	return sb.String()
}

func formatSimpleVolView(svv *proto.SimpleVolView) string {

	var sb = strings.Builder{}
	sb.WriteString(fmt.Sprintf("  ID                   : %v\n", svv.ID))
	sb.WriteString(fmt.Sprintf("  Name                 : %v\n", svv.Name))
	sb.WriteString(fmt.Sprintf("  Owner                : %v\n", svv.Owner))
	sb.WriteString(fmt.Sprintf("  Zone                 : %v\n", svv.ZoneName))
	sb.WriteString(fmt.Sprintf("  Status               : %v\n", formatVolumeStatus(svv.Status)))
	sb.WriteString(fmt.Sprintf("  EcEnable             : %v\n", svv.EcEnable))
	sb.WriteString(fmt.Sprintf("  Capacity             : %v GB\n", svv.Capacity))
	sb.WriteString(fmt.Sprintf("  Used                 : %v GB\n", svv.UsedSizeGB))
	sb.WriteString(fmt.Sprintf("  Create time          : %v\n", svv.CreateTime))
	sb.WriteString(fmt.Sprintf("  Authenticate         : %v\n", formatEnabledDisabled(svv.Authenticate)))
	sb.WriteString(fmt.Sprintf("  Follower read        : %v\n", formatEnabledDisabled(svv.FollowerRead)))
	sb.WriteString(fmt.Sprintf("  Near read            : %v\n", formatEnabledDisabled(svv.NearRead)))
	sb.WriteString(fmt.Sprintf("  Force ROW            : %v\n", formatEnabledDisabled(svv.ForceROW)))
	sb.WriteString(fmt.Sprintf("  Enable Write Cache   : %v\n", formatEnabledDisabled(svv.EnableWriteCache)))
	sb.WriteString(fmt.Sprintf("  Cross zone           : %v\n", formatEnabledDisabled(svv.CrossZone)))
	sb.WriteString(fmt.Sprintf("  Cross Region HA      : %s\n", svv.CrossRegionHAType))
	if svv.MasterRegionZone != "" {
		sb.WriteString(fmt.Sprintf("  Master region zone   : %s\n", svv.MasterRegionZone))
	}
	if svv.SlaveRegionZone != "" {
		sb.WriteString(fmt.Sprintf("  Slave region zone    : %s\n", svv.SlaveRegionZone))
	}
	sb.WriteString(fmt.Sprintf("  Quorum               : %v\n", svv.Quorum))
	sb.WriteString(fmt.Sprintf("  Auto repair          : %v\n", formatEnabledDisabled(svv.AutoRepair)))
	sb.WriteString(fmt.Sprintf("  Volume write mutex   : %v\n", formatEnabledDisabled(svv.VolWriteMutexEnable)))
	sb.WriteString(fmt.Sprintf("  Trash remaining days : %v\n", svv.TrashRemainingDays))
	sb.WriteString(fmt.Sprintf("  Inode count          : %v\n", svv.InodeCount))
	sb.WriteString(fmt.Sprintf("  Dentry count         : %v\n", svv.DentryCount))
	sb.WriteString(fmt.Sprintf("  Max metaPartition ID : %v\n", svv.MaxMetaPartitionID))
	sb.WriteString(fmt.Sprintf("  Meta partition count : %v\n", svv.MpCnt))
	sb.WriteString(fmt.Sprintf("  Meta replicas        : %v\n", svv.MpReplicaNum))
	sb.WriteString(fmt.Sprintf("  Meta learner num     : %v\n", svv.MpLearnerNum))
	sb.WriteString(fmt.Sprintf("  Data partition count : %v\n", svv.DpCnt))
	sb.WriteString(fmt.Sprintf("  Data replicas        : %v\n", svv.DpReplicaNum))
	sb.WriteString(fmt.Sprintf("  Data learner num     : %v\n", svv.DpLearnerNum))
	sb.WriteString(fmt.Sprintf("  DpSelectorName       : %v\n", svv.DpSelectorName))
	sb.WriteString(fmt.Sprintf("  DpSelectorParm       : %v\n", svv.DpSelectorParm))
	sb.WriteString(fmt.Sprintf("  OSS bucket policy    : %s\n", svv.OSSBucketPolicy))
	sb.WriteString(fmt.Sprintf("  DP convert mode      : %s\n", svv.DPConvertMode))
	sb.WriteString(fmt.Sprintf("  MP convert mode      : %s\n", svv.MPConvertMode))
	sb.WriteString(fmt.Sprintf("  ExtentCache Expire   : %v\n", time.Duration(svv.ExtentCacheExpireSec)*time.Second))
	sb.WriteString(fmt.Sprintf("  writable mp count    : %v\n", svv.RwMpCnt))
	sb.WriteString(fmt.Sprintf("  writable dp count    : %v\n", svv.RwDpCnt))
	sb.WriteString(fmt.Sprintf("  min writable mp num  : %v\n", svv.MinWritableMPNum))
	sb.WriteString(fmt.Sprintf("  min writable dp num  : %v\n", svv.MinWritableDPNum))
	sb.WriteString(fmt.Sprintf("  Default store mode   : %v\n", svv.DefaultStoreMode.Str()))
	sb.WriteString(fmt.Sprintf("  Convert state        : %v\n", svv.ConvertState.Str()))
	sb.WriteString(fmt.Sprintf("  Meta layout          : %3d%% - %3d%%\n", svv.MpLayout.PercentOfMP, svv.MpLayout.PercentOfReplica))
	sb.WriteString(fmt.Sprintf("  ump collect way      : %v\n", svv.UmpCollectWay))
	sb.WriteString(fmt.Sprintf("  smart                : %s\n", formatEnabledDisabled(svv.IsSmart)))
	sb.WriteString(fmt.Sprintf("  smart enable time    : %v\n", svv.SmartEnableTime))
	sb.WriteString(fmt.Sprintf("  smart rules          : %v\n", strings.Join(svv.SmartRules, ",")))
	sb.WriteString(fmt.Sprintf("  Compact              : %v\n", svv.CompactTag))
	sb.WriteString(fmt.Sprintf("  Host Delay Interval  : %v\n", svv.DpFolReadDelayConfig.DelaySummaryInterval))
	sb.WriteString(fmt.Sprintf("  LowestDelay Host Weight: %v%%\n", svv.FolReadHostWeight))
	sb.WriteString(fmt.Sprintf("  Child File Max Count : %v\n", svv.ChildFileMaxCount))
	sb.WriteString(fmt.Sprintf("  Trash clean interval : %v\n", svv.TrashCleanInterval))
	sb.WriteString(fmt.Sprintf("  Batch del inode cnt  : %v\n", svv.BatchDelInodeCnt))
	sb.WriteString(fmt.Sprintf("  Del ino interval(ms) : %v\n", svv.DelInodeInterval))
	sb.WriteString(fmt.Sprintf("  BitMapAllocator      : %v\n", formatEnabledDisabled(svv.EnableBitMapAllocator)))
	sb.WriteString(fmt.Sprintf("  TrashCleanDuration   : %v\n", svv.TrashCleanDuration))
	sb.WriteString(fmt.Sprintf("  TrashCleanMaxCount   : %v\n", svv.TrashCleanMaxCount))
	if svv.NewVolName == "" && svv.OldVolName == "" && svv.RenameConvertStatus == 0 {
		sb.WriteString(fmt.Sprintf("  rename convert status: No Rename Operation\n"))
	} else {
		sb.WriteString(fmt.Sprintf("  rename convert status: %s\n", svv.RenameConvertStatus))
	}
	sb.WriteString(fmt.Sprintf("  new vol name         : %v\n", svv.NewVolName))
	sb.WriteString(fmt.Sprintf("  New vol id           : %v\n", svv.NewVolID))
	sb.WriteString(fmt.Sprintf("  old vol name         : %v\n", svv.OldVolName))
	sb.WriteString(fmt.Sprintf("  final vol status     : %v\n", svv.FinalVolStatus))
	sb.WriteString(fmt.Sprintf("  mark delete time     : %v\n", formatTime(svv.MarkDeleteTime)))
	return sb.String()
}

func formatInodeInfoView(inodeInfo *proto.InodeInfoView) string {

	var sb = strings.Builder{}
	sb.WriteString(fmt.Sprintf("  InodeNumber                 : %v\n", inodeInfo.Ino))
	sb.WriteString(fmt.Sprintf("  PartitionID                 : %v\n", inodeInfo.PartitionID))
	sb.WriteString(fmt.Sprintf("  AccessTime                  : %v\n", inodeInfo.At))
	sb.WriteString(fmt.Sprintf("  CreateTime                  : %v\n", inodeInfo.Ct))
	sb.WriteString(fmt.Sprintf("  ModifyTime                  : %v\n", inodeInfo.Mt))
	sb.WriteString(fmt.Sprintf("  Nlink                       : %v\n", inodeInfo.Nlink))
	sb.WriteString(fmt.Sprintf("  Size                        : %v\n", formatIntWithThousandComma(int64(inodeInfo.Size))))
	sb.WriteString(fmt.Sprintf("  Gen                         : %v\n", inodeInfo.Gen))
	sb.WriteString(fmt.Sprintf("  Gid                         : %v\n", inodeInfo.Gid))
	sb.WriteString(fmt.Sprintf("  Uid                         : %v\n", inodeInfo.Uid))
	sb.WriteString(fmt.Sprintf("  Mode                        : %v\n", inodeInfo.Mode))
	return sb.String()
}

var (
	inodeExtentInfoTablePattern = "% -15v    % -15v    % -15v    % -10v    % -15v    % -10v    % -10v"
	inodeExtentInfoTableHeader  = fmt.Sprintf(inodeExtentInfoTablePattern, "EkStart", "EkEnd", "PartitionId", "ExtentId", "ExtentOffset", "Size", "CRC")
)

func formatInodeExtentInfoTableRow(ie *proto.InodeExtentInfoView) string {
	return fmt.Sprintf(inodeExtentInfoTablePattern,
		formatIntWithThousandComma(int64(ie.FileOffset)), formatIntWithThousandComma(int64(ie.FileOffset+ie.Size)), ie.PartitionId, ie.ExtentId, ie.ExtentOffset, formatSize(ie.Size), ie.CRC)
}

func formatVolumeStatus(status uint8) string {
	switch status {
	case proto.VolStNormal:
		return "Normal"
	case proto.VolStMarkDelete:
		return "Marked Del"
	default:
		return "Unknown"
	}
}

var (
	volumeInfoTablePattern = "%-63v    %-20v    %-8v    %-8v    %-10v    %-8v    %-10v   %-45v"
	volumeInfoTableHeader  = fmt.Sprintf(volumeInfoTablePattern, "VOLUME", "OWNER", "USED", "TOTAL", "USED RATIO", "STATUS", "IS SMART", "CREATE TIME")
)

func formatVolInfoTableRow(vi *proto.VolInfo) string {
	return fmt.Sprintf(volumeInfoTablePattern,
		vi.Name, vi.Owner, formatSize(vi.UsedSize), formatSize(vi.TotalSize), formatFloat(vi.UsedRatio),
		formatVolumeStatus(vi.Status), vi.IsSmart, time.Unix(vi.CreateTime, 0).Local().Format(time.RFC1123))
}

var (
	volumeDetailInfoTablePattern = "%-63v    %-20v    %-30v    %-10v    %-12v    %-8v    %-8v    %-8v    %-8v    %-10v	 %-45v"
	volumeDetailInfoTableHeader  = fmt.Sprintf(volumeDetailInfoTablePattern, "VOLUME", "OWNER", "ZONE NAME", "CROSS ZONE", "INODE COUNT",
		"DP COUNT", "USED", "TOTAL", "STATUS", "IS SMART", "CREATE TIME")
)

func formatVolDetailInfoTableRow(vv *proto.SimpleVolView, vi *proto.VolInfo) string {
	return fmt.Sprintf(volumeDetailInfoTablePattern,
		vv.Name, vv.Owner, vv.ZoneName, vv.CrossZone, vv.InodeCount, vv.DpCnt, formatSize(vi.UsedSize), formatSize(vi.TotalSize),
		formatVolumeStatus(vi.Status), vi.IsSmart, time.Unix(vi.CreateTime, 0).Local().Format(time.RFC1123))
}

// cfs-cli volume info [vol name] -m/-d/-e
var (
	dataPartitionTablePattern = "%-8v    %-8v    %-10v    %-10v    %-10v     %-18v    %-18v"
	dataPartitionTableHeader  = fmt.Sprintf(dataPartitionTablePattern,
		"ID", "REPLICAS", "STATUS", "EcMigrateStatus", "ISRECOVER", "LEADER", "MEMBERS")
)

func formatDataPartitionTableRow(view *proto.DataPartitionResponse) string {
	return fmt.Sprintf(dataPartitionTablePattern,
		view.PartitionID, view.ReplicaNum, formatDataPartitionStatus(view.Status), EcStatusMap[view.EcMigrateStatus], view.IsRecover, view.GetLeaderAddr(),
		strings.Join(view.Hosts, ","))
}

var (
	ecPartitionTablePattern = "%-6v   %-8v   %-8v   %-10v   %-18v"
	ecPartitionTableHeader  = fmt.Sprintf(ecPartitionTablePattern,
		"ID", "DataNum", "ParityNum", "STATUS", "MEMBERS")
)

func formatEcPartitionTableRow(view *proto.EcPartitionResponse) string {
	return fmt.Sprintf(ecPartitionTablePattern,
		view.PartitionID, view.DataUnitsNum, view.ParityUnitsNum, formatDataPartitionStatus(view.Status), strings.Join(view.Hosts, ","))
}

var (
	partitionInfoTablePattern      = "%-8v    %-25v    %-10v    %-28v    %-10v    %-18v"
	partitionInfoColorTablePattern = "%-8v    %-25v    %-10v    %-28v    \033[1;40;32m%-10v\033[0m    %-18v"
	partitionInfoTableHeader       = fmt.Sprintf(partitionInfoTablePattern,
		"ID", "VOLUME", "STATUS", "POSITION", "REPLICANUM", "HOSTS")
)

func formatDataPartitionInfoRow(partition *proto.DataPartitionInfo) string {
	var sb = strings.Builder{}
	sort.Strings(partition.Hosts)
	sb.WriteString(fmt.Sprintf(partitionInfoTablePattern+"\n",
		partition.PartitionID, partition.VolName, formatDataPartitionStatus(partition.Status), "master", fmt.Sprintf("%v/%v", len(partition.Hosts), partition.ReplicaNum), "(hosts)"+strings.Join(partition.Hosts, ",")))
	if len(partition.Learners) > 0 {
		sb.WriteString(fmt.Sprintf(partitionInfoTablePattern+"\n",
			"", "", "", "master", fmt.Sprintf("%v/%v", len(partition.Learners), len(partition.Learners)), "(learners)"+strings.Join(convertLearnersToArray(partition.Learners), ",")))
	}
	return sb.String()
}

func formatMetaPartitionInfoRow(partition *proto.MetaPartitionInfo) string {
	var sb = strings.Builder{}
	sb.WriteString(fmt.Sprintf(partitionInfoTablePattern+"\n",
		partition.PartitionID, partition.VolName, formatDataPartitionStatus(partition.Status), "master", fmt.Sprintf("%v/%v", len(partition.Hosts), partition.ReplicaNum+partition.LearnerNum), "(hosts)"+strings.Join(partition.Hosts, ",")))
	if len(partition.Learners) > 0 {
		sb.WriteString(fmt.Sprintf(partitionInfoTablePattern+"\n",
			"", "", "", "master", fmt.Sprintf("%v/%v", len(partition.Learners), partition.LearnerNum), "(learners)"+strings.Join(convertLearnersToArray(partition.Learners), ",")))
	}
	return sb.String()
}

func formatDataPartitionInfo(human bool, partition *proto.DataPartitionInfo) string {
	var sb = strings.Builder{}
	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("volume name   : %v\n", partition.VolName))
	sb.WriteString(fmt.Sprintf("volume ID     : %v\n", partition.VolID))
	sb.WriteString(fmt.Sprintf("PartitionID   : %v\n", partition.PartitionID))
	sb.WriteString(fmt.Sprintf("ReplicaNum    : %v\n", partition.ReplicaNum))
	sb.WriteString(fmt.Sprintf("Status        : %v\n", formatDataPartitionStatus(partition.Status)))
	sb.WriteString(fmt.Sprintf("CreateTime: %v\n", formatTime(partition.CreateTime)))
	sb.WriteString(fmt.Sprintf("Recovering    : %v\n", formatIsRecover(partition.IsRecover)))
	sb.WriteString(fmt.Sprintf("IsManual      : %v\n", formatIsRecover(partition.IsManual)))
	sb.WriteString(fmt.Sprintf("EcMigrateStatus : %v\n", EcStatusMap[partition.EcMigrateStatus]))
	sb.WriteString(fmt.Sprintf("LastLoadedTime: %v\n", formatTime(partition.LastLoadedTime)))
	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("Replicas : \n"))
	sb.WriteString(fmt.Sprintf("%v\n", formatDataReplicaTableHeader()))
	for _, replica := range partition.Replicas {
		sb.WriteString(fmt.Sprintf("%v\n", formatDataReplica(human, "", replica, true)))
	}
	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("Peers :\n"))
	sb.WriteString(fmt.Sprintf("%v\n", formatPeerTableHeader()))
	for _, peer := range partition.Peers {
		sb.WriteString(fmt.Sprintf("%v\n", formatPeer(peer)))
	}
	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("Learners :\n"))
	sb.WriteString(fmt.Sprintf("%v\n", formatLearnerTableHeader()))
	for _, learner := range partition.Learners {
		sb.WriteString(fmt.Sprintf("%v\n", formatLearner(learner)))
	}
	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("Hosts :\n"))
	for _, host := range partition.Hosts {
		sb.WriteString(fmt.Sprintf("  [%v]", host))
	}
	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("Zones :\n"))
	for _, zone := range partition.Zones {
		sb.WriteString(fmt.Sprintf("  [%v]", zone))
	}
	sb.WriteString("\n")
	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("MissingNodes :\n"))
	for partitionHost, id := range partition.MissingNodes {
		sb.WriteString(fmt.Sprintf("  [%v, %v]\n", partitionHost, id))
	}
	sb.WriteString(fmt.Sprintf("FilesWithMissingReplica : \n"))
	for file, id := range partition.FilesWithMissingReplica {
		sb.WriteString(fmt.Sprintf("  [%v, %v]\n", file, id))
	}
	return sb.String()
}

func formatMetaPartitionInfo(partition *proto.MetaPartitionInfo) string {
	var sb = strings.Builder{}
	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("volume name   : %v\n", partition.VolName))
	sb.WriteString(fmt.Sprintf("PartitionID   : %v\n", partition.PartitionID))
	sb.WriteString(fmt.Sprintf("ReplicaNum    : %v\n", partition.ReplicaNum))
	sb.WriteString(fmt.Sprintf("LearnerNum    : %v\n", partition.LearnerNum))
	sb.WriteString(fmt.Sprintf("Status        : %v\n", formatMetaPartitionStatus(partition.Status)))
	sb.WriteString(fmt.Sprintf("Recovering    : %v\n", formatIsRecover(partition.IsRecover)))
	sb.WriteString(fmt.Sprintf("Start         : %v\n", partition.Start))
	sb.WriteString(fmt.Sprintf("End           : %v\n", partition.End))
	sb.WriteString(fmt.Sprintf("MaxInodeID    : %v\n", partition.MaxInodeID))
	sb.WriteString(fmt.Sprintf("InodeCount    : %v\n", partition.InodeCount))
	sb.WriteString(fmt.Sprintf("DentryCount   : %v\n", partition.DentryCount))
	sb.WriteString(fmt.Sprintf("MaxExistIno   : %v\n", partition.MaxExistIno))
	sb.WriteString(fmt.Sprintf("BitMapUseCnt  : %v\n", partition.AllocatorInuseCnt))
	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("Replicas : \n"))
	sb.WriteString(fmt.Sprintf("%v\n", formatMetaReplicaTableHeader()))
	learnerReplicas := make([]*proto.MetaReplicaInfo, 0)
	for _, replica := range partition.Replicas {
		if replica.IsLearner {
			learnerReplicas = append(learnerReplicas, replica)
			continue
		}
		sb.WriteString(fmt.Sprintf("%v\n", formatMetaReplica("", replica, true)))
	}
	for _, learnerReplica := range learnerReplicas {
		sb.WriteString(fmt.Sprintf("%v\n", formatMetaReplica("", learnerReplica, true)))
	}
	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("Peers :\n"))
	sb.WriteString(fmt.Sprintf("%v\n", formatPeerTableHeader()))
	for _, peer := range partition.Peers {
		sb.WriteString(fmt.Sprintf("%v\n", formatPeer(peer)))
	}
	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("Learners :\n"))
	sb.WriteString(fmt.Sprintf("%v\n", formatLearnerTableHeader()))
	for _, learner := range partition.Learners {
		sb.WriteString(fmt.Sprintf("%v\n", formatLearner(learner)))
	}
	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("Hosts :\n"))
	for _, host := range partition.Hosts {
		sb.WriteString(fmt.Sprintf("  [%v]", host))
	}
	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("Zones :\n"))
	for _, zone := range partition.Zones {
		sb.WriteString(fmt.Sprintf("  [%v]", zone))
	}
	sb.WriteString("\n")
	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("MissingNodes :\n"))
	for partitionHost, id := range partition.MissNodes {
		sb.WriteString(fmt.Sprintf("  [%v, %v]\n", partitionHost, id))
	}
	return sb.String()
}

var (
	metaPartitionTablePattern = "%-8v    %-12v    %-12v    %-12v    %-12v    %-12v    %-12v    %-10v    %-10v    %-20v    %-18v"
	metaPartitionTableHeader  = fmt.Sprintf(metaPartitionTablePattern,
		"ID", "MAX INODE", "DENTRY COUNT", "INODE COUNT", "START", "END", "MAX EXIST INO", "STATUS", "STOREMODE", "LEADER", "MEMBERS")
	metaPartitionSnapshotCrcInfoTablePattern = "%-12v    %-12v    %-18v    %-12v    %-12v\n"
	metaPartitionSnapshotCrcInfoTableHeader  = fmt.Sprintf(metaPartitionSnapshotCrcInfoTablePattern, "LocalAddr", "Role",
		"SnapshotCreateTime", "  LeaderCrc", "  FollowerCrc")
)

func formatMetaPartitionTableRow(view *proto.MetaPartitionView) string {
	var rangeToString = func(num uint64) string {
		if num >= math.MaxInt64 {
			return "unlimited"
		}
		return strconv.FormatUint(num, 10)
	}
	return fmt.Sprintf(metaPartitionTablePattern,
		view.PartitionID, view.MaxInodeID, view.DentryCount, view.InodeCount, view.Start, rangeToString(view.End), view.MaxExistIno,
		formatMetaPartitionStatus(view.Status), view.StoreMode.Str(), view.LeaderAddr, strings.Join(view.Members, ","))
}

var (
	userInfoTablePattern = "%-20v    %-6v    %-16v    %-32v    %-10v"
	userInfoTableHeader  = fmt.Sprintf(userInfoTablePattern,
		"ID", "TYPE", "ACCESS KEY", "SECRET KEY", "CREATE TIME")
)

func formatUserInfoTableRow(userInfo *proto.UserInfo) string {
	return fmt.Sprintf(userInfoTablePattern,
		userInfo.UserID, formatUserType(userInfo.UserType), userInfo.AccessKey, userInfo.SecretKey, userInfo.CreateTime)
}

func formatDataPartitionStatus(status int8) string {
	switch status {
	case 1:
		return "Read only"
	case 2:
		return "Writable"
	case -1:
		return "Unavailable"
	default:
		return "Unknown"
	}
}

func formatIsRecover(isRecover bool) string {
	switch isRecover {
	case true:
		return "Yes"
	default:
		return "No"
	}
}

func formatMetaPartitionStatus(status int8) string {
	switch status {
	case 1:
		return "Read only"
	case 2:
		return "Writable"
	case -1:
		return "Unavailable"
	default:
		return "Unknown"
	}
}

func formatUserType(userType proto.UserType) string {
	switch userType {
	case proto.UserTypeRoot:
		return "Root"
	case proto.UserTypeAdmin:
		return "Admin"
	case proto.UserTypeNormal:
		return "Normal"
	default:
	}
	return "Unknown"
}

func formatYesNo(b bool) string {
	if b {
		return "Yes"
	}
	return "No"
}

func formatEnabledDisabled(b bool) string {
	if b {
		return "Enabled"
	}
	return "Disabled"
}

func formatNodeStatus(status bool) string {
	if status {
		return "Active"
	}
	return "Inactive"
}

var units = []string{"B", "KB", "MB", "GB", "TB", "PB"}
var step uint64 = 1024

func fixUnit(curSize uint64, curUnitIndex int) (newSize uint64, newUnitIndex int) {
	if curSize >= step && curUnitIndex < len(units)-1 {
		return fixUnit(curSize/step, curUnitIndex+1)
	}
	return curSize, curUnitIndex
}

func formatSize(size uint64) string {
	fixedSize, fixedUnitIndex := fixUnit(size, 0)
	return fmt.Sprintf("%v %v", fixedSize, units[fixedUnitIndex])
}

func formatFloat(value float64) string {
	return fmt.Sprintf("%.2v", value)
}

func formatIntWithThousandComma(n int64) string {
	var (
		groupSize int  = 3
		grouping  byte = ','
	)

	in := strconv.FormatInt(n, 10)
	numOfDigits := len(in)
	if n < 0 {
		numOfDigits-- // First character is the - sign (not a digit)
	}
	numOfCommas := (numOfDigits - 1) / groupSize

	out := make([]byte, len(in)+numOfCommas)
	if n < 0 {
		in, out[0] = in[1:], '-'
	}

	for i, j, k := len(in)-1, len(out)-1, 0; ; i, j = i-1, j-1 {
		out[j] = in[i]
		if i == 0 {
			return string(out)
		}
		if k++; k == groupSize {
			j, k = j-1, 0
			out[j] = grouping
		}
	}
}

func formatTime(timeUnix int64) string {
	return time.Unix(timeUnix, 0).Format("2006-01-02 15:04:05")
}

func formatTimeToString(t time.Time) string {
	return t.Format("2006-01-02 15:04:05")
}

func formatTimeInterval(intervalSecond int64) string {
	d := time.Duration(intervalSecond) * time.Second
	return d.String()
}

var dataReplicaTableRowPattern = "%-20v    %-12v    %-6v    %-8v    %-8v    %-12v    %-10v    %-20v    %-8v		%-8v"

func formatDataReplicaTableHeader() string {
	return fmt.Sprintf(dataReplicaTableRowPattern, "ADDRESS", "USED", "TOTAL", "ISLEADER", "RECOVER", "FILECOUNT", "STATUS", "REPORT TIME", "ISLEARNER", "MEDIUM-TYPE")
}

func formatDataReplica(human bool, indentation string, replica *proto.DataReplica, rowTable bool) string {
	var usedSize string
	if human {
		usedSize = formatSize(replica.Used)
	} else {
		usedSize = strconv.FormatUint(replica.Used, 10)
	}
	if rowTable {
		return fmt.Sprintf(dataReplicaTableRowPattern, replica.Addr, usedSize, formatSize(replica.Total),
			replica.IsLeader, replica.IsRecover, replica.FileCount, formatDataPartitionStatus(replica.Status), formatTime(replica.ReportTime), replica.IsLearner, replica.MType)
	}
	var sb = strings.Builder{}
	sb.WriteString(fmt.Sprintf("%v- Addr           : %v\n", indentation, replica.Addr))
	sb.WriteString(fmt.Sprintf("%v  Used           : %v\n", indentation, usedSize))
	sb.WriteString(fmt.Sprintf("%v  Total          : %v\n", indentation, formatSize(replica.Total)))
	sb.WriteString(fmt.Sprintf("%v  IsLeader       : %v\n", indentation, replica.IsLeader))
	sb.WriteString(fmt.Sprintf("%v  IsLearner      : %v\n", indentation, replica.IsLearner))
	sb.WriteString(fmt.Sprintf("%v  FileCount      : %v\n", indentation, replica.FileCount))
	sb.WriteString(fmt.Sprintf("%v  HasLoadResponse: %v\n", indentation, replica.HasLoadResponse))
	sb.WriteString(fmt.Sprintf("%v  NeedsToCompare : %v\n", indentation, replica.NeedsToCompare))
	sb.WriteString(fmt.Sprintf("%v  Status         : %v\n", indentation, formatDataPartitionStatus(replica.Status)))
	sb.WriteString(fmt.Sprintf("%v  DiskPath       : %v\n", indentation, replica.DiskPath))
	sb.WriteString(fmt.Sprintf("%v  ReportTime     : %v\n", indentation, formatTime(replica.ReportTime)))
	return sb.String()
}

var metaReplicaTableRowPattern = "%-20v    %-8v    %-10v    %-10v    %-16v    %-20v    %-12v    %-12v"

func formatMetaReplicaTableHeader() string {
	return fmt.Sprintf(metaReplicaTableRowPattern, "ADDRESS", "ISLEADER", "APPLY ID", "STATUS", "STORE MODE", "REPORT TIME", "ISLEARNER", "ISRECOVER")
}

func formatMetaReplica(indentation string, replica *proto.MetaReplicaInfo, rowTable bool) string {
	if rowTable {
		return fmt.Sprintf(metaReplicaTableRowPattern, replica.Addr, replica.IsLeader, replica.ApplyId,
			formatMetaPartitionStatus(replica.Status), replica.StoreMode.Str(), formatTime(replica.ReportTime), replica.IsLearner, replica.IsRecover)
	}
	var sb = strings.Builder{}
	sb.WriteString(fmt.Sprintf("%v- Addr           : %v\n", indentation, replica.Addr))
	sb.WriteString(fmt.Sprintf("%v  Status         : %v\n", indentation, formatMetaPartitionStatus(replica.Status)))
	sb.WriteString(fmt.Sprintf("%v  IsLeader       : %v\n", indentation, replica.IsLeader))
	sb.WriteString(fmt.Sprintf("%v  IsLearner      : %v\n", indentation, replica.IsLearner))
	sb.WriteString(fmt.Sprintf("%v  Apply Id       : %v\n", indentation, replica.ApplyId))
	sb.WriteString(fmt.Sprintf("%v  Store Mode     : %v\n", indentation, replica.StoreMode.Str()))
	sb.WriteString(fmt.Sprintf("%v  ReportTime     : %v\n", indentation, formatTime(replica.ReportTime)))
	return sb.String()
}

var peerTableRowPattern = "%-6v    %-18v"

func formatPeerTableHeader() string {
	return fmt.Sprintf(peerTableRowPattern, "ID", "PEER")
}
func formatPeer(peer proto.Peer) string {
	return fmt.Sprintf(peerTableRowPattern, peer.ID, peer.Addr)
}

var learnerTableRowPattern = "%-6v    %-18v    %-12v    %-6v"

func formatLearnerTableHeader() string {
	return fmt.Sprintf(learnerTableRowPattern, "ID", "ADDRESS", "AUTOPROMOTE", "THRESHOLD")
}

func formatLearner(learner proto.Learner) string {
	return fmt.Sprintf(learnerTableRowPattern, learner.ID, learner.Addr, learner.PmConfig.AutoProm, learner.PmConfig.PromThreshold)
}

var dataNodeDetailTableRowPattern = "%-6v    %-6v    %-18v    %-6v    %-6v    %-6v    %-10v"

func formatDataNodeDetailTableHeader() string {
	return fmt.Sprintf(dataNodeDetailTableRowPattern, "ID", "ZONE", "ADDRESS", "USED", "TOTAL", "STATUS", "REPORT TIME")
}

func formatDataNodeDetail(dn *proto.DataNodeInfo, rowTable bool) string {
	if rowTable {
		return fmt.Sprintf(dataNodeDetailTableRowPattern, dn.ID, dn.ZoneName, dn.Addr, formatSize(dn.Used),
			formatSize(dn.Total), formatNodeStatus(dn.IsActive), formatTimeToString(dn.ReportTime))
	}
	var sb = strings.Builder{}
	sb.WriteString(fmt.Sprintf("  ID                  : %v\n", dn.ID))
	sb.WriteString(fmt.Sprintf("  Address             : %v\n", dn.Addr))
	sb.WriteString(fmt.Sprintf("  HttpPort            : %v\n", dn.HttpPort))
	sb.WriteString(fmt.Sprintf("  Version             : %v\n", dn.Version))
	sb.WriteString(fmt.Sprintf("  Carry               : %v\n", dn.Carry))
	sb.WriteString(fmt.Sprintf("  Used ratio          : %v\n", dn.UsageRatio))
	sb.WriteString(fmt.Sprintf("  Used                : %v\n", formatSize(dn.Used)))
	sb.WriteString(fmt.Sprintf("  Available           : %v\n", formatSize(dn.AvailableSpace)))
	sb.WriteString(fmt.Sprintf("  Total               : %v\n", formatSize(dn.Total)))
	sb.WriteString(fmt.Sprintf("  Zone                : %v\n", dn.ZoneName))
	sb.WriteString(fmt.Sprintf("  IsActive            : %v\n", formatNodeStatus(dn.IsActive)))
	sb.WriteString(fmt.Sprintf("  Report time         : %v\n", formatTimeToString(dn.ReportTime)))
	sb.WriteString(fmt.Sprintf("  Partition count     : %v\n", dn.DataPartitionCount))
	sb.WriteString(fmt.Sprintf("  Bad disks           : %v\n", dn.BadDisks))
	sb.WriteString(fmt.Sprintf("  Persist partitions  : %v\n", dn.PersistenceDataPartitions))
	return sb.String()
}

var metaNodeDetailTableRowPattern = "%-6v    %-6v    %-18v    %-6v    %-6v    %-6v    %-6v    %-10v"

func formatMetaNodeDetailTableHeader() string {
	return fmt.Sprintf(metaNodeDetailTableRowPattern, "ID", "ZONE", "ADDRESS", "VERSION", "USED", "TOTAL", "STATUS", "REPORT TIME")
}

func formatMetaNodeDetail(mn *proto.MetaNodeInfo, rowTable bool) string {
	if rowTable {
		return fmt.Sprintf(metaNodeDetailTableRowPattern, mn.ID, mn.ZoneName, mn.Addr, mn.Version, mn.Used, mn.Total, mn.IsActive, formatTimeToString(mn.ReportTime))
	}
	var sb = strings.Builder{}
	sb.WriteString(fmt.Sprintf("  ID                  : %v\n", mn.ID))
	sb.WriteString(fmt.Sprintf("  Address             : %v\n", mn.Addr))
	sb.WriteString(fmt.Sprintf("  Version             : %v\n", mn.Version))
	sb.WriteString(fmt.Sprintf("  Carry               : %v\n", mn.Carry))
	sb.WriteString(fmt.Sprintf("  Threshold           : %v\n", mn.Threshold))
	sb.WriteString(fmt.Sprintf("  MaxMemAvailWeight   : %v\n", formatSize(mn.MaxMemAvailWeight)))
	sb.WriteString(fmt.Sprintf("  Used                : %v\n", formatSize(mn.Used)))
	sb.WriteString(fmt.Sprintf("  Total               : %v\n", formatSize(mn.Total)))
	sb.WriteString(fmt.Sprintf("  Zone                : %v\n", mn.ZoneName))
	sb.WriteString(fmt.Sprintf("  IsActive            : %v\n", formatNodeStatus(mn.IsActive)))
	sb.WriteString(fmt.Sprintf("  Report time         : %v\n", formatTimeToString(mn.ReportTime)))
	sb.WriteString(fmt.Sprintf("  Partition count     : %v\n", mn.MetaPartitionCount))
	sb.WriteString(fmt.Sprintf("  Persist partitions  : %v\n", mn.PersistenceMetaPartitions))
	return sb.String()
}

var (
	zoneInfoTablePattern = "%-16v    %-16s    %-16v"
	zoneInfoTableHeader  = fmt.Sprintf(zoneInfoTablePattern, "Name", "Status", "Region")
)

func formatZoneInfoTableRow(zv *proto.ZoneView) string {
	return fmt.Sprintf(zoneInfoTablePattern, zv.Name, zv.Status, zv.Region)
}

func formatZoneView(zv *proto.ZoneView) string {
	var sb = strings.Builder{}
	sb.WriteString(fmt.Sprintf("  Name                 : %v\n", zv.Name))
	sb.WriteString(fmt.Sprintf("  Status                 : %s\n", zv.Status))
	sb.WriteString(fmt.Sprintf("  Region               : %v\n", zv.Region))
	sb.WriteString(fmt.Sprintf("  Medium Type     : %v\n", zv.MediumType))
	return sb.String()
}

var (
	regionInfoTablePattern = "%-16v    %-16s    %-16v"
	regionInfoTableHeader  = fmt.Sprintf(regionInfoTablePattern, "Name", "RegionType", "Zones")
)

func formatRegionInfoTableRow(rv *proto.RegionView) string {
	return fmt.Sprintf(regionInfoTablePattern, rv.Name, rv.RegionType, strings.Join(rv.Zones, ","))
}

func formatRegionView(rv *proto.RegionView) string {
	var sb = strings.Builder{}
	sb.WriteString(fmt.Sprintf("  Name                 : %v\n", rv.Name))
	sb.WriteString(fmt.Sprintf("  RegionType           : %s\n", rv.RegionType))
	sb.WriteString(fmt.Sprintf("  Zones                : %v\n", strings.Join(rv.Zones, ",")))
	return sb.String()
}

var (
	monitorQueryDataTableRowPattern = "%-10v    %-60v    %-20v    %-15v    %-10v    %-10v"
	monitorQueryDataTableHeader     = fmt.Sprintf(monitorQueryDataTableRowPattern, "PID", "VOL", "IP", "OP", "COUNT", "SIZE(unit: MB)")
)

func formatMonitorQueryData(data *proto.QueryData) string {
	return fmt.Sprintf(monitorQueryDataTableRowPattern, data.Pid, data.Vol, data.IP, data.Op, data.Count, data.Size/1024/1024)
}

func formatConvertProcessorInfo(processors []*proto.ConvertProcessorInfo) string {
	ProcessorInfoRowPattern := "    %-10v    %-10v    %-10v\n"
	ProcessorInfoTableHeader := fmt.Sprintf(ProcessorInfoRowPattern, "ID", "STATE", "COUNT")

	sb := strings.Builder{}
	sb.WriteString(ProcessorInfoTableHeader)
	for _, p := range processors {
		sb.WriteString(fmt.Sprintf(ProcessorInfoRowPattern, p.Id, p.State, p.Count))
	}

	return sb.String()
}

func formatConvertClusterInfo(clusters []*proto.ConvertClusterInfo) string {
	clusterRowPattern := "    %-10v    %-10v\n"
	clusterTableHeader := fmt.Sprintf(clusterRowPattern, "NAME", "NODES")

	sb := strings.Builder{}
	sb.WriteString(clusterTableHeader)
	for _, c := range clusters {
		nodes := strings.Builder{}
		for index, node := range c.Nodes {
			if index != 0 {
				nodes.WriteString(",")
			}
			nodes.WriteString(node)

		}
		sb.WriteString(fmt.Sprintf(clusterRowPattern, c.ClusterName, nodes.String()))
	}

	return sb.String()
}

func formatConvertTaskTable(tasks []*proto.ConvertTaskInfo) string {
	taskRowPattern := "    %-10v    %-10v    %-10v    %-10v    %-10v    %-10v    %-10v\n"
	taskTableHeader := fmt.Sprintf(taskRowPattern, "CLUSTER", "VOLNAME", "PROCESSORID", "CONMPNUM", "FINMPNUM", "RUNNINGMPID", "LAYOUT")

	sb := strings.Builder{}
	sb.WriteString(taskTableHeader)
	for _, task := range tasks {
		sb.WriteString(fmt.Sprintf(taskRowPattern, task.ClusterName, task.VolName, task.ProcessorID, len(task.SelectedMP),
			len(task.FinishedMP), task.RunningMP,
			fmt.Sprintf("%d_%d", task.Layout.PercentOfMP, task.Layout.PercentOfReplica)))
	}

	return sb.String()
}

func formatConvertNodesData(data *proto.ConvertNodeViewInfo) string {
	var sb = strings.Builder{}
	sb.WriteString(fmt.Sprintf("  Port                : %v\n", data.Port))
	sb.WriteString(fmt.Sprintf("  Cluster   Num       : %v\n", len(data.Clusters)))
	sb.WriteString(fmt.Sprintf("  Processor Num       : %v\n", len(data.Processors)))
	sb.WriteString(fmt.Sprintf("  Task      Num       : %v\n", len(data.Tasks)))
	sb.WriteString("\n  Clusters   Info     :\n")
	sb.WriteString(formatConvertClusterInfo(data.Clusters))
	sb.WriteString("\n  Processors   Info   :\n")
	sb.WriteString(formatConvertProcessorInfo(data.Processors))
	sb.WriteString("\n  Tasks   Info        :\n")
	sb.WriteString(formatConvertTaskTable(data.Tasks))
	return sb.String()

}

func formatConvertClusterDetailInfo(cluster *proto.ConvertClusterDetailInfo) string {
	var sb = strings.Builder{}

	sb.WriteString("[Cluster]\n")
	sb.WriteString(fmt.Sprintf("  Name                : %v\n", cluster.Cluster.ClusterName))
	for index, node := range cluster.Cluster.Nodes {
		sb.WriteString(fmt.Sprintf("  Nodes%-02d             : %v\n", index, node))
	}
	sb.WriteString(fmt.Sprintf("  Task Count          : %v\n", len(cluster.Tasks)))
	sb.WriteString("\n  Tasks   Info        :\n")
	sb.WriteString(formatConvertTaskTable(cluster.Tasks))

	return sb.String()
}

func formatConvertProcessorDetailInfo(processor *proto.ConvertProcessorDetailInfo) string {
	var sb = strings.Builder{}

	sb.WriteString("[Processor]\n")
	sb.WriteString(fmt.Sprintf("  Id                  : %v\n", processor.Processor.Id))
	sb.WriteString(fmt.Sprintf("  State               : %s\n", processor.Processor.StateStr()))
	sb.WriteString(fmt.Sprintf("  Count               : %v\n", processor.Processor.Count))

	sb.WriteString("\n  Tasks   Info        :\n")
	sb.WriteString(formatConvertTaskTable(processor.Tasks))

	return sb.String()
}

func formatConvertTaskSelMP(task *proto.ConvertTaskInfo) string {
	var sb = strings.Builder{}
	for index, mpId := range task.SelectedMP {
		if index != 0 && index%5 == 0 {
			sb.WriteString("\n                        ")
		}
		sb.WriteString(fmt.Sprintf("%-5d", mpId))
	}
	sb.WriteString("\n")
	return sb.String()
}

func formatConvertTaskFinMP(task *proto.ConvertTaskInfo) string {
	var sb = strings.Builder{}
	for index, mpId := range task.FinishedMP {
		if index != 0 && index%5 == 0 {
			sb.WriteString("\n                        ")
		}
		sb.WriteString(fmt.Sprintf("%-5d", mpId))
	}
	sb.WriteString("\n")
	return sb.String()
}

func formatConvertTaskIgnMP(task *proto.ConvertTaskInfo) string {
	var sb = strings.Builder{}
	for index, mpId := range task.IgnoredMP {
		if index != 0 && index%5 == 0 {
			sb.WriteString("\n                        ")
		}
		sb.WriteString(fmt.Sprintf("%-5d", mpId))
	}
	sb.WriteString("\n")
	return sb.String()
}

func formatConvertTaskDetailInfo(task *proto.ConvertTaskInfo) string {
	var sb = strings.Builder{}

	sb.WriteString("[ConvertTask]\n")
	sb.WriteString(fmt.Sprintf("  Cluster Name        : %v\n", task.ClusterName))
	sb.WriteString(fmt.Sprintf("  Volume  Name        : %v\n", task.VolName))
	sb.WriteString(fmt.Sprintf("  Processor Id        : %v\n", task.ProcessorID))
	sb.WriteString(fmt.Sprintf("  State               : %v\n", task.TaskState.Str()))
	sb.WriteString(fmt.Sprintf("  Layout              : %v\n", fmt.Sprintf("%d_%d", task.Layout.PercentOfMP, task.Layout.PercentOfReplica)))
	sb.WriteString(fmt.Sprintf("  Running MP Id       : %v\n", task.RunningMP))

	sb.WriteString("  Selected MPs Id     : ")
	sb.WriteString(formatConvertTaskSelMP(task))

	sb.WriteString("  Finished MPs Id     : ")
	sb.WriteString(formatConvertTaskFinMP(task))

	sb.WriteString("  Ignored  MPs Id     : ")
	sb.WriteString(formatConvertTaskIgnMP(task))

	sb.WriteString(fmt.Sprintf("  Error  Msg          : %v\n", task.ErrMsg))

	return sb.String()
}

func formatConvertDetailInfo(data interface{}, objType string) string {
	var sb = strings.Builder{}
	switch objType {
	case "cluster":
		sb.WriteString(formatConvertClusterDetailInfo(data.(*proto.ConvertClusterDetailInfo)))
	case "task":
		sb.WriteString(formatConvertTaskDetailInfo(data.(*proto.ConvertTaskInfo)))
	case "processor":
		sb.WriteString(formatConvertProcessorDetailInfo(data.(*proto.ConvertProcessorDetailInfo)))
	default:
		sb.WriteString(fmt.Sprintf("unknown objtype[%s]\n", objType))
	}

	return sb.String()
}

func contains(arr []string, element string) (ok bool) {
	if arr == nil || len(arr) == 0 {
		return
	}

	for _, e := range arr {
		if e == element {
			ok = true
			break
		}
	}
	return
}
func convertPeersToArray(peers []proto.Peer) (addrs []string) {
	addrs = make([]string, 0)
	for _, peer := range peers {
		addrs = append(addrs, peer.Addr)
	}
	return
}
func convertLearnersToArray(learners []proto.Learner) (addrs []string) {
	addrs = make([]string, 0)
	for _, peer := range learners {
		addrs = append(addrs, peer.Addr)
	}
	return
}

//if tow strings are equal
func isEqualStrings(strs1, strs2 []string) bool {
	sort.Strings(strs1)
	sort.Strings(strs2)
	if len(strs1) != len(strs2) {
		return false
	}
	for i, s := range strs1 {
		if strs2[i] != s {
			return false
		}
	}
	return true
}

var (
	idcQueryTablePattern    = "%-16s	%-16s"
	idcQueryDataTableHeader = fmt.Sprintf(idcQueryTablePattern, "Name", "Zones")
)

func formatIdcInfoTableRow(name string, zones string) string {
	return fmt.Sprintf(idcQueryTablePattern, name, zones)
}

var raftInfoTableHeader = "%-6v     %-8v    %-10v     %-10v   %-10v      %-10v    %-10v    %-8v    %-16v    %-10v"
var dataPartitionRaftTableHeaderInfo = fmt.Sprintf(raftInfoTableHeader, "ID", "ISLEADER", "COMMIT", "INDEX", "APPLIED", "LOGFIRST", "LOGLAST", "PENDQUE", "STATE", "STOPED")

func formatDataPartitionRaftTableInfo(raft *proto.Status) string {
	var sb = strings.Builder{}
	sb.WriteString(fmt.Sprintf(raftInfoTableHeader, raft.NodeID, raft.Leader == raft.NodeID, raft.Commit, raft.Index, raft.Applied, raft.Log.FirstIndex, raft.Log.LastIndex, raft.PendQueue, raft.State, raft.Stopped))
	return sb.String()
}

var ecReplicaTableRowPattern = "%-18v    %-6v    %-6v    %-6v    %-6v    %-6v    %-10v    %-8v"

func formatEcReplicaTableHeader() string {
	return fmt.Sprintf(ecReplicaTableRowPattern, "ADDRESS", "USED", "TOTAL", "ISLEADER", "FILECOUNT", "STATUS", "REPORT TIME", "HTTPPORT")
}

func formatEcReplica(indentation string, replica *proto.EcReplica, rowTable bool) string {
	if rowTable {
		return fmt.Sprintf(ecReplicaTableRowPattern, replica.Addr, formatSize(replica.Used), formatSize(replica.Total),
			replica.IsLeader, replica.FileCount, formatDataPartitionStatus(replica.Status), formatTime(replica.ReportTime), replica.HttpPort)
	}
	var sb = strings.Builder{}
	sb.WriteString(fmt.Sprintf("%v- Addr           : %v\n", indentation, replica.Addr))
	sb.WriteString(fmt.Sprintf("%v  Used           : %v\n", indentation, formatSize(replica.Used)))
	sb.WriteString(fmt.Sprintf("%v  Total          : %v\n", indentation, formatSize(replica.Total)))
	sb.WriteString(fmt.Sprintf("%v  IsLeader       : %v\n", indentation, replica.IsLeader))
	sb.WriteString(fmt.Sprintf("%v  FileCount      : %v\n", indentation, replica.FileCount))
	sb.WriteString(fmt.Sprintf("%v  HasLoadResponse: %v\n", indentation, replica.HasLoadResponse))
	sb.WriteString(fmt.Sprintf("%v  NeedsToCompare : %v\n", indentation, replica.NeedsToCompare))
	sb.WriteString(fmt.Sprintf("%v  Status         : %v\n", indentation, formatDataPartitionStatus(replica.Status)))
	sb.WriteString(fmt.Sprintf("%v  DiskPath       : %v\n", indentation, replica.DiskPath))
	sb.WriteString(fmt.Sprintf("%v  ReportTime     : %v\n", indentation, formatTime(replica.ReportTime)))
	sb.WriteString(fmt.Sprintf("%v  HttpPort       : %v\n", indentation, replica.HttpPort))
	return sb.String()
}

var ecNodeDetailTableRowPattern = "%-6v    %-6v    %-6v    %-10v"

func formatSimpleVolEcView(svv *proto.SimpleVolView) string {

	var sb = strings.Builder{}
	sb.WriteString(fmt.Sprintf("  Ec is enabled            : %v\n", svv.EcEnable))
	sb.WriteString(fmt.Sprintf("  Ec data numbers          : %v\n", svv.EcDataNum))
	sb.WriteString(fmt.Sprintf("  Ec parity numbers        : %v\n", svv.EcParityNum))
	sb.WriteString(fmt.Sprintf("  Ec migration waitTime    : %v min\n", svv.EcWaitTime))
	sb.WriteString(fmt.Sprintf("  rep partition saveTime   : %v min\n", svv.EcSaveTime))
	sb.WriteString(fmt.Sprintf("  Ec migrate timeOut       : %v min\n", svv.EcTimeOut))
	sb.WriteString(fmt.Sprintf("  Ec fail retry waitTime   : %v min\n", svv.EcRetryWait))
	sb.WriteString(fmt.Sprintf("  Ec max unit size         : %v bytes\n", svv.EcMaxUnitSize))
	return sb.String()
}

func formatEcPartitionInfo(partition *proto.EcPartitionInfo) string {
	var sb = strings.Builder{}
	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("volume name     : %v\n", partition.VolName))
	sb.WriteString(fmt.Sprintf("volume ID       : %v\n", partition.VolID))
	sb.WriteString(fmt.Sprintf("PartitionID     : %v\n", partition.PartitionID))
	sb.WriteString(fmt.Sprintf("IsRecover       : %v\n", partition.IsRecover))
	sb.WriteString(fmt.Sprintf("Status          : %v\n", formatDataPartitionStatus(partition.Status)))
	sb.WriteString(fmt.Sprintf("EcMigrateStatus : %v\n", EcStatusMap[partition.EcMigrateStatus]))
	sb.WriteString(fmt.Sprintf("LastLoadedTime  : %v\n", formatTime(partition.LastLoadedTime)))
	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("Replicas : \n"))
	sb.WriteString(fmt.Sprintf("%v\n", formatEcReplicaTableHeader()))
	for _, replica := range partition.EcReplicas {
		sb.WriteString(fmt.Sprintf("%v\n", formatEcReplica("", replica, true)))
	}

	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("Hosts :\n"))
	for _, host := range partition.Hosts {
		sb.WriteString(fmt.Sprintf("  [%v]", host))
	}
	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("Zones :\n"))
	for _, zone := range partition.Zones {
		sb.WriteString(fmt.Sprintf("  [%v]", zone))
	}
	sb.WriteString("\n")
	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("MissingNodes :\n"))
	for partitionHost, id := range partition.MissingNodes {
		sb.WriteString(fmt.Sprintf("  [%v, %v]\n", partitionHost, id))
	}
	sb.WriteString(fmt.Sprintf("FilesWithMissingReplica : \n"))
	for file, id := range partition.FilesWithMissingReplica {
		sb.WriteString(fmt.Sprintf("  [%v, %v]\n", file, id))
	}
	return sb.String()
}

func formatEcPartitionInfoRow(partition *proto.EcPartitionInfo) string {
	var sb = strings.Builder{}
	sort.Strings(partition.Hosts)
	sb.WriteString(fmt.Sprintf(partitionInfoTablePattern+"\n",
		partition.PartitionID, partition.VolName, formatDataPartitionStatus(partition.Status), "master", fmt.Sprintf("%v/%v", len(partition.Hosts), partition.ReplicaNum), "(hosts)"+strings.Join(partition.Hosts, ",")))
	return sb.String()
}

func formatEcNodeDetail(en *proto.EcNodeInfo, rowTable bool) string {
	if rowTable {
		return fmt.Sprintf(dataNodeDetailTableRowPattern, en.ID, en.ZoneName, en.Addr, formatSize(en.Used),
			formatSize(en.Total), formatNodeStatus(en.IsActive), formatTimeToString(en.ReportTime))
	}
	var sb = strings.Builder{}
	sb.WriteString(fmt.Sprintf("  ID                  : %v\n", en.ID))
	sb.WriteString(fmt.Sprintf("  Address             : %v\n", en.Addr))
	sb.WriteString(fmt.Sprintf("  HttpPort            : %v\n", en.HttpPort))
	sb.WriteString(fmt.Sprintf("  Carry               : %v\n", en.Carry))
	sb.WriteString(fmt.Sprintf("  Used ratio          : %v\n", en.UsageRatio))
	sb.WriteString(fmt.Sprintf("  Used                : %v\n", formatSize(en.Used)))
	sb.WriteString(fmt.Sprintf("  Available           : %v\n", formatSize(en.AvailableSpace)))
	sb.WriteString(fmt.Sprintf("  MaxDiskAvail        : %v\n", formatSize(en.MaxDiskAvailSpace)))
	sb.WriteString(fmt.Sprintf("  Total               : %v\n", formatSize(en.Total)))
	sb.WriteString(fmt.Sprintf("  Zone                : %v\n", en.ZoneName))
	sb.WriteString(fmt.Sprintf("  IsActive            : %v\n", formatNodeStatus(en.IsActive)))
	sb.WriteString(fmt.Sprintf("  Report time         : %v\n", formatTimeToString(en.ReportTime)))
	sb.WriteString(fmt.Sprintf("  Partition count     : %v\n", en.DataPartitionCount))
	sb.WriteString(fmt.Sprintf("  Bad disks           : %v\n", en.BadDisks))
	sb.WriteString(fmt.Sprintf("  Persist partitions  : %v\n", en.PersistenceDataPartitions))
	sb.WriteString(fmt.Sprintf("  tobeOffline         : %v\n", en.ToBeOffline))
	sb.WriteString(fmt.Sprintf("  tobeMigrated        : %v\n", en.ToBeMigrated))
	return sb.String()
}

var compactViewTableRowPattern = "%-63v    %-20v    %-8v    %-8v"

func formatCompactVolViewTableHeader() string {
	return fmt.Sprintf(compactViewTableRowPattern, "VOLUME", "OWNER", "FORCEROW", "COMPACT")
}

func formatCompactVolView(view *proto.CompactVolume) string {
	return fmt.Sprintf(compactViewTableRowPattern, view.Name, view.Owner, view.ForceROW, view.CompactTag.Bool())
}

var checkVolViewTableRowPattern = "%-8v    %-63v    %-17v    %-8v"

func formatCompactCheckVolViewTableHeader() string {
	return fmt.Sprintf(checkVolViewTableRowPattern, "INDEX", "VOLUME", "CROSSREGIONHATYTE", "FORCEROW")
}

func formatCompactCheckVolView(index int, view *proto.SimpleVolView) string {
	return fmt.Sprintf(checkVolViewTableRowPattern, index, view.Name, view.CrossRegionHAType, view.ForceROW)
}

var checkFragViewTableRowPattern = "%-63v    %-8v    %-8v    %-8v    %-8v    %-8v"

func formatCompactCheckFragViewTableHeader() string {
	return fmt.Sprintf(checkFragViewTableRowPattern, "VOLUME", "MPID", "INODEID", "EKLENGTH", "EKAVGSIZE", "INODESIZE")
}

func formatCompactCheckFragView(volName string, mpId, inodeId uint64, extLength int, ekAvgSize uint64, inodeSize uint64) string {
	return fmt.Sprintf(checkFragViewTableRowPattern, volName, mpId, inodeId, extLength, ekAvgSize, inodeSize)
}

var tinyExtentTableRowPattern = "%-20v    %-20v    %-20v    %-20v    %-32v    %-8v    %-10v    %-8v   %-20v"

func formatTinyExtentTableHeader() string {
	return fmt.Sprintf(tinyExtentTableRowPattern, "Address", "Disk", "IsLeader", "Size", "MD5", "Block", "AvailSize", "HoleNum", "ModifyTime")
}

func formatTinyExtent(r *proto.DataReplica, extent *proto.ExtentInfoBlock, tinyHole *proto.DNTinyExtentInfo, md5Sum string) string {
	if extent == nil && tinyHole == nil {
		return fmt.Sprintf(tinyExtentTableRowPattern, r.Addr, r.DiskPath, r.IsLeader, "NULL", "NULL", "NULL", "NULL", "NULL",
			"NULL")
	}
	if extent == nil {
		return fmt.Sprintf(tinyExtentTableRowPattern, r.Addr, r.DiskPath, r.IsLeader, "NULL", md5Sum, tinyHole.BlocksNum, tinyHole.ExtentAvaliSize, len(tinyHole.Holes),
			"NULL")
	}
	if tinyHole == nil {
		return fmt.Sprintf(tinyExtentTableRowPattern, r.Addr, r.DiskPath, r.IsLeader, extent[proto.ExtentInfoSize], md5Sum, "NULL", "NULL", "NULL", formatTime(int64(extent[proto.ExtentInfoModifyTime])))
	}
	return fmt.Sprintf(tinyExtentTableRowPattern, r.Addr, r.DiskPath, r.IsLeader, extent[proto.ExtentInfoSize], md5Sum, tinyHole.BlocksNum, tinyHole.ExtentAvaliSize, len(tinyHole.Holes), formatTime(int64(extent[proto.ExtentInfoModifyTime])))
}

var normalExtentTableRowPattern = "%-20v    %-20v    %-20v    %-20v    %-20v     %-32v     %-20v"

func formatNormalExtentTableHeader() string {
	return fmt.Sprintf(normalExtentTableRowPattern, "Address", "Disk", "IsLeader", "Size", "Crc", "MD5", "ModifyTime")
}

func formatNormalExtent(r *proto.DataReplica, extent *proto.ExtentInfoBlock, md5Sum string) string {
	if extent == nil {
		return fmt.Sprintf(normalExtentTableRowPattern, r.Addr, r.DiskPath, r.IsLeader, "NULL", "NULL", "NULL", "NULL")
	}
	return fmt.Sprintf(normalExtentTableRowPattern, r.Addr, r.DiskPath, r.IsLeader, extent[proto.ExtentInfoSize], extent[proto.ExtentInfoCrc], md5Sum, formatTime(int64(extent[proto.ExtentInfoModifyTime])))
}

func formatClusterNodeInfo(info *proto.LimitInfo) string {
	var sb = strings.Builder{}
	sb.WriteString("NodeParam :\n")
	sb.WriteString(fmt.Sprintf("  DnNormalExtentDeleteExpire  : %v\n", info.DataNodeNormalExtentDeleteExpire))
	sb.WriteString(fmt.Sprintf("  RocksDBDiskReservedSpace    : %v\n", info.RocksDBDiskReservedSpace))
	sb.WriteString(fmt.Sprintf("  LogMaxSize                  : %v\n", info.LogMaxSize))
	sb.WriteString(fmt.Sprintf("  DataNodeFlushFDInterval     : %v\n", info.DataNodeFlushFDInterval))
	sb.WriteString(fmt.Sprintf("  MetaNodeDumpWaterLevel      : %v\n", info.MetaNodeDumpWaterLevel))
	sb.WriteString(fmt.Sprintf("  ExtentMergeSleepMs          : %v\n", info.ExtentMergeSleepMs))
	sb.WriteString(fmt.Sprintf("  ExtentMergeIno              : %v\n", info.ExtentMergeIno))
	sb.WriteString(fmt.Sprintf("  RocksdbDiskUsageThreshold   : %v\n", info.RocksdbDiskUsageThreshold))
	return sb.String()
}

var (
	trashVolumeInfoTablePattern = "%-63v    %-63v   %-45v    %-20v    %-8v    %-8v    %-45v"
	trashVolumeInfoTableHeader  = fmt.Sprintf(trashVolumeInfoTablePattern, "VOLUME", "OLD VOLUME", "MRAK DEL TIME", "OWNER", "STATUS", "USED", "CREATE TIME")
)

func formatTrashVolInfoTableRow(svv *proto.SimpleVolView, vi *proto.VolInfo) string {
	return fmt.Sprintf(trashVolumeInfoTablePattern,
		svv.Name, svv.OldVolName, time.Unix(svv.MarkDeleteTime, 0).Local().Format(time.RFC1123), svv.Owner,
		formatVolumeStatus(svv.Status), formatSize(svv.UsedSize),
		time.Unix(vi.CreateTime, 0).Local().Format(time.RFC1123))
}
