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
	"strconv"
	"strings"
	"time"

	"github.com/cubefs/cubefs/proto"
)

func formatClusterView(cv *proto.ClusterView, cn *proto.ClusterNodeInfo, cp *proto.ClusterIP) string {
	var sb = strings.Builder{}
	sb.WriteString(fmt.Sprintf("  Cluster name       : %v\n", cv.Name))
	sb.WriteString(fmt.Sprintf("  Master leader      : %v\n", cv.LeaderAddr))
	sb.WriteString(fmt.Sprintf("  Auto allocate      : %v\n", formatEnabledDisabled(!cv.DisableAutoAlloc)))
	sb.WriteString(fmt.Sprintf("  MetaNode count     : %v\n", len(cv.MetaNodes)))
	sb.WriteString(fmt.Sprintf("  MetaNode used      : %v GB\n", cv.MetaNodeStatInfo.UsedGB))
	sb.WriteString(fmt.Sprintf("  MetaNode total     : %v GB\n", cv.MetaNodeStatInfo.TotalGB))
	sb.WriteString(fmt.Sprintf("  DataNode count     : %v\n", len(cv.DataNodes)))
	sb.WriteString(fmt.Sprintf("  DataNode used      : %v GB\n", cv.DataNodeStatInfo.UsedGB))
	sb.WriteString(fmt.Sprintf("  DataNode total     : %v GB\n", cv.DataNodeStatInfo.TotalGB))
	sb.WriteString(fmt.Sprintf("  Volume count       : %v\n", len(cv.VolStatInfo)))
	sb.WriteString(fmt.Sprintf("  EbsAddr            : %v\n", cp.EbsAddr))
	sb.WriteString(fmt.Sprintf("  LoadFactor         : %v\n", cn.LoadFactor))
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

var nodeViewTableRowPattern = "%-6v    %-18v    %-8v    %-8v"

func formatNodeViewTableHeader() string {
	return fmt.Sprintf(nodeViewTableRowPattern, "ID", "ADDRESS", "WRITABLE", "STATUS")
}

func formatNodeView(view *proto.NodeView, tableRow bool) string {
	if tableRow {
		return fmt.Sprintf(nodeViewTableRowPattern, view.ID, view.Addr,
			formatYesNo(view.IsWritable), formatNodeStatus(view.Status))
	}
	var sb = strings.Builder{}
	sb.WriteString(fmt.Sprintf("  ID      : %v\n", view.ID))
	sb.WriteString(fmt.Sprintf("  Address : %v\n", view.Addr))
	sb.WriteString(fmt.Sprintf("  Writable: %v\n", formatYesNo(view.IsWritable)))
	sb.WriteString(fmt.Sprintf("  Status  : %v", formatNodeStatus(view.Status)))
	return sb.String()
}

func formatSimpleVolView(svv *proto.SimpleVolView) string {

	var sb = strings.Builder{}
	sb.WriteString(fmt.Sprintf("  ID                   : %v\n", svv.ID))
	sb.WriteString(fmt.Sprintf("  Name                 : %v\n", svv.Name))
	sb.WriteString(fmt.Sprintf("  Owner                : %v\n", svv.Owner))
	sb.WriteString(fmt.Sprintf("  Authenticate         : %v\n", formatEnabledDisabled(svv.Authenticate)))
	sb.WriteString(fmt.Sprintf("  Capacity             : %v GB\n", svv.Capacity))
	sb.WriteString(fmt.Sprintf("  Create time          : %v\n", svv.CreateTime))
	sb.WriteString(fmt.Sprintf("  Cross zone           : %v\n", formatEnabledDisabled(svv.CrossZone)))
	sb.WriteString(fmt.Sprintf("  DefaultPriority      : %v\n", svv.DefaultPriority))
	sb.WriteString(fmt.Sprintf("  Dentry count         : %v\n", svv.DentryCount))
	sb.WriteString(fmt.Sprintf("  Description          : %v\n", string([]rune(svv.Description)[:])))
	sb.WriteString(fmt.Sprintf("  DpCnt                : %v\n", svv.DpCnt))
	sb.WriteString(fmt.Sprintf("  DpReplicaNum         : %v\n", svv.DpReplicaNum))
	sb.WriteString(fmt.Sprintf("  Follower read        : %v\n", formatEnabledDisabled(svv.FollowerRead)))
	sb.WriteString(fmt.Sprintf("  Inode count          : %v\n", svv.InodeCount))
	sb.WriteString(fmt.Sprintf("  Max metaPartition ID : %v\n", svv.MaxMetaPartitionID))
	sb.WriteString(fmt.Sprintf("  MpCnt                : %v\n", svv.MpCnt))
	sb.WriteString(fmt.Sprintf("  MpReplicaNum         : %v\n", svv.MpReplicaNum))
	sb.WriteString(fmt.Sprintf("  NeedToLowerReplica   : %v\n", formatEnabledDisabled(svv.NeedToLowerReplica)))
	sb.WriteString(fmt.Sprintf("  RwDpCnt              : %v\n", svv.RwDpCnt))
	sb.WriteString(fmt.Sprintf("  Status               : %v\n", formatVolumeStatus(svv.Status)))
	sb.WriteString(fmt.Sprintf("  ZoneName             : %v\n", svv.ZoneName))
	sb.WriteString(fmt.Sprintf("  VolType              : %v\n", svv.VolType))
	sb.WriteString(fmt.Sprintf("  DpReadOnlyWhenVolFull: %v\n", svv.DpReadOnlyWhenVolFull))
	sb.WriteString(fmt.Sprintf("  Transaction Mask     : %v\n", svv.EnableTransaction))
	sb.WriteString(fmt.Sprintf("  Transaction timeout  : %v\n", svv.TxTimeout))
	if svv.VolType == 1 {
		sb.WriteString(fmt.Sprintf("  ObjBlockSize         : %v byte\n", svv.ObjBlockSize))
		sb.WriteString(fmt.Sprintf("  CacheCapacity        : %v G\n", svv.CacheCapacity))
		sb.WriteString(fmt.Sprintf("  CacheAction          : %v\n", svv.CacheAction))
		sb.WriteString(fmt.Sprintf("  CacheThreshold       : %v byte\n", svv.CacheThreshold))
		sb.WriteString(fmt.Sprintf("  CacheLruInterval     : %v min\n", svv.CacheLruInterval))
		sb.WriteString(fmt.Sprintf("  CacheTtl             : %v day\n", svv.CacheTtl))
		sb.WriteString(fmt.Sprintf("  CacheLowWater        : %v\n", svv.CacheLowWater))
		sb.WriteString(fmt.Sprintf("  CacheHighWater       : %v\n", svv.CacheHighWater))
		sb.WriteString(fmt.Sprintf("  CacheRule            : %v\n", svv.CacheRule))
	}
	return sb.String()
}

func formatVolumeStatus(status uint8) string {
	switch status {
	case 0:
		return "Normal"
	case 1:
		return "Marked delete"
	default:
		return "Unknown"
	}
}

var (
	volumeInfoTablePattern = "%-63v    %-20v    %-8v    %-8v    %-8v    %-10v"
	volumeInfoTableHeader  = fmt.Sprintf(volumeInfoTablePattern, "VOLUME", "OWNER", "USED", "TOTAL", "STATUS", "CREATE TIME")
)

func formatVolInfoTableRow(vi *proto.VolInfo) string {
	return fmt.Sprintf(volumeInfoTablePattern,
		vi.Name, vi.Owner, formatSize(vi.UsedSize), formatSize(vi.TotalSize),
		formatVolumeStatus(vi.Status), time.Unix(vi.CreateTime, 0).Local().Format(time.RFC1123))
}

var (
	volumeVersionPattern     = "%-20v    %-40v    %-8v    %-8v"
	volumeVersionTableHeader = fmt.Sprintf(volumeVersionPattern, "VER", "CTIME", "STATUS", "OTHER")
)

var (
	volumeAclPattern     = "%-20v    %-40v    %-8v"
	volumeAclTableHeader = fmt.Sprintf(volumeAclPattern, "IP", "CTIME", "OTHER")
)

func formatAclInfoTableRow(aclInfo *proto.AclIpInfo) string {
	return fmt.Sprintf(volumeAclPattern,
		aclInfo.Ip, time.Unix(aclInfo.CTime, 0).Format(time.RFC1123), "")
}

var (
	volumeUidPattern     = "%-20v    %-40v     %-8v   %-8v   %-8v    %-8v"
	volumeUidTableHeader = fmt.Sprintf(volumeUidPattern, "UID", "CTIME", "ENABLED", "LIMITED", "LIMITSIZE", "USED")
)

func formatUidInfoTableRow(uidInfo *proto.UidSpaceInfo) string {
	return fmt.Sprintf(volumeUidPattern,
		uidInfo.Uid, time.Unix(uidInfo.CTime, 0).Format(time.RFC1123), uidInfo.Enabled, uidInfo.Limited, uidInfo.LimitSize, uidInfo.UsedSize)
}

var (
	dataPartitionTablePattern = "%-8v    %-8v    %-10v    %-10v     %-18v    %-18v"
	dataPartitionTableHeader  = fmt.Sprintf(dataPartitionTablePattern,
		"ID", "REPLICAS", "STATUS", "ISRECOVER", "LEADER", "MEMBERS")
)

func formatDataPartitionTableRow(view *proto.DataPartitionResponse) string {
	return fmt.Sprintf(dataPartitionTablePattern,
		view.PartitionID, view.ReplicaNum, formatDataPartitionStatus(view.Status), view.IsRecover, view.LeaderAddr,
		strings.Join(view.Hosts, ","))
}

var (
	partitionInfoTablePattern = "%-8v    %-8v    %-10v     %-12v    %-18v"
	partitionInfoTableHeader  = fmt.Sprintf(partitionInfoTablePattern,
		"ID", "VOLUME", "REPLICAS", "STATUS", "MEMBERS")

	badReplicaPartitionInfoTablePattern = "%-8v    %-8v    %-8v    %-8v    %-24v    %-24v"
	badReplicaPartitionInfoTableHeader  = fmt.Sprintf(badReplicaPartitionInfoTablePattern,
		"DP_ID", "VOLUME", "REPLICAS", "DP_STATUS", "MEMBERS", "UNAVAILABLE_REPLICAS")

	repFileCountDifferPartitionInfoTablePattern = "%-8v    %-8v    %-8v    %-8v    %-24v"
	RepFileCountDifferInfoTableHeader           = fmt.Sprintf(repFileCountDifferPartitionInfoTablePattern,
		"DP_ID", "VOLUME", "REPLICAS", "DP_STATUS", "MEMBERS(fileCount)")

	repUsedSizeDifferPartitionInfoTablePattern = "%-8v    %-8v    %-8v    %-8v    %-24v"
	RepUsedSizeDifferInfoTableHeader           = fmt.Sprintf(repUsedSizeDifferPartitionInfoTablePattern,
		"DP_ID", "VOLUME", "REPLICAS", "DP_STATUS", "MEMBERS(usedSize)")
)

func formatDataPartitionInfoRow(partition *proto.DataPartitionInfo) string {
	return fmt.Sprintf(partitionInfoTablePattern, partition.PartitionID, partition.VolName, partition.ReplicaNum,
		formatDataPartitionStatus(partition.Status), strings.Join(partition.Hosts, ", "))
}

func formatBadReplicaDpInfoRow(partition *proto.DataPartitionInfo) string {
	var sb = strings.Builder{}
	sb.WriteString("[")
	var firstItem = true
	for _, replica := range partition.Replicas {
		if replica.Status == proto.Unavailable {
			if !firstItem {
				sb.WriteString(",")
			}

			sb.WriteString(fmt.Sprintf("%v", replica.Addr))
			firstItem = false
		}
	}
	sb.WriteString("]")
	return fmt.Sprintf(badReplicaPartitionInfoTablePattern, partition.PartitionID, partition.VolName, partition.ReplicaNum,
		formatDataPartitionStatus(partition.Status), "["+strings.Join(partition.Hosts, ", ")+"]", sb.String())
}

func formatReplicaFileCountDiffDpInfoRow(partition *proto.DataPartitionInfo) string {
	var sb = strings.Builder{}
	sb.WriteString("[")
	var firstItem = true
	for _, replica := range partition.Replicas {
		if !firstItem {
			sb.WriteString(",")
		}

		if replica.IsLeader {
			sb.WriteString(fmt.Sprintf("%v(%v isLeader)", replica.Addr, replica.FileCount))
		} else {
			sb.WriteString(fmt.Sprintf("%v(%v)", replica.Addr, replica.FileCount))
		}
		firstItem = false
	}
	sb.WriteString("]")
	return fmt.Sprintf(repFileCountDifferPartitionInfoTablePattern, partition.PartitionID, partition.VolName, partition.ReplicaNum,
		formatDataPartitionStatus(partition.Status), sb.String())
}

func formatReplicaSizeDiffDpInfoRow(partition *proto.DataPartitionInfo) string {
	var sb = strings.Builder{}
	sb.WriteString("[")
	var firstItem = true
	for _, replica := range partition.Replicas {
		if !firstItem {
			sb.WriteString(",")
		}

		if replica.IsLeader {
			sb.WriteString(fmt.Sprintf("%v(%v isLeader)", replica.Addr, replica.Used))
		} else {
			sb.WriteString(fmt.Sprintf("%v(%v)", replica.Addr, replica.Used))
		}
		firstItem = false
	}
	sb.WriteString("]")
	return fmt.Sprintf(repUsedSizeDifferPartitionInfoTablePattern, partition.PartitionID, partition.VolName, partition.ReplicaNum,
		formatDataPartitionStatus(partition.Status), sb.String())
}

func formatMetaPartitionInfoRow(partition *proto.MetaPartitionInfo) string {
	return fmt.Sprintf(partitionInfoTablePattern,
		partition.PartitionID, partition.VolName, partition.ReplicaNum, formatDataPartitionStatus(partition.Status), strings.Join(partition.Hosts, ", "))
}

func formatBadReplicaMpInfoRow(partition *proto.MetaPartitionInfo) string {
	var sb = strings.Builder{}
	sb.WriteString("[")
	var firstItem = true
	for _, replica := range partition.Replicas {
		if replica.Status == proto.Unavailable {
			if !firstItem {
				sb.WriteString(", ")
			}

			sb.WriteString(fmt.Sprintf("%v", replica.Addr))
			firstItem = false
		}
	}
	sb.WriteString("]")
	return fmt.Sprintf(badReplicaPartitionInfoTablePattern, partition.PartitionID, partition.VolName, partition.ReplicaNum,
		formatDataPartitionStatus(partition.Status), "["+strings.Join(partition.Hosts, ", ")+"]", sb.String())
}

func formatDataPartitionInfo(partition *proto.DataPartitionInfo) string {
	var sb = strings.Builder{}
	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("volume name   : %v\n", partition.VolName))
	sb.WriteString(fmt.Sprintf("volume ID     : %v\n", partition.VolID))
	sb.WriteString(fmt.Sprintf("PartitionID   : %v\n", partition.PartitionID))
	sb.WriteString(fmt.Sprintf("Status        : %v\n", formatDataPartitionStatus(partition.Status)))
	sb.WriteString(fmt.Sprintf("LastLoadedTime: %v\n", formatTime(partition.LastLoadedTime)))
	sb.WriteString(fmt.Sprintf("OfflinePeerID : %v\n", partition.OfflinePeerID))
	sb.WriteString(fmt.Sprintf("ISRdOnly      : %v\n", partition.RdOnly))
	sb.WriteString(fmt.Sprintf("IsDiscard     : %v\n", partition.IsDiscard))
	sb.WriteString(fmt.Sprintf("ReplicaNum    : %v\n", partition.ReplicaNum))
	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("Replicas : \n"))
	sb.WriteString(fmt.Sprintf("%v\n", formatDataReplicaTableHeader()))
	for _, replica := range partition.Replicas {
		sb.WriteString(fmt.Sprintf("%v\n", formatDataReplica("", replica, true)))
	}

	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("FileInCoreMap : \n"))
	sb.WriteString(fmt.Sprintf("%v\n", formatDataFileInCoreTableHeader()))
	sb.WriteString(fmt.Sprintf("%v\n", formatDataFileMetadateTableHeader()))
	for id, fileCore := range partition.FileInCoreMap {
		sb.WriteString(fmt.Sprintf(formatDataFileInCoreMap(id, "", fileCore, true)))
	}

	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("Peers :\n"))
	sb.WriteString(fmt.Sprintf("%v\n", formatPeerTableHeader()))
	for _, peer := range partition.Peers {
		sb.WriteString(fmt.Sprintf("%v\n", formatPeer(peer)))
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
	sb.WriteString(fmt.Sprintf("Status        : %v\n", formatMetaPartitionStatus(partition.Status)))
	sb.WriteString(fmt.Sprintf("Recovering    : %v\n", formatIsRecover(partition.IsRecover)))
	sb.WriteString(fmt.Sprintf("Start         : %v\n", partition.Start))
	sb.WriteString(fmt.Sprintf("End           : %v\n", partition.End))
	sb.WriteString(fmt.Sprintf("MaxInodeID    : %v\n", partition.MaxInodeID))
	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("Replicas : \n"))
	sb.WriteString(fmt.Sprintf("%v\n", formatMetaReplicaTableHeader()))
	for _, replica := range partition.Replicas {
		sb.WriteString(fmt.Sprintf("%v\n", formatMetaReplica("", replica, true)))
	}
	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("Peers :\n"))
	for _, peer := range partition.Peers {
		sb.WriteString(fmt.Sprintf("%v\n", formatPeer(peer)))
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
	metaPartitionTablePattern = "%-8v    %-12v    %-10v    %-12v    %-12v    %-12v    %-8v    %-12v    %-18v"
	metaPartitionTableHeader  = fmt.Sprintf(metaPartitionTablePattern,
		"ID", "MAX INODE", "DENTRY COUNT", "INODE COUNT", "START", "END", "STATUS", "LEADER", "MEMBERS")
)

func formatMetaPartitionTableRow(view *proto.MetaPartitionView) string {
	var rangeToString = func(num uint64) string {
		if num >= math.MaxInt64 {
			return "unlimited"
		}
		return strconv.FormatUint(num, 10)
	}
	return fmt.Sprintf(metaPartitionTablePattern,
		view.PartitionID, view.MaxInodeID, view.DentryCount, view.InodeCount, view.Start, rangeToString(view.End), formatMetaPartitionStatus(view.Status),
		view.LeaderAddr, strings.Join(view.Members, ","))
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
var step float64 = 1024

func fixUnit(curSize float64, curUnitIndex int) (newSize float64, newUnitIndex int) {
	if curSize >= step && curUnitIndex < len(units)-1 {
		return fixUnit(curSize/step, curUnitIndex+1)
	}
	return curSize, curUnitIndex
}

func formatSize(size uint64) string {
	fixedSize, fixedUnitIndex := fixUnit(float64(size), 0)
	return fmt.Sprintf("%.2f %v", fixedSize, units[fixedUnitIndex])
}

func formatTime(timeUnix int64) string {
	return time.Unix(timeUnix, 0).Format("2006-01-02 15:04:05")
}

func formatTimeToString(t time.Time) string {
	return t.Format("2006-01-02 15:04:05")
}

var dataReplicaTableRowPattern = "%-18v    %-12v    %-12v    %-12v    %-12v    %-12v    %-12v    %-12v    %-18v    %-10v"

func formatDataReplicaTableHeader() string {
	return fmt.Sprintf(dataReplicaTableRowPattern, "ADDR", "USEDSIZE", "TOTALSIZE", "ISLEADER", "FILECOUNT", "HASLOADRESPONSE", "NEEDSTOCOMPARE", "STATUS", "DISKPATH", "REPORT TIME")
}

var dataFileInCoreTableRowPattern = "%-12v    %-12v    %-10v    %-10v"

func formatDataFileInCoreTableHeader() string {
	return fmt.Sprintf(dataFileInCoreTableRowPattern, "KEY", "NAME", "LASTMODIFY", "FILEMETADATE")
}

var dataFileMetadateTableRowPattern = "%-12v    %-12v    %-10v    %-10v    %-10v    %-18v"

func formatDataFileMetadateTableHeader() string {
	return fmt.Sprintf(dataFileMetadateTableRowPattern, "", "", "", "CRC", "SIZE", "LOCADDR")
}

func formatDataFileInCoreMap(id string, indentation string, fileCore *proto.FileInCore, rowTable bool) string {
	var sb = strings.Builder{}
	if rowTable {
		sb.WriteString(fmt.Sprintf(dataFileInCoreTableRowPattern, id, fileCore.Name, fileCore.LastModify, ""))
		for _, v := range fileCore.MetadataArray {
			sb.WriteString("\n")
			sb.WriteString(formatDataFileMetadate("", v))
		}
		return sb.String()
	}
	sb.WriteString(fmt.Sprintf("%v- Name           : %v\n", indentation, fileCore.Name))
	sb.WriteString(fmt.Sprintf("%v  LastModify     : %v\n", indentation, fileCore.LastModify))
	sb.WriteString(fmt.Sprintf("%v  FileMetadate   : %v\n", indentation, fileCore.MetadataArray))
	return sb.String()
}

func formatDataFileMetadate(indentation string, fileMeta *proto.FileMetadata) string {
	return fmt.Sprintf(dataFileMetadateTableRowPattern, "", "", "", fileMeta.Crc, fileMeta.Size, fileMeta.LocAddr)
}

func formatDataReplica(indentation string, replica *proto.DataReplica, rowTable bool) string {
	if rowTable {
		return fmt.Sprintf(dataReplicaTableRowPattern, replica.Addr, formatSize(replica.Used), formatSize(replica.Total), replica.IsLeader,
			replica.FileCount, replica.HasLoadResponse, replica.NeedsToCompare, formatDataPartitionStatus(replica.Status),
			replica.DiskPath, formatTime(replica.ReportTime))
	}
	var sb = strings.Builder{}
	sb.WriteString(fmt.Sprintf("%v- Addr           : %v\n", indentation, replica.Addr))
	sb.WriteString(fmt.Sprintf("%v  Allocated      : %v\n", indentation, formatSize(replica.Used)))
	sb.WriteString(fmt.Sprintf("%v  Total          : %v\n", indentation, formatSize(replica.Total)))
	sb.WriteString(fmt.Sprintf("%v  IsLeader       : %v\n", indentation, replica.IsLeader))
	sb.WriteString(fmt.Sprintf("%v  FileCount      : %v\n", indentation, replica.FileCount))
	sb.WriteString(fmt.Sprintf("%v  HasLoadResponse: %v\n", indentation, replica.HasLoadResponse))
	sb.WriteString(fmt.Sprintf("%v  NeedsToCompare : %v\n", indentation, replica.NeedsToCompare))
	sb.WriteString(fmt.Sprintf("%v  Status         : %v\n", indentation, formatDataPartitionStatus(replica.Status)))
	sb.WriteString(fmt.Sprintf("%v  DiskPath       : %v\n", indentation, replica.DiskPath))
	sb.WriteString(fmt.Sprintf("%v  ReportTime     : %v\n", indentation, formatTime(replica.ReportTime)))
	return sb.String()
}

var metaReplicaTableRowPattern = "%-18v    %-6v    %-6v    %-10v"

func formatMetaReplicaTableHeader() string {
	return fmt.Sprintf(metaReplicaTableRowPattern, "ADDRESS", "ISLEADER", "STATUS", "REPORT TIME")
}

func formatMetaReplica(indentation string, replica *proto.MetaReplicaInfo, rowTable bool) string {
	if rowTable {
		return fmt.Sprintf(metaReplicaTableRowPattern, replica.Addr, replica.IsLeader, formatMetaPartitionStatus(replica.Status),
			formatTime(replica.ReportTime))
	}
	var sb = strings.Builder{}
	sb.WriteString(fmt.Sprintf("%v- Addr           : %v\n", indentation, replica.Addr))
	sb.WriteString(fmt.Sprintf("%v  Status         : %v\n", indentation, formatMetaPartitionStatus(replica.Status)))
	sb.WriteString(fmt.Sprintf("%v  IsLeader       : %v\n", indentation, replica.IsLeader))
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
	sb.WriteString(fmt.Sprintf("  Carry               : %v\n", dn.Carry))
	sb.WriteString(fmt.Sprintf("  Allocated ratio     : %v\n", dn.UsageRatio))
	sb.WriteString(fmt.Sprintf("  Allocated           : %v\n", formatSize(dn.Used)))
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

var metaNodeDetailTableRowPattern = "%-6v    %-6v    %-18v    %-6v    %-6v    %-6v    %-10v"

func formatMetaNodeDetailTableHeader() string {
	return fmt.Sprintf(metaNodeDetailTableRowPattern, "ID", "ZONE", "ADDRESS", "USED", "TOTAL", "STATUS", "REPORT TIME")
}

func formatMetaNodeDetail(mn *proto.MetaNodeInfo, rowTable bool) string {
	if rowTable {
		return fmt.Sprintf(metaNodeDetailTableRowPattern, mn.ID, mn.ZoneName, mn.Addr, mn.Used, mn.Total, mn.IsActive, formatTimeToString(mn.ReportTime))
	}
	var sb = strings.Builder{}
	sb.WriteString(fmt.Sprintf("  ID                  : %v\n", mn.ID))
	sb.WriteString(fmt.Sprintf("  Address             : %v\n", mn.Addr))
	sb.WriteString(fmt.Sprintf("  Carry               : %v\n", mn.Carry))
	sb.WriteString(fmt.Sprintf("  Threshold           : %v\n", mn.Threshold))
	sb.WriteString(fmt.Sprintf("  MaxMemAvailWeight   : %v\n", formatSize(mn.MaxMemAvailWeight)))
	sb.WriteString(fmt.Sprintf("  Allocated           : %v\n", formatSize(mn.Used)))
	sb.WriteString(fmt.Sprintf("  Total               : %v\n", formatSize(mn.Total)))
	sb.WriteString(fmt.Sprintf("  Zone                : %v\n", mn.ZoneName))
	sb.WriteString(fmt.Sprintf("  IsActive            : %v\n", formatNodeStatus(mn.IsActive)))
	sb.WriteString(fmt.Sprintf("  Report time         : %v\n", formatTimeToString(mn.ReportTime)))
	sb.WriteString(fmt.Sprintf("  Partition count     : %v\n", mn.MetaPartitionCount))
	sb.WriteString(fmt.Sprintf("  Persist partitions  : %v\n", mn.PersistenceMetaPartitions))
	return sb.String()
}

func formatZoneView(zv *proto.ZoneView) string {
	var sb = strings.Builder{}
	sb.WriteString(fmt.Sprintf("Zone Name:   %v\n", zv.Name))
	sb.WriteString(fmt.Sprintf("Status:      %v\n", zv.Status))
	sb.WriteString(fmt.Sprintf("\n"))
	for index, ns := range zv.NodeSet {
		sb.WriteString(fmt.Sprintf("NodeSet-%v:\n", index))
		sb.WriteString(fmt.Sprintf("  DataNodes[%v]:\n", ns.DataNodeLen))
		sb.WriteString(fmt.Sprintf("    %v\n", formatNodeViewTableHeader()))
		for _, nv := range ns.DataNodes {
			sb.WriteString(fmt.Sprintf("    %v\n", formatNodeView(&nv, true)))
		}
		sb.WriteString(fmt.Sprintf("\n"))
		sb.WriteString(fmt.Sprintf("  MetaNodes[%v]:\n", ns.MetaNodeLen))
		sb.WriteString(fmt.Sprintf("    %v\n", formatNodeViewTableHeader()))
		for _, nv := range ns.MetaNodes {
			sb.WriteString(fmt.Sprintf("    %v\n", formatNodeView(&nv, true)))
		}
	}
	return sb.String()
}

var quotaTableRowPattern = "%-6v    %-30v    %-15v    %-8v    %-18v    %-10v    %-10v    %-12v    %-12v    %-10v    %-10v    %-10v    %-10v"

func formatQuotaTableHeader() string {
	return fmt.Sprintf(quotaTableRowPattern, "ID", "PATH", "VOL", "STATUS", "CTIME",
		"PID", "RINODE", "LIMITEDFILES", "LIMITEDBYTES", "USEDFILES", "USEDBYTES", "MAXFILES", "MAXBYTES")
}

func formatQuotaInfo(info *proto.QuotaInfo) string {
	var status string
	if info.Status == proto.QuotaInit {
		status = "Init"
	} else if info.Status == proto.QuotaComplete {
		status = "Complete"
	} else {
		status = "Deleting"
	}
	t := time.Unix(info.CTime, 0)
	return fmt.Sprintf(quotaTableRowPattern, info.QuotaId, info.FullPath, info.VolName, status, t.Format("2006-01-02 15:04:05"), info.PartitionId,
		info.RootInode, info.LimitedInfo.LimitedFiles, info.LimitedInfo.LimitedBytes, info.UsedInfo.UsedFiles, info.UsedInfo.UsedBytes,
		info.MaxFiles, info.MaxBytes)
}
