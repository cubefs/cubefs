// Copyright 2018 The Chubao Authors.
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

	"github.com/chubaofs/chubaofs/proto"
)

func formatClusterView(cv *proto.ClusterView) string {
	var sb = strings.Builder{}
	sb.WriteString(fmt.Sprintf("  Cluster name  : %v\n", cv.Name))
	sb.WriteString(fmt.Sprintf("  Master leader : %v\n", cv.LeaderAddr))
	sb.WriteString(fmt.Sprintf("  Auto allocate : %v\n", formatEnabledDisabled(!cv.DisableAutoAlloc)))
	sb.WriteString(fmt.Sprintf("  MetaNode count: %v\n", len(cv.MetaNodes)))
	sb.WriteString(fmt.Sprintf("  MetaNode used : %v GB\n", cv.MetaNodeStatInfo.UsedGB))
	sb.WriteString(fmt.Sprintf("  MetaNode total: %v GB\n", cv.MetaNodeStatInfo.TotalGB))
	sb.WriteString(fmt.Sprintf("  DataNode count: %v\n", len(cv.DataNodes)))
	sb.WriteString(fmt.Sprintf("  DataNode used : %v GB\n", cv.DataNodeStatInfo.UsedGB))
	sb.WriteString(fmt.Sprintf("  DataNode total: %v GB\n", cv.DataNodeStatInfo.TotalGB))
	sb.WriteString(fmt.Sprintf("  Volume count  : %v", len(cv.VolStatInfo)))
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
	sb.WriteString(fmt.Sprintf("  ID                  : %v\n", svv.ID))
	sb.WriteString(fmt.Sprintf("  Name                : %v\n", svv.Name))
	sb.WriteString(fmt.Sprintf("  Owner               : %v\n", svv.Owner))
	sb.WriteString(fmt.Sprintf("  Zone                : %v\n", svv.ZoneName))
	sb.WriteString(fmt.Sprintf("  Status              : %v\n", formatVolumeStatus(svv.Status)))
	sb.WriteString(fmt.Sprintf("  Capacity            : %v GB\n", svv.Capacity))
	sb.WriteString(fmt.Sprintf("  Create time         : %v\n", svv.CreateTime))
	sb.WriteString(fmt.Sprintf("  Authenticate        : %v\n", formatEnabledDisabled(svv.Authenticate)))
	sb.WriteString(fmt.Sprintf("  Follower read       : %v\n", formatEnabledDisabled(svv.FollowerRead)))
	sb.WriteString(fmt.Sprintf("  Cross zone          : %v\n", formatEnabledDisabled(svv.CrossZone)))
	sb.WriteString(fmt.Sprintf("  Meta partition count: %v\n", svv.MpCnt))
	sb.WriteString(fmt.Sprintf("  Meta replicas       : %v\n", svv.MpReplicaNum))
	sb.WriteString(fmt.Sprintf("  Data partition count: %v\n", svv.DpCnt))
	sb.WriteString(fmt.Sprintf("  Data replicas       : %v", svv.DpReplicaNum))
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
	partitionInfoTablePattern = "%-8v    %-8v    %-10v     %-18v    %-18v"
	partitionInfoTableHeader  = fmt.Sprintf(partitionInfoTablePattern,
		"ID", "VOLUME", "REPLICAS", "STATUS", "MEMBERS")
)

func formatDataPartitionInfoRow(partition *proto.DataPartitionInfo) string {
	return fmt.Sprintf(partitionInfoTablePattern,
		partition.PartitionID, partition.VolName, partition.ReplicaNum, formatDataPartitionStatus(partition.Status), strings.Join(partition.Hosts, ","))
}

func formatMetaPartitionInfoRow(partition *proto.MetaPartitionInfo) string {
	return fmt.Sprintf(partitionInfoTablePattern,
		partition.PartitionID, partition.VolName, partition.ReplicaNum, formatDataPartitionStatus(partition.Status), strings.Join(partition.Hosts, ","))
}

func formatDataPartitionInfo(partition *proto.DataPartitionInfo) string {
	var sb = strings.Builder{}
	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("volume name   : %v\n", partition.VolName))
	sb.WriteString(fmt.Sprintf("volume ID     : %v\n", partition.VolID))
	sb.WriteString(fmt.Sprintf("PartitionID   : %v\n", partition.PartitionID))
	sb.WriteString(fmt.Sprintf("Status        : %v\n", partition.Status))
	sb.WriteString(fmt.Sprintf("LastLoadedTime: %v\n", formatTime(partition.LastLoadedTime)))
	sb.WriteString(fmt.Sprintf("Replicas : \n"))
	for _, replica := range partition.Replicas {
		sb.WriteString(formatDataReplica("  ", replica))
		sb.WriteString("\n")
	}
	sb.WriteString(fmt.Sprintf("Peers :\n"))
	for _, peer := range partition.Peers {
		sb.WriteString(formatPeer("  ", peer))
	}
	sb.WriteString(fmt.Sprintf("Hosts :\n"))
	for _, host := range partition.Hosts {
		sb.WriteString(fmt.Sprintf("  [%v]\n", host))
	}
	sb.WriteString(fmt.Sprintf("Zones :\n"))
	for _, zone := range partition.Zones {
		sb.WriteString(fmt.Sprintf("  [%v]\n", zone))
	}
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
	sb.WriteString(fmt.Sprintf("Replicas : \n"))
	for _, replica := range partition.Replicas {
		sb.WriteString(formatMetaReplica("  ", replica))
		sb.WriteString("\n")
	}
	sb.WriteString(fmt.Sprintf("Peers :\n"))
	for _, peer := range partition.Peers {
		sb.WriteString(formatPeer("  ", peer))
	}
	sb.WriteString(fmt.Sprintf("Hosts :\n"))
	for _, host := range partition.Hosts {
		sb.WriteString(fmt.Sprintf("  [%v]\n", host))
	}
	sb.WriteString(fmt.Sprintf("Zones :\n"))
	for _, zone := range partition.Zones {
		sb.WriteString(fmt.Sprintf("  [%v]\n", zone))
	}
	sb.WriteString(fmt.Sprintf("MissingNodes :\n"))
	for partitionHost, id := range partition.MissNodes {
		sb.WriteString(fmt.Sprintf("  [%v, %v]\n", partitionHost, id))
	}
	return sb.String()
}

var (
	metaPartitionTablePattern = "%-8v    %-12v    %-12v    %-12v    %-10v    %-18v    %-18v"
	metaPartitionTableHeader  = fmt.Sprintf(metaPartitionTablePattern,
		"ID", "MAX INODE", "START", "END", "STATUS", "LEADER", "MEMBERS")
)

func formatMetaPartitionTableRow(view *proto.MetaPartitionView) string {
	var rangeToString = func(num uint64) string {
		if num >= math.MaxInt64 {
			return "unlimited"
		}
		return strconv.FormatUint(num, 10)
	}
	return fmt.Sprintf(metaPartitionTablePattern,
		view.PartitionID, view.MaxInodeID, view.Start, rangeToString(view.End), formatMetaPartitionStatus(view.Status),
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

func formatTime(timeUnix int64) string {
	return time.Unix(timeUnix, 0).Format("2006-01-02 15:04:05")
}

func formatDataReplica(indentation string, replica *proto.DataReplica) string {
	var sb = strings.Builder{}
	sb.WriteString(fmt.Sprintf("%v- Addr           : %v\n", indentation, replica.Addr))
	sb.WriteString(fmt.Sprintf("%v  Status         : %v\n", indentation, replica.Status))
	sb.WriteString(fmt.Sprintf("%v  Total          : %v\n", indentation, formatSize(replica.Total)))
	sb.WriteString(fmt.Sprintf("%v  DiskPath       : %v\n", indentation, replica.DiskPath))
	sb.WriteString(fmt.Sprintf("%v  Used           : %v\n", indentation, formatSize(replica.Used)))
	sb.WriteString(fmt.Sprintf("%v  IsLeader       : %v\n", indentation, replica.IsLeader))
	sb.WriteString(fmt.Sprintf("%v  FileCount      : %v\n", indentation, replica.FileCount))
	sb.WriteString(fmt.Sprintf("%v  HasLoadResponse: %v\n", indentation, replica.HasLoadResponse))
	sb.WriteString(fmt.Sprintf("%v  NeedsToCompare : %v\n", indentation, replica.NeedsToCompare))
	sb.WriteString(fmt.Sprintf("%v  ReportTime     : %v\n", indentation, time.Unix(replica.ReportTime, 0).Format("2006-01-02 15:04:05")))
	return sb.String()
}

func formatMetaReplica(indentation string, replica *proto.MetaReplicaInfo) string {
	var sb = strings.Builder{}
	sb.WriteString(fmt.Sprintf("%v- Addr           : %v\n", indentation, replica.Addr))
	sb.WriteString(fmt.Sprintf("%v  Status         : %v\n", indentation, replica.Status))
	sb.WriteString(fmt.Sprintf("%v  IsLeader       : %v\n", indentation, replica.IsLeader))
	sb.WriteString(fmt.Sprintf("%v  ReportTime     : %v\n", indentation, time.Unix(replica.ReportTime, 0).Format("2006-01-02 15:04:05")))
	return sb.String()
}

func formatPeer(indentation string, peer proto.Peer) string {
	var sb = strings.Builder{}
	sb.WriteString(fmt.Sprintf("%v- ID  :%v\n", indentation, peer.ID))
	sb.WriteString(fmt.Sprintf("%v  Peer:%v\n", indentation, peer.Addr))
	return sb.String()
}

func formatDataNodeDetail(dn *proto.DataNodeInfo) string {
	var sb = strings.Builder{}
	sb.WriteString(fmt.Sprintf("  ID                  : %v\n", dn.ID))
	sb.WriteString(fmt.Sprintf("  Address             : %v\n", dn.Addr))
	sb.WriteString(fmt.Sprintf("  Carry               : %v\n", dn.Carry))
	sb.WriteString(fmt.Sprintf("  Used ratio          : %v\n", dn.UsageRatio))
	sb.WriteString(fmt.Sprintf("  Used                : %v\n", formatSize(dn.Used)))
	sb.WriteString(fmt.Sprintf("  Available           : %v\n", formatSize(dn.AvailableSpace)))
	sb.WriteString(fmt.Sprintf("  Total               : %v\n", formatSize(dn.Total)))
	sb.WriteString(fmt.Sprintf("  Zone                : %v\n", dn.ZoneName))
	sb.WriteString(fmt.Sprintf("  IsActive            : %v\n", formatNodeStatus(dn.IsActive)))
	sb.WriteString(fmt.Sprintf("  Report time         : %v\n", dn.ReportTime))
	sb.WriteString(fmt.Sprintf("  Partition count     : %v\n", dn.DataPartitionCount))
	sb.WriteString(fmt.Sprintf("  Bad disks           : %v\n", dn.BadDisks))
	sb.WriteString(fmt.Sprintf("  Persist partitions  : %v\n", dn.PersistenceDataPartitions))
	return sb.String()
}

func formatMetaNodeDetail(mn *proto.MetaNodeInfo) string {
	var sb = strings.Builder{}
	sb.WriteString(fmt.Sprintf("  ID                  : %v\n", mn.ID))
	sb.WriteString(fmt.Sprintf("  Address             : %v\n", mn.Addr))
	sb.WriteString(fmt.Sprintf("  Carry               : %v\n", mn.Carry))
	sb.WriteString(fmt.Sprintf("  Threshold           : %v\n", mn.Threshold))
	sb.WriteString(fmt.Sprintf("  MaxMemAvailWeight   : %v\n", formatSize(mn.MaxMemAvailWeight)))
	sb.WriteString(fmt.Sprintf("  Used                : %v\n", formatSize(mn.Used)))
	sb.WriteString(fmt.Sprintf("  Total               : %v\n", formatSize(mn.Total)))
	sb.WriteString(fmt.Sprintf("  Zone                : %v\n", mn.ZoneName))
	sb.WriteString(fmt.Sprintf("  IsActive            : %v\n", formatNodeStatus(mn.IsActive)))
	sb.WriteString(fmt.Sprintf("  Report time         : %v\n", mn.ReportTime))
	sb.WriteString(fmt.Sprintf("  Partition count     : %v\n", mn.MetaPartitionCount))
	sb.WriteString(fmt.Sprintf("  Persist partitions  : %v\n", mn.PersistenceMetaPartitions))
	return sb.String()
}
