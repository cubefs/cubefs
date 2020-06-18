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
	sb.WriteString(fmt.Sprintf("  ID                   : %v\n", svv.ID))
	sb.WriteString(fmt.Sprintf("  Name                 : %v\n", svv.Name))
	sb.WriteString(fmt.Sprintf("  Owner                : %v\n", svv.Owner))
	sb.WriteString(fmt.Sprintf("  Zone                 : %v\n", svv.ZoneName))
	sb.WriteString(fmt.Sprintf("  Status               : %v\n", formatVolumeStatus(svv.Status)))
	sb.WriteString(fmt.Sprintf("  Capacity             : %v GB\n", svv.Capacity))
	sb.WriteString(fmt.Sprintf("  Create time          : %v\n", svv.CreateTime))
	sb.WriteString(fmt.Sprintf("  Authenticate         : %v\n", formatEnabledDisabled(svv.Authenticate)))
	sb.WriteString(fmt.Sprintf("  Follower read        : %v\n", formatEnabledDisabled(svv.FollowerRead)))
	sb.WriteString(fmt.Sprintf("  Cross zone           : %v\n", formatEnabledDisabled(svv.CrossZone)))
	sb.WriteString(fmt.Sprintf("  Inode count          : %v\n", svv.InodeCount))
	sb.WriteString(fmt.Sprintf("  Dentry count         : %v\n", svv.DentryCount))
	sb.WriteString(fmt.Sprintf("  Max metaPartition ID : %v\n", svv.MaxMetaPartitionID))
	sb.WriteString(fmt.Sprintf("  Meta partition count : %v\n", svv.MpCnt))
	sb.WriteString(fmt.Sprintf("  Meta replicas        : %v\n", svv.MpReplicaNum))
	sb.WriteString(fmt.Sprintf("  Data partition count : %v\n", svv.DpCnt))
	sb.WriteString(fmt.Sprintf("  Data replicas        : %v", svv.DpReplicaNum))
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
	dataPartitionTablePattern = "%-8v    %-8v    %-10v    %-18v    %-18v"
	dataPartitionTableHeader  = fmt.Sprintf(dataPartitionTablePattern,
		"ID", "REPLICAS", "STATUS", "LEADER", "MEMBERS")
)

func formatDataPartitionTableRow(view *proto.DataPartitionResponse) string {
	return fmt.Sprintf(dataPartitionTablePattern,
		view.PartitionID, view.ReplicaNum, formatDataPartitionStatus(view.Status), view.LeaderAddr,
		strings.Join(view.Hosts, ","))
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

func formatReplica(indentation string, replica *proto.DataReplica) string {
	var sb = strings.Builder{}
	sb.WriteString(fmt.Sprintf("%vStatus:          %v\n", indentation, replica.Status))
	sb.WriteString(fmt.Sprintf("%vTotal:           %v GB\n", indentation, replica.Total/(1024*1024*1024)))
	sb.WriteString(fmt.Sprintf("%vDiskPath:        %v\n", indentation, replica.DiskPath))
	sb.WriteString(fmt.Sprintf("%vAddr:            %v\n", indentation, replica.Addr))
	sb.WriteString(fmt.Sprintf("%vUsed:            %v GB\n", indentation, replica.Used))
	sb.WriteString(fmt.Sprintf("%vIsLeader:        %v\n", indentation, replica.IsLeader))
	sb.WriteString(fmt.Sprintf("%vFileCount:       %v\n", indentation, replica.FileCount))
	sb.WriteString(fmt.Sprintf("%vHasLoadResponse: %v\n", indentation, replica.HasLoadResponse))
	sb.WriteString(fmt.Sprintf("%vNeedsToCompare:  %v\n", indentation, replica.NeedsToCompare))
	sb.WriteString(fmt.Sprintf("%vReportTime:      %v\n", indentation, time.Unix(replica.ReportTime, 0).Format("2006-01-02 15:04:05")))
	return sb.String()
}
func formatPeer(indentation string, peer proto.Peer) string {
	var sb = strings.Builder{}
	sb.WriteString(fmt.Sprintf("%vID:   %v\n", indentation, peer.ID))
	sb.WriteString(fmt.Sprintf("%vPeer: %v\n", indentation, peer.Addr))
	return sb.String()
}
