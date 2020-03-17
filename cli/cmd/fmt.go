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
	sb.WriteString(fmt.Sprintf("  Name          : %v\n", cv.Name))
	sb.WriteString(fmt.Sprintf("  Auto allocate : %v\n", !cv.DisableAutoAlloc))
	sb.WriteString(fmt.Sprintf("  MetaNode count: %v\n", len(cv.MetaNodes)))
	sb.WriteString(fmt.Sprintf("  DataNode count: %v\n", len(cv.DataNodes)))
	sb.WriteString(fmt.Sprintf("  Volume count  : %v\n", len(cv.VolStatInfo)))
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
	sb.WriteString(fmt.Sprintf("  Data replicas       : %v\n", svv.DpReplicaNum))
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
	volumeInfoTablePattern = "  %10v\t%10v\t%10v\t%10v\t%8v\t%10v"
	volumeInfoTableHeader  = fmt.Sprintf(volumeInfoTablePattern, "VOLUME", "OWNER", "USED", "TOTAL", "STATUS", "CREATE TIME")
)

func formatVolInfoTableRow(vi *proto.VolInfo) string {
	return fmt.Sprintf(volumeInfoTablePattern,
		vi.Name, vi.Owner, vi.UsedSize, vi.TotalSize, formatVolumeStatus(vi.Status), time.Unix(vi.CreateTime, 0))
}

var (
	dataPartitionTablePattern = "  %10v\t%8v\t%10v\t%18v\t%18v"
	dataPartitionTableHeader  = fmt.Sprintf(dataPartitionTablePattern,
		"ID", "REPLICAS", "STATUS", "LEADER", "MEMBERS")
)

func formatDataPartitionTableRow(view *proto.DataPartitionResponse) string {
	return fmt.Sprintf(dataPartitionTablePattern,
		view.PartitionID, view.ReplicaNum, formatDataPartitionStatus(view.Status), view.LeaderAddr, view.Hosts)
}

var (
	metaPartitionTablePattern = "  %10v\t%12v\t%12v\t%12v\t%10v\t%18v\t%18v"
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
		view.PartitionID, view.MaxInodeID, view.Start, rangeToString(view.End), formatMetaPartitionStatus(view.Status), view.LeaderAddr, view.Members)
}

var (
	userInfoTablePattern = "  %10v    %6v    %16v    %32v    %10v"
	userInfoTableHeader  = fmt.Sprintf(userInfoTablePattern,
		"ID", "TYPE", "ACCESS KEY", "SECRET KEY", "CREATE TIME")
)

func formatUserInfoTableRow(akp *proto.AKPolicy) string {
	return fmt.Sprintf(userInfoTablePattern,
		akp.UserID, formatUserType(akp.UserType), akp.AccessKey, akp.SecretKey, akp.CreateTime)
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
