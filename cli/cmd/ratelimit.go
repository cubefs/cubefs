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
	"strings"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/master"
	"github.com/spf13/cobra"
)

const (
	cmdRateLimitUse       = "ratelimit [COMMAND]"
	cmdRateLimitShort     = "Manage requests rate limit"
	cmdRateLimitInfoShort = "Current rate limit"
	cmdRateLimitSetShort  = "Set rate limit"
	minRate               = 100
	minPartRate           = 1
)

func newRateLimitCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   cmdRateLimitUse,
		Short: cmdRateLimitShort,
	}
	cmd.AddCommand(
		newRateLimitInfoCmd(client),
		newRateLimitSetCmd(client),
	)
	return cmd
}

func newRateLimitInfoCmd(client *master.MasterClient) *cobra.Command {
	var vol string
	var cmd = &cobra.Command{
		Use:   CliOpInfo,
		Short: cmdRateLimitInfoShort,
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var info *proto.LimitInfo
			if info, err = client.AdminAPI().GetLimitInfo(vol); err != nil {
				errout("Get cluster info fail:\n%v\n", err)
			}
			stdout("[Cluster rate limit]\n")
			stdout(formatRateLimitInfo(info))
			stdout("\n")
		},
	}
	cmd.Flags().StringVar(&vol, "volume", "", "volume (empty volume acts as default)")
	return cmd
}

func newRateLimitSetCmd(client *master.MasterClient) *cobra.Command {
	var info proto.RateLimitInfo
	var cmd = &cobra.Command{
		Use:   CliOpSet,
		Short: cmdRateLimitSetShort,
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			if (info.ClientReadVolRate > 0 && info.ClientReadVolRate < minRate) ||
				(info.ClientWriteVolRate > 0 && info.ClientWriteVolRate < minRate) ||
				(info.MetaNodeReqRate > 0 && info.MetaNodeReqRate < minRate) ||
				(info.MetaNodeReqOpRate > 0 && info.MetaNodeReqOpRate < minRate) ||
				(info.DataNodeReqRate > 0 && info.DataNodeReqRate < minRate) ||
				(info.DataNodeReqOpRate > 0 && info.DataNodeReqOpRate < minRate) ||
				(info.DataNodeReqVolOpRate > 0 && info.DataNodeReqVolOpRate < minRate) {
				errout("limit rate can't be less than %d\n", minRate)
			}
			if (info.DataNodeReqVolPartRate > 0 && info.DataNodeReqVolPartRate < minPartRate) ||
				(info.DataNodeReqVolOpPartRate > 0 && info.DataNodeReqVolOpPartRate < minPartRate) {
				errout("limit rate can't be less than %d\n", minPartRate)
			}
			if info.ClientVolOpRate < -2 {
				errout("client meta op limit rate can't be less than %d\n", -1)
			}
			msg := ""
			if info.ClientReadVolRate >= 0 {
				msg += fmt.Sprintf("clientReadVolRate: %d, volume: %s, ", info.ClientReadVolRate, info.Volume)
			}
			if info.ClientWriteVolRate >= 0 {
				msg += fmt.Sprintf("clientWriteVolRate: %d, volume: %s, ", info.ClientWriteVolRate, info.Volume)
			}
			if info.ClientVolOpRate > -2 {
				msg += fmt.Sprintf("clientVolOpRate: %v, ", info.ClientVolOpRate)
			}
			if info.DataNodeRepairTaskCount > 0 {
				msg += fmt.Sprintf("dataNodeRepairTaskCount: %d, ", info.DataNodeRepairTaskCount)
			}
			if info.DataNodeRepairTaskSSDZone > 0 {
				msg += fmt.Sprintf("dataNodeRepairTaskSSDZoneCount: %d, ", info.DataNodeRepairTaskSSDZone)
			}
			if info.MetaNodeReqRate >= 0 {
				msg += fmt.Sprintf("metaNodeReqRate: %d, ", info.MetaNodeReqRate)
			}
			if info.MetaNodeReqOpRate >= 0 {
				msg += fmt.Sprintf("metaNodeReqOpRate: %d, opcode: %d, ", info.MetaNodeReqOpRate, info.Opcode)
			}
			if info.DataNodeRepairTaskZoneCount >= 0 {
				msg += fmt.Sprintf("DataNodeRepairTaskZoneCount: %d, zone: %s, ", info.DataNodeRepairTaskZoneCount, info.ZoneName)
			}
			if info.DataNodeReqRate >= 0 {
				msg += fmt.Sprintf("dataNodeReqZoneRate: %d, zone: %s, ", info.DataNodeReqRate, info.ZoneName)
			}
			if info.DataNodeReqOpRate >= 0 {
				msg += fmt.Sprintf("dataNodeReqZoneOpRate: %d, zone: %s, opcode: %d, ", info.DataNodeReqOpRate, info.ZoneName, info.Opcode)
			}
			if info.DataNodeReqVolOpRate >= 0 {
				msg += fmt.Sprintf("dataNodeReqZoneVolOpRate: %d, zone: %s, vol:%s, opcode: %d, ", info.DataNodeReqVolOpRate, info.ZoneName, info.Volume, info.Opcode)
			}
			if info.DataNodeReqVolPartRate >= 0 {
				msg += fmt.Sprintf("dataNodeReqVolPartRate: %d, volume: %s, ", info.DataNodeReqVolPartRate, info.Volume)
			}
			if info.DataNodeReqVolOpPartRate >= 0 {
				msg += fmt.Sprintf("dataNodeReqVolOpPartRate: %d, volume: %s, opcode: %d, ", info.DataNodeReqVolOpPartRate, info.Volume, info.Opcode)
			}
			if info.ExtentMergeIno != "" {
				msg += fmt.Sprintf("extentMergeIno: %s, volume: %s, ", info.ExtentMergeIno, info.Volume)
			}
			if info.ExtentMergeSleepMs >= 0 {
				msg += fmt.Sprintf("extentMergeSleepMs: %d, ", info.ExtentMergeSleepMs)
			}
			if info.MetaNodeDumpWaterLevel > 0 {
				msg += fmt.Sprintf("dumpWaterLevel    : %d, ", info.MetaNodeDumpWaterLevel)
			}
			if msg == "" {
				stdout("No valid parameters\n")
				return
			}

			stdout("Set rate limit: %s\n", strings.TrimRight(msg, " ,"))

			if err = client.AdminAPI().SetRateLimit(&info); err != nil {
				errout("Set rate limit fail:\n%v\n", err)
			}
			stdout("Set rate limit success: %s\n", strings.TrimRight(msg, " ,"))
		},
	}
	cmd.Flags().Int64Var(&info.DataNodeRepairTaskCount, "dataNodeRepairTaskCount", -1, "data node repair task count")
	cmd.Flags().Int64Var(&info.DataNodeRepairTaskSSDZone, "dataNodeRepairTaskSSDZoneCount", -1, "data node repair task count of ssd zones")
	cmd.Flags().StringVar(&info.ZoneName, "zone", "", "zone (empty zone acts as default)")
	cmd.Flags().StringVar(&info.Volume, "volume", "", "volume (empty volume acts as default)")
	cmd.Flags().Int8Var(&info.Opcode, "opcode", 0, "opcode (zero opcode acts as default)")
	cmd.Flags().Int64Var(&info.MetaNodeReqRate, "metaNodeReqRate", -1, "meta node request rate limit")
	cmd.Flags().Int64Var(&info.MetaNodeReqOpRate, "metaNodeReqOpRate", -1, "meta node request rate limit for opcode")
	cmd.Flags().Int64Var(&info.DataNodeReqRate, "dataNodeReqZoneRate", -1, "data node request rate limit")
	cmd.Flags().Int64Var(&info.DataNodeReqOpRate, "dataNodeReqZoneOpRate", -1, "data node request rate limit for opcode")
	cmd.Flags().Int64Var(&info.DataNodeReqVolOpRate, "dataNodeReqZoneVolOpRate", -1, "data node request rate limit for a given vol & opcode")
	cmd.Flags().Int64Var(&info.DataNodeReqVolPartRate, "dataNodeReqVolPartRate", -1, "data node per partition request rate limit for a given volume")
	cmd.Flags().Int64Var(&info.DataNodeReqVolOpPartRate, "dataNodeReqVolOpPartRate", -1, "data node per partition request rate limit for a given volume & opcode")
	cmd.Flags().Int64Var(&info.ClientReadVolRate, "clientReadVolRate", -1, "client read rate limit for volume")
	cmd.Flags().Int64Var(&info.ClientWriteVolRate, "clientWriteVolRate", -1, "client write limit rate for volume")
	cmd.Flags().Int64Var(&info.ClientVolOpRate, "clientVolOpRate", -2, "client meta op limit rate. '-1': unlimit, '0': disable")
	cmd.Flags().StringVar(&info.ExtentMergeIno, "extentMergeIno", "", "comma separated inodes to be merged. '-1': no inodes, '0': all inodes")
	cmd.Flags().Int64Var(&info.ExtentMergeSleepMs, "extentMergeSleepMs", -1, "extent merge interval(ms)")
	cmd.Flags().Int64Var(&info.DnFixTinyDeleteRecordLimit, "fixTinyDeleteRecordLimit", -1, "data node fix tiny delete record limit")
	cmd.Flags().Int64Var(&info.DataNodeRepairTaskZoneCount, "dataNodeRepairTaskZoneCount", -1, "data node repair task count of target zone")
	cmd.Flags().Int64Var(&info.MetaNodeDumpWaterLevel, "metaNodeDumpWaterLevel", -1, "meta node dump snap shot water level")
	return cmd
}

func formatRateLimitInfo(info *proto.LimitInfo) string {
	var sb = strings.Builder{}
	sb.WriteString(fmt.Sprintf("  Cluster name                : %v\n", info.Cluster))
	sb.WriteString(fmt.Sprintf("  DnFixTinyDeleteRecordLimit  : %v\n", info.DataNodeFixTinyDeleteRecordLimitOnDisk))
	sb.WriteString(fmt.Sprintf("  MetaNodeReqRate             : %v\n", info.MetaNodeReqRateLimit))
	sb.WriteString(fmt.Sprintf("  MetaNodeReqOpRateMap        : %v\n", info.MetaNodeReqOpRateLimitMap))
	sb.WriteString(fmt.Sprintf("  (map[opcode]limit)\n"))
	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("  DataNodeRepairTaskCount     : %v\n", info.DataNodeRepairTaskLimitOnDisk))
	sb.WriteString(fmt.Sprintf("  DataNodeRepairTaskCluster   : %v\n", info.DataNodeRepairClusterTaskLimitOnDisk))
	sb.WriteString(fmt.Sprintf("  DataNodeRepairTaskSSDZone   : %v\n", info.DataNodeRepairSSDZoneTaskLimitOnDisk))
	sb.WriteString(fmt.Sprintf("  DataNodeReqZoneRateMap      : %v\n", info.DataNodeReqZoneRateLimitMap))
	sb.WriteString(fmt.Sprintf("  (map[zone]limit)\n"))
	sb.WriteString(fmt.Sprintf("  DataNodeReqZoneOpRateMap    : %v\n", info.DataNodeReqZoneOpRateLimitMap))
	sb.WriteString(fmt.Sprintf("  (map[zone]map[opcode]limit)\n"))
	sb.WriteString(fmt.Sprintf("  DataNodeReqZoneVolOpRateMap : %v\n", info.DataNodeReqZoneVolOpRateLimitMap))
	sb.WriteString(fmt.Sprintf("  (map[zone]map[vol]map[opcode]limit)\n"))
	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("  DataNodeReqVolPartRateMap   : %v\n", info.DataNodeReqVolPartRateLimitMap))
	sb.WriteString(fmt.Sprintf("  (map[volume]limit - per partition)\n"))
	sb.WriteString(fmt.Sprintf("  DataNodeReqVolOpPartRateMap : %v\n", info.DataNodeReqVolOpPartRateLimitMap))
	sb.WriteString(fmt.Sprintf("  (map[volume]map[opcode]limit - per partition)\n"))
	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("  ClientReadVolRateMap        : %v\n", info.ClientReadVolRateLimitMap))
	sb.WriteString(fmt.Sprintf("  (map[volume]limit of specified volume)\n"))
	sb.WriteString(fmt.Sprintf("  ClientWriteVolRateMap       : %v\n", info.ClientWriteVolRateLimitMap))
	sb.WriteString(fmt.Sprintf("  (map[volume]limit of specified volume)\n"))
	sb.WriteString(fmt.Sprintf("  ClientVolOpRate             : %v\n", info.ClientVolOpRateLimit))
	sb.WriteString(fmt.Sprintf("  (map[opcode]limit of specified volume)\n"))
	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("  ExtentMergeIno              : %v\n", info.ExtentMergeIno))
	sb.WriteString(fmt.Sprintf("  (map[volume][]inode)\n"))
	sb.WriteString(fmt.Sprintf("  ExtentMergeSleepMs          : %v\n", info.ExtentMergeSleepMs))
	sb.WriteString(fmt.Sprintf("  DataNodeRepairTaskZoneLimit : %v\n", info.DataNodeRepairTaskCountZoneLimit))
	sb.WriteString(fmt.Sprintf("  MetaNodeDumpWaterLevel      : %v\n", info.MetaNodeDumpWaterLevel))
	sb.WriteString(fmt.Sprintf("  (map[zone]limit)\n"))
	return sb.String()
}
