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
	var cmd = &cobra.Command{
		Use:   CliOpInfo,
		Short: cmdRateLimitInfoShort,
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var info *proto.LimitInfo
			if info, err = client.AdminAPI().GetLimitInfo(); err != nil {
				errout("Get cluster info fail:\n%v\n", err)
			}
			stdout("[Cluster rate limit]\n")
			stdout(formatRateLimitInfo(info))
			stdout("\n")
		},
	}
	return cmd
}

func newRateLimitSetCmd(client *master.MasterClient) *cobra.Command {
	var info proto.RateLimitInfo
	var cmd = &cobra.Command{
		Use:   CliOpSet,
		Short: cmdRateLimitSetShort,
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			if (info.ClientReadRate > 0 && info.ClientReadRate < minRate) ||
				(info.ClientWriteRate > 0 && info.ClientWriteRate < minRate) ||
				(info.MetaNodeReqRate > 0 && info.MetaNodeReqRate < minRate) ||
				(info.MetaNodeReqOpRate > 0 && info.MetaNodeReqOpRate < minRate) ||
				(info.DataNodeReqRate > 0 && info.DataNodeReqRate < minRate) ||
				(info.DataNodeReqOpRate > 0 && info.DataNodeReqOpRate < minRate) ||
				(info.DataNodeReqVolPartRate > 0 && info.DataNodeReqVolPartRate < minRate) ||
				(info.DataNodeReqVolOpPartRate > 0 && info.DataNodeReqVolOpPartRate < minRate) {
				errout("limit rate can't be less than %d\n", minRate)
			}
			if err = client.AdminAPI().SetRateLimit(&info); err != nil {
				errout("Set rate limit fail:\n%v\n", err)
			}
			stdout("Set rate limit success.\n")
		},
	}
	cmd.Flags().StringVar(&info.Volume, "volume", "", "volume (empty volume acts as default)")
	cmd.Flags().Int8Var(&info.Opcode, "opcode", -1, "opcode (zero opcode acts as default)")
	cmd.Flags().Int64Var(&info.MetaNodeReqRate, "metaNodeReqRate", -1, "meta node request rate limit")
	cmd.Flags().Int64Var(&info.MetaNodeReqOpRate, "metaNodeReqOpRate", -1, "meta node request rate limit for opcode")
	cmd.Flags().Int64Var(&info.DataNodeReqRate, "dataNodeReqRate", -1, "data node request rate limit")
	cmd.Flags().Int64Var(&info.DataNodeReqOpRate, "dataNodeReqOpRate", -1, "data node request rate limit for opcode")
	cmd.Flags().Int64Var(&info.DataNodeReqVolPartRate, "dataNodeReqVolPartRate", -1, "data node per partition request rate limit for a given volume")
	cmd.Flags().Int64Var(&info.DataNodeReqVolOpPartRate, "dataNodeReqVolOpPartRate", -1, "data node per partition request rate limit for a given volume & opcode")
	cmd.Flags().Int64Var(&info.ClientReadRate, "clientReadRate", -1, "client read rate limit")
	cmd.Flags().Int64Var(&info.ClientWriteRate, "clientWriteRate", -1, "client write limit rate")
	return cmd
}

func formatRateLimitInfo(info *proto.LimitInfo) string {
	var sb = strings.Builder{}
	sb.WriteString(fmt.Sprintf("  Cluster name                : %v\n", info.Cluster))
	sb.WriteString(fmt.Sprintf("  MetaNodeReqRate             : %v\n", info.MetaNodeReqRateLimit))
	sb.WriteString(fmt.Sprintf("  MetaNodeReqOpRateMap        : %v\n", info.MetaNodeReqOpRateLimitMap))
	sb.WriteString(fmt.Sprintf("  (map[opcode]limit)\n"))
	sb.WriteString(fmt.Sprintf("  DataNodeReqRate             : %v\n", info.DataNodeReqRateLimit))
	sb.WriteString(fmt.Sprintf("  DataNodeReqOpRateMap        : %v\n", info.DataNodeReqOpRateLimitMap))
	sb.WriteString(fmt.Sprintf("  (map[opcode]limit)\n"))
	sb.WriteString(fmt.Sprintf("  DataNodeReqVolPartRateMap   : %v\n", info.DataNodeReqVolPartRateLimitMap))
	sb.WriteString(fmt.Sprintf("  (map[volume]limit - per partition)\n"))
	sb.WriteString(fmt.Sprintf("  DataNodeReqVolOpPartRateMap : %v\n", info.DataNodeReqVolOpPartRateLimitMap))
	sb.WriteString(fmt.Sprintf("  (map[volume]map[opcode]limit - per partition)\n"))
	sb.WriteString(fmt.Sprintf("  ClientReadRate              : %v\n", info.ClientReadRateLimit))
	sb.WriteString(fmt.Sprintf("  ClientWriteRate             : %v\n", info.ClientWriteRateLimit))
	return sb.String()
}
