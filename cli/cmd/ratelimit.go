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
	minLimitRate          = 100
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
			var info *proto.ClusterInfo
			if info, err = client.AdminAPI().GetClusterInfo(); err != nil {
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
			if (info.ClientReadLimitRate > 0 && info.ClientReadLimitRate < minLimitRate) ||
				(info.ClientWriteLimitRate > 0 && info.ClientWriteLimitRate < minLimitRate) ||
				(info.MetaNodeReqLimitRate > 0 && info.MetaNodeReqLimitRate < minLimitRate) ||
				(info.DataNodeReqLimitRate > 0 && info.DataNodeReqLimitRate < minLimitRate) ||
				(info.DataNodeReqVolPartLimitRate > 0 && info.DataNodeReqVolPartLimitRate < minLimitRate) {
				errout("limit rate can't be less than %d\n", minLimitRate)
			}
			if err = client.AdminAPI().SetRateLimit(&info); err != nil {
				errout("Set rate limit fail:\n%v\n", err)
			}
			stdout("Set rate limit success.\n")
		},
	}
	cmd.Flags().Int64Var(&info.MetaNodeReqLimitRate, "metaNodeReqRate", -1, "meta node request rate limit")
	cmd.Flags().Int64Var(&info.DataNodeReqLimitRate, "dataNodeReqRate", -1, "data node request rate limit")
	cmd.Flags().Int64Var(&info.DataNodeReqVolPartLimitRate, "dataNodeReqVolPartRate", -1, "data node per partition request rate limit for a given volume")
	cmd.Flags().StringVar(&info.Volume, "volume", "", "volume")
	cmd.Flags().Int64Var(&info.ClientReadLimitRate, "clientReadRate", -1, "client read rate limit")
	cmd.Flags().Int64Var(&info.ClientWriteLimitRate, "clientWriteRate", -1, "client write limit rate")
	return cmd
}

func formatRateLimitInfo(info *proto.ClusterInfo) string {
	var sb = strings.Builder{}
	sb.WriteString(fmt.Sprintf("  Cluster name              : %v\n", info.Cluster))
	sb.WriteString(fmt.Sprintf("  MetaNodeReqRate           : %v\n", info.MetaNodeReqLimitRate))
	sb.WriteString(fmt.Sprintf("  DataNodeReqRate           : %v\n", info.DataNodeReqLimitRate))
	sb.WriteString(fmt.Sprintf("  DataNodeReqVolPartRateMap : %v\n", info.DataNodeReqVolPartLimitRateMap))
	sb.WriteString(fmt.Sprintf("  (data node per patition request rate limit for a given volume, empty volume represent all other unspecified volumes)\n"))
	sb.WriteString(fmt.Sprintf("  ClientReadRate            : %v\n", info.ClientReadLimitRate))
	sb.WriteString(fmt.Sprintf("  ClientWriteRate           : %v\n", info.ClientWriteLimitRate))
	return sb.String()
}
