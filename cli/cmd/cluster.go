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
	"strconv"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/master"
	"github.com/spf13/cobra"
)

const (
	cmdClusterUse   = "cluster [COMMAND]"
	cmdClusterShort = "Manage cluster components"
)

func (cmd *ChubaoFSCmd) newClusterCmd(client *master.MasterClient) *cobra.Command {
	var clusterCmd = &cobra.Command{
		Use:   cmdClusterUse,
		Short: cmdClusterShort,
	}
	clusterCmd.AddCommand(
		newClusterInfoCmd(client),
		newClusterStatCmd(client),
		newClusterFreezeCmd(client),
		newClusterSetThresholdCmd(client),
	)
	return clusterCmd
}

const (
	cmdClusterInfoShort             = "Show cluster summary information"
	cmdClusterStatShort             = "Show cluster status information"
	cmdClusterFreezeShort           = "Freeze cluster"
	cmdClusterThresholdShort        = "Set memory threshold of metanodes"
	cmdClusterExtentDelRocksDbShort = "Set extent del in rocksdb enable"
)

func newClusterInfoCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpInfo,
		Short: cmdClusterInfoShort,
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var cv *proto.ClusterView
			if cv, err = client.AdminAPI().GetCluster(); err != nil {
				errout("Get cluster info fail:\n%v\n", err)
			}
			stdout("[Cluster]\n")
			stdout(formatClusterView(cv))
			stdout("\n")
		},
	}
	return cmd
}

func newClusterStatCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpStatus,
		Short: cmdClusterStatShort,
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var cs *proto.ClusterStatInfo
			if cs, err = client.AdminAPI().GetClusterStat(); err != nil {
				errout("Get cluster info fail:\n%v\n", err)
			}
			stdout("[Cluster Status]\n")
			stdout(formatClusterStat(cs))
			stdout("\n")
		},
	}
	return cmd
}

func newClusterFreezeCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:       CliOpFreeze + " [ENABLE]",
		ValidArgs: []string{"true", "false"},
		Short:     cmdClusterFreezeShort,
		Args:      cobra.MinimumNArgs(1),
		Long: `Turn on or off the automatic allocation of the data partitions. 
If 'freeze=false', ChubaoFS WILL automatically allocate new data partitions for the volume when:
  1. the used space is below the max capacity,
  2. and the number of r&w data partition is less than 20.
		
If 'freeze=true', ChubaoFS WILL NOT automatically allocate new data partitions `,
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var enable bool
			if enable, err = strconv.ParseBool(args[0]); err != nil {
				errout("Parse bool fail: %v\n", err)
			}
			if err = client.AdminAPI().IsFreezeCluster(enable); err != nil {
				errout("Failed: %v\n", err)
			}
			if enable {
				stdout("Freeze cluster successful!\n")
			} else {
				stdout("Unfreeze cluster successful!\n")
			}
		},
	}
	return cmd
}

func newClusterSetThresholdCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpSetThreshold + " [THRESHOLD]",
		Short: cmdClusterThresholdShort,
		Args:  cobra.MinimumNArgs(1),
		Long: `Set the threshold of memory on each meta node.
If the memory usage reaches this threshold, all the mata partition will be readOnly.`,
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var threshold float64
			if threshold, err = strconv.ParseFloat(args[0], 64); err != nil {
				errout("Parse Float fail: %v\n", err)
			}
			if threshold > 1.0 {
				errout("Threshold too big\n")
			}
			if err = client.AdminAPI().SetMetaNodeThreshold(threshold); err != nil {
				errout("Failed: %v\n", err)
			}
			stdout("MetaNode threshold is set to %v!\n", threshold)
		},
	}
	return cmd
}
