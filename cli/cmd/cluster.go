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
	"strconv"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/spf13/cobra"
)

const (
	cmdClusterUse   = "cluster [COMMAND]"
	cmdClusterShort = "Manage cluster components"
)

func (cmd *CubeFSCmd) newClusterCmd(client *master.MasterClient) *cobra.Command {
	var clusterCmd = &cobra.Command{
		Use:   cmdClusterUse,
		Short: cmdClusterShort,
	}
	clusterCmd.AddCommand(
		newClusterInfoCmd(client),
		newClusterStatCmd(client),
		newClusterFreezeCmd(client),
		newClusterSetThresholdCmd(client),
		newClusterSetParasCmd(client),
	)
	return clusterCmd
}

const (
	cmdClusterInfoShort           = "Show cluster summary information"
	cmdClusterStatShort           = "Show cluster status information"
	cmdClusterFreezeShort         = "Freeze cluster"
	cmdClusterThresholdShort      = "Set memory threshold of metanodes"
	cmdClusterSetClusterInfoShort = "Set cluster parameters"
	nodeDeleteBatchCountKey       = "batchCount"
	nodeMarkDeleteRateKey         = "markDeleteRate"
	nodeDeleteWorkerSleepMs       = "deleteWorkerSleepMs"
	nodeAutoRepairRateKey         = "autoRepairRate"
)

func newClusterInfoCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpInfo,
		Short: cmdClusterInfoShort,
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var cv *proto.ClusterView
			var cn *proto.ClusterNodeInfo
			var cp *proto.ClusterIP
			var delPara map[string]string
			if cv, err = client.AdminAPI().GetCluster(); err != nil {
				errout("Error: %v", err)
			}
			if cn, err = client.AdminAPI().GetClusterNodeInfo(); err != nil {
				errout("Error: %v", err)
			}
			if cp, err = client.AdminAPI().GetClusterIP(); err != nil {
				errout("Error: %v", err)
			}
			stdout("[Cluster]\n")
			stdout(formatClusterView(cv, cn, cp))
			if delPara, err = client.AdminAPI().GetDeleteParas(); err != nil {
				errout("Error: %v", err)
			}

			stdout(fmt.Sprintf("  BatchCount         : %v\n", delPara[nodeDeleteBatchCountKey]))
			stdout(fmt.Sprintf("  MarkDeleteRate     : %v\n", delPara[nodeMarkDeleteRateKey]))
			stdout(fmt.Sprintf("  DeleteWorkerSleepMs: %v\n", delPara[nodeDeleteWorkerSleepMs]))
			stdout(fmt.Sprintf("  AutoRepairRate     : %v\n", delPara[nodeAutoRepairRateKey]))
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
			var (
				err error
				cs  *proto.ClusterStatInfo
			)
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			if cs, err = client.AdminAPI().GetClusterStat(); err != nil {
				err = fmt.Errorf("Get cluster info fail:\n%v\n", err)
				return
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
If 'freeze=false', CubeFS WILL automatically allocate new data partitions for the volume when:
  1. the used space is below the max capacity,
  2. and the number of r&w data partition is less than 20.
		
If 'freeze=true', CubeFS WILL NOT automatically allocate new data partitions `,
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err    error
				enable bool
			)
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			if enable, err = strconv.ParseBool(args[0]); err != nil {
				err = fmt.Errorf("Parse bool fail: %v\n", err)
				return
			}
			if err = client.AdminAPI().IsFreezeCluster(enable); err != nil {
				return
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
			var (
				err       error
				threshold float64
			)
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			if threshold, err = strconv.ParseFloat(args[0], 64); err != nil {
				err = fmt.Errorf("Parse Float fail: %v\n", err)
				return
			}
			if threshold > 1.0 {
				err = fmt.Errorf("Threshold too big\n")
				return
			}
			if err = client.AdminAPI().SetMetaNodeThreshold(threshold); err != nil {
				return
			}
			stdout("MetaNode threshold is set to %v!\n", threshold)
		},
	}
	return cmd
}

func newClusterSetParasCmd(client *master.MasterClient) *cobra.Command {
	var optAutoRepairRate, optMarkDeleteRate, optDelBatchCount, optDelWorkerSleepMs, optLoadFactor, opMaxDpCntLimit string
	var cmd = &cobra.Command{
		Use:   CliOpSetCluster,
		Short: cmdClusterSetClusterInfoShort,
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err error
			)
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()

			if err = client.AdminAPI().SetClusterParas(optDelBatchCount, optMarkDeleteRate, optDelWorkerSleepMs, optAutoRepairRate, optLoadFactor, opMaxDpCntLimit); err != nil {
				return
			}
			stdout("Cluster parameters has been set successfully. \n")
		},
	}
	cmd.Flags().StringVar(&optDelBatchCount, CliFlagDelBatchCount, "", "MetaNode delete batch count")
	cmd.Flags().StringVar(&optLoadFactor, CliFlagLoadFactor, "", "Load Factor")
	cmd.Flags().StringVar(&optMarkDeleteRate, CliFlagMarkDelRate, "", "DataNode batch mark delete limit rate. if 0 for no infinity limit")
	cmd.Flags().StringVar(&optAutoRepairRate, CliFlagAutoRepairRate, "", "DataNode auto repair rate")
	cmd.Flags().StringVar(&optDelWorkerSleepMs, CliFlagDelWorkerSleepMs, "", "MetaNode delete worker sleep time with millisecond. if 0 for no sleep")
	cmd.Flags().StringVar(&opMaxDpCntLimit, CliFlagMaxDpCntLimit, "", "Maximum number of dp on each datanode, default 3000, 0 represents setting to default")

	return cmd
}
