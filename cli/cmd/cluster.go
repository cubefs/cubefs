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
	"strings"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/spf13/cobra"
)

const (
	cmdClusterUse   = "cluster [COMMAND]"
	cmdClusterShort = "Manage cluster components"
)

func newClusterCmd(client *master.MasterClient) *cobra.Command {
	clusterCmd := &cobra.Command{
		Use:   cmdClusterUse,
		Short: cmdClusterShort,
	}
	clusterCmd.AddCommand(
		newClusterInfoCmd(client),
		newClusterStatCmd(client),
		newClusterFreezeCmd(client),
		newClusterSetThresholdCmd(client),
		newClusterSetParasCmd(client),
		newClusterDisableMpDecommissionCmd(client),
		newClusterSetVolDeletionDelayTimeCmd(client),
		newClusterQueryDecommissionStatusCmd(client),
		// newClusterSetDecommissionLimitCmd(client),
		newClusterQueryDecommissionFailedDiskCmd(client),
		// newClusterSetDecommissionDiskLimitCmd(client),
		newClusterQueryDataNodeOpCmd(client),
		newClusterQueryDpOpCmd(client),
		newClusterQueryDiskOpCmd(client),
		newClusterChangeMasterLeaderCmd(client),
	)
	return clusterCmd
}

const (
	cmdClusterInfoShort                    = "Show cluster summary information"
	cmdClusterStatShort                    = "Show cluster status information"
	cmdClusterFreezeShort                  = "Freeze cluster"
	cmdClusterThresholdShort               = "Set memory threshold of metanodes"
	cmdClusterSetClusterInfoShort          = "Set cluster parameters"
	cmdClusterSetVolDeletionDelayTimeShort = "Set volDeletionDelayTime of master"
	nodeDeleteBatchCountKey                = "batchCount"
	nodeMarkDeleteRateKey                  = "markDeleteRate"
	nodeDeleteWorkerSleepMs                = "deleteWorkerSleepMs"
	nodeAutoRepairRateKey                  = "autoRepairRate"
	nodeMaxDpCntLimit                      = "maxDpCntLimit"
	nodeMaxMpCntLimit                      = "maxMpCntLimit"
	cmdForbidMpDecommission                = "forbid meta partition decommission"
	// cmdSetDecommissionLimitShort           = "set cluster decommission limit"
	cmdQueryDecommissionStatus = "query decommission status"
	// cmdEnableAutoDecommissionDiskShort  = "enable auto decommission disk"
	cmdQueryDecommissionFailedDiskShort = "query auto or manual decommission failed disk"
	// cmdSetDecommissionDiskLimit            = "set decommission disk limit"
	cmdQueryDataNodeOpShort = "query DataNode_op information of a cluster"
	cmdQueryDpOpShort       = "query Dp_op information of a cluster"
	cmdQueryDiskOpShort     = "query Disk_op information of a cluster"
)

func newClusterInfoCmd(client *master.MasterClient) *cobra.Command {
	var volStorageClass bool
	cmd := &cobra.Command{
		Use:   CliOpInfo,
		Short: cmdClusterInfoShort,
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var cv *proto.ClusterView
			var cn *proto.ClusterNodeInfo
			var cp *proto.ClusterIP
			var clusterPara map[string]string
			if cv, err = client.AdminAPI().GetCluster(volStorageClass); err != nil {
				errout(err)
			}
			if cn, err = client.AdminAPI().GetClusterNodeInfo(); err != nil {
				errout(err)
			}
			if cp, err = client.AdminAPI().GetClusterIP(); err != nil {
				errout(err)
			}
			stdout("[Cluster]\n")
			stdout("%v", formatClusterView(cv, cn, cp))
			if clusterPara, err = client.AdminAPI().GetClusterParas(); err != nil {
				errout(err)
			}

			stdout(fmt.Sprintf("  BatchCount         : %v\n", clusterPara[nodeDeleteBatchCountKey]))
			stdout(fmt.Sprintf("  MarkDeleteRate     : %v\n", clusterPara[nodeMarkDeleteRateKey]))
			stdout(fmt.Sprintf("  DeleteWorkerSleepMs: %v\n", clusterPara[nodeDeleteWorkerSleepMs]))
			stdout(fmt.Sprintf("  AutoRepairRate     : %v\n", clusterPara[nodeAutoRepairRateKey]))
			stdout(fmt.Sprintf("  MaxDpCntLimit      : %v\n", clusterPara[nodeMaxDpCntLimit]))
			stdout(fmt.Sprintf("  MaxMpCntLimit      : %v\n", clusterPara[nodeMaxMpCntLimit]))
			stdout("\n")

			if volStorageClass {
				stdout("Usage by storage class:\n")
				stdout("%v\n", hybridCloudStorageTableHeader)
				for _, view := range cv.StatOfStorageClass {
					stdout("%v\n", formatHybridCloudStorageTableRow(view))
				}

				stdout("\nMigration Usage by storage class:\n")
				stdout("%v\n", hybridCloudStorageTableHeader)
				for _, view := range cv.StatMigrateStorageClass {
					stdout("%v\n", formatHybridCloudStorageTableRow(view))
				}
			}
		},
	}
	cmd.Flags().BoolVarP(&volStorageClass, "storage-class", "s", false, "Display hybrid cloud storage info")
	return cmd
}

func newClusterStatCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   CliOpStatus,
		Short: cmdClusterStatShort,
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err error
				cs  *proto.ClusterStatInfo
			)
			defer func() {
				if err != nil {
					errout(err)
				}
			}()
			if cs, err = client.AdminAPI().GetClusterStat(); err != nil {
				err = fmt.Errorf("Get cluster info fail:\n%v\n", err)
				return
			}
			stdout("[Cluster Status]\n")
			stdout("%v", formatClusterStat(cs))
			stdout("\n")
		},
	}
	return cmd
}

func newClusterFreezeCmd(client *master.MasterClient) *cobra.Command {
	var clientIDKey string
	cmd := &cobra.Command{
		Use:       CliOpFreeze + " [ENABLE]",
		ValidArgs: []string{"true", "false"},
		Short:     cmdClusterFreezeShort,
		Args:      cobra.MinimumNArgs(1),
		Long: `Turn on or off the automatic allocation of the data partitions.
If 'cluster freeze false', CubeFS WILL automatically allocate new data partitions for the volume when:
  1. the used space is below the max capacity,
  2. and the number of r&w data partition is less than 20.

If 'cluster freeze true', CubeFS WILL NOT automatically allocate new data partitions `,
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err    error
				enable bool
			)
			defer func() {
				errout(err)
			}()
			if enable, err = strconv.ParseBool(args[0]); err != nil {
				err = fmt.Errorf("Parse bool fail: %v\n", err)
				return
			}
			if err = client.AdminAPI().IsFreezeCluster(enable, clientIDKey); err != nil {
				return
			}
			if enable {
				stdout("Freeze cluster successful!\n")
			} else {
				stdout("Unfreeze cluster successful!\n")
			}
		},
	}
	cmd.Flags().StringVar(&clientIDKey, CliFlagClientIDKey, client.ClientIDKey(), CliUsageClientIDKey)
	return cmd
}

func newClusterSetThresholdCmd(client *master.MasterClient) *cobra.Command {
	var clientIDKey string
	cmd := &cobra.Command{
		Use:   CliOpSetThreshold + " [THRESHOLD]",
		Short: cmdClusterThresholdShort,
		Args:  cobra.MinimumNArgs(1),
		Long: `Set the threshold of memory on each meta node.
If the memory usage reaches this threshold, all the meta partition will be readOnly.`,
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err       error
				threshold float64
			)
			defer func() {
				errout(err)
			}()
			if threshold, err = strconv.ParseFloat(args[0], 64); err != nil {
				err = fmt.Errorf("Parse Float fail: %v\n", err)
				return
			}
			if threshold > 1.0 {
				err = fmt.Errorf("Threshold too big\n")
				return
			}
			if err = client.AdminAPI().SetMetaNodeThreshold(threshold, clientIDKey); err != nil {
				return
			}
			stdout("MetaNode threshold is set to %v!\n", threshold)
		},
	}
	cmd.Flags().StringVar(&clientIDKey, CliFlagClientIDKey, client.ClientIDKey(), CliUsageClientIDKey)
	return cmd
}

func newClusterSetVolDeletionDelayTimeCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   CliOpSetVolDeletionDelayTime + " [VOLDELETIONDELAYTIME]",
		Short: cmdClusterSetVolDeletionDelayTimeShort,
		Args:  cobra.MinimumNArgs(1),
		Long:  `Set the volDeletionDelayTime of master on each master.`,
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err                      error
				volDeletionDelayTimeHour int
			)
			defer func() {
				if err != nil {
					errout(err)
				}
			}()
			if volDeletionDelayTimeHour, err = strconv.Atoi(args[0]); err != nil {
				err = fmt.Errorf("Parse int fail: %v\n", err)
				return
			}
			if volDeletionDelayTimeHour <= 0 {
				err = fmt.Errorf("volDeletionDelayTime is less than or equal to 0\n")
				return
			}
			if err = client.AdminAPI().SetMasterVolDeletionDelayTime(volDeletionDelayTimeHour); err != nil {
				return
			}
			stdout("master volDeletionDelayTime is set to %v h!\n", volDeletionDelayTimeHour)
		},
	}
	return cmd
}

func newClusterSetParasCmd(client *master.MasterClient) *cobra.Command {
	var clientIDKey string
	var optAutoRepairRate, optMarkDeleteRate, optDelBatchCount, optDelWorkerSleepMs, optLoadFactor string
	// dataNodesetSelector := ""
	// metaNodesetSelector := ""
	// dataNodeSelector := ""
	// metaNodeSelector := ""
	// markBrokenDiskThreshold := ""
	autoDecommissionDisk := ""
	autoDecommissionDiskInterval := ""
	autoDpMetaRepair := ""
	autoDpMetaRepairParallelCnt := ""
	opMaxDpCntLimit := ""
	opMaxMpCntLimit := ""
	dpRepairTimeout := ""
	dpTimeout := ""
	dpBackupTimeout := ""
	decommissionDpLimit := ""
	decommissionDiskLimit := ""
	forbidWriteOpOfProtoVersion0 := ""
	dataMediaType := ""
	cmd := &cobra.Command{
		Use:   CliOpSetCluster,
		Short: cmdClusterSetClusterInfoShort,
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				errout(err)
			}()

			// if markBrokenDiskThreshold != "" {
			//	val, err := strutil.ParsePercent(markBrokenDiskThreshold)
			//	if err != nil {
			//		return
			//	}
			//	markBrokenDiskThreshold = fmt.Sprintf("%v", val)
			// }

			if autoDecommissionDisk != "" {
				if _, err = strconv.ParseBool(autoDecommissionDisk); err != nil {
					return
				}
			}
			if autoDecommissionDiskInterval != "" {
				var interval time.Duration
				interval, err = time.ParseDuration(autoDecommissionDiskInterval)
				if err != nil {
					return
				}
				if interval < time.Second {
					err = fmt.Errorf("auto decommission disk interval %v smaller than 1s", interval)
					return
				}

				autoDecommissionDiskInterval = strconv.FormatInt(int64(interval), 10)
			}

			if autoDpMetaRepair != "" {
				if _, err = strconv.ParseBool(autoDpMetaRepair); err != nil {
					return
				}
			}
			if autoDpMetaRepairParallelCnt != "" {
				if _, err = strconv.ParseInt(autoDpMetaRepairParallelCnt, 10, 64); err != nil {
					return
				}
			}

			if dpRepairTimeout != "" {
				var repairTimeout time.Duration
				repairTimeout, err = time.ParseDuration(dpRepairTimeout)
				if err != nil {
					return
				}
				if repairTimeout < time.Second {
					err = fmt.Errorf("dp repair timeout %v smaller than 1s", repairTimeout)
					return
				}

				dpRepairTimeout = strconv.FormatInt(int64(repairTimeout), 10)
			}
			if dpTimeout != "" {
				var heartbeatTimeout time.Duration
				heartbeatTimeout, err = time.ParseDuration(dpTimeout)
				if err != nil {
					return
				}
				if heartbeatTimeout < time.Second {
					err = fmt.Errorf("dp timeout %v smaller than 1s", heartbeatTimeout)
					return
				}

				dpTimeout = strconv.FormatInt(int64(heartbeatTimeout.Seconds()), 10)
			}
			if dpBackupTimeout != "" {
				var backupTimeout time.Duration
				backupTimeout, err = time.ParseDuration(dpBackupTimeout)
				if err != nil {
					return
				}
				if backupTimeout < proto.DefaultDataPartitionBackupTimeOut {
					err = fmt.Errorf("dp backup timeout %v smaller than %v", backupTimeout, proto.DefaultDataPartitionBackupTimeOut)
					return
				}

				dpBackupTimeout = strconv.FormatInt(int64(backupTimeout), 10)
			}

			if forbidWriteOpOfProtoVersion0 != "" {
				if _, err = strconv.ParseBool(forbidWriteOpOfProtoVersion0); err != nil {
					err = fmt.Errorf("param forbidWriteOpOfProtoVersion0(%v) should be true or false", forbidWriteOpOfProtoVersion0)
					return
				}
			}

			if dataMediaType != "" {
				if _, err = strconv.ParseInt(dataMediaType, 10, 64); err != nil {
					err = fmt.Errorf("param (%v) failed, should be int", dataMediaType)
					return
				}
			}

			if err = client.AdminAPI().SetClusterParas(optDelBatchCount, optMarkDeleteRate, optDelWorkerSleepMs,
				optAutoRepairRate, optLoadFactor, opMaxDpCntLimit, opMaxMpCntLimit, clientIDKey,
				autoDecommissionDisk, autoDecommissionDiskInterval,
				autoDpMetaRepair, autoDpMetaRepairParallelCnt,
				dpRepairTimeout, dpTimeout, dpBackupTimeout, decommissionDpLimit, decommissionDiskLimit,
				forbidWriteOpOfProtoVersion0, dataMediaType); err != nil {
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
	cmd.Flags().StringVar(&opMaxMpCntLimit, CliFlagMaxMpCntLimit, "", "Maximum number of mp on each metanode, default 300, 0 represents setting to default")
	cmd.Flags().StringVar(&clientIDKey, CliFlagClientIDKey, client.ClientIDKey(), CliUsageClientIDKey)
	// cmd.Flags().StringVar(&dataNodesetSelector, CliFlagDataNodesetSelector, "", "Set the nodeset select policy(datanode) for cluster")
	// cmd.Flags().StringVar(&metaNodesetSelector, CliFlagMetaNodesetSelector, "", "Set the nodeset select policy(metanode) for cluster")
	// cmd.Flags().StringVar(&dataNodeSelector, CliFlagDataNodeSelector, "", "Set the node select policy(datanode) for cluster")
	// cmd.Flags().StringVar(&metaNodeSelector, CliFlagMetaNodeSelector, "", "Set the node select policy(metanode) for cluster")
	// cmd.Flags().StringVar(&markBrokenDiskThreshold, CliFlagMarkDiskBrokenThreshold, "", "Threshold to mark disk as broken")
	cmd.Flags().StringVar(&autoDpMetaRepair, CliFlagAutoDpMetaRepair, "", "Enable or disable auto data partition meta repair")
	cmd.Flags().StringVar(&autoDpMetaRepairParallelCnt, CliFlagAutoDpMetaRepairParallelCnt, "", "Parallel count of auto data partition meta repair")
	cmd.Flags().StringVar(&dpRepairTimeout, CliFlagDpRepairTimeout, "", "Data partition repair timeout(example: 1h)")
	cmd.Flags().StringVar(&dpTimeout, CliFlagDpTimeout, "", "Data partition heartbeat timeout(example: 10s)")
	cmd.Flags().StringVar(&autoDecommissionDisk, CliFlagAutoDecommissionDisk, "", "Enable or disable auto decommission disk")
	cmd.Flags().StringVar(&autoDecommissionDiskInterval, CliFlagAutoDecommissionDiskInterval, "", "Interval of auto decommission disk(example: 10s)")
	cmd.Flags().StringVar(&dpBackupTimeout, CliFlagDpBackupTimeout, "", "Data partition backup directory timeout(example: 1h)")
	cmd.Flags().StringVar(&decommissionDpLimit, CliFlagDecommissionDpLimit, "", "Limit for parallel  decommission dp")
	cmd.Flags().StringVar(&decommissionDiskLimit, CliFlagDecommissionDiskLimit, "", "Limit for parallel decommission disk")
	cmd.Flags().StringVar(&forbidWriteOpOfProtoVersion0, CliForbidWriteOpOfProtoVersion0, "",
		"set datanode and metanode whether forbid write operate of packet whose protocol version is version-0: [true | false]")
	cmd.Flags().StringVar(&dataMediaType, "clusterDataMediaType", "", "set cluster media type, 1(ssd), 2(hdd)")
	return cmd
}

func newClusterDisableMpDecommissionCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:       CliOpForbidMpDecommission + " [true|false]",
		ValidArgs: []string{"true", "false"},
		Short:     cmdForbidMpDecommission,
		Args:      cobra.MinimumNArgs(1),
		Long: `Forbid or allow MetaPartition decommission in the cluster.
the forbid flag is false by default when cluster created
If 'forbid=false', MetaPartition decommission/migrate and MetaNode decommission is allowed.
If 'forbid=true', MetaPartition decommission/migrate and MetaNode decommission is forbidden.`,
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err    error
				forbid bool
			)
			defer func() {
				errout(err)
			}()
			if forbid, err = strconv.ParseBool(args[0]); err != nil {
				err = fmt.Errorf("Parse bool fail: %v\n", err)
				return
			}
			if err = client.AdminAPI().SetForbidMpDecommission(forbid); err != nil {
				return
			}
			if forbid {
				stdout("Forbid MetaPartition decommission successful!\n")
			} else {
				stdout("Allow MetaPartition decommission successful!\n")
			}
		},
	}
	return cmd
}

// func newClusterSetDecommissionLimitCmd(client *master.MasterClient) *cobra.Command {
//	cmd := &cobra.Command{
//		Use:   CliOpSetDecommissionLimit + " [LIMIT]",
//		Short: cmdSetDecommissionLimitShort,
//		Args:  cobra.MinimumNArgs(1),
//		Run: func(cmd *cobra.Command, args []string) {
//			var err error
//			defer func() {
//				if err != nil {
//					errout(err)
//				}
//			}()
//			limit, err := strconv.ParseInt(args[0], 10, 32)
//			if err = client.AdminAPI().SetClusterDecommissionLimit(int32(limit)); err != nil {
//				return
//			}
//
//			stdout("Set decommission limit to %v successfully\n", limit)
//		},
//	}
//	return cmd
// }

func newClusterQueryDecommissionStatusCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   CliOpQueryDecommissionStatus,
		Short: cmdQueryDecommissionStatus,
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var status []proto.DecommissionTokenStatus
			defer func() {
				if err != nil {
					errout(err)
				}
			}()
			if status, err = client.AdminAPI().QueryDecommissionToken(); err != nil {
				return
			}

			for _, s := range status {
				stdout("%v\n", formatDecommissionTokenStatus(&s))
			}
		},
	}
	return cmd
}

func newClusterQueryDecommissionFailedDiskCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   CliOpQueryDecommissionFailedDisk + " [TYPE]",
		Short: cmdQueryDecommissionFailedDiskShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err        error
				decommType int
			)

			defer func() {
				errout(err)
			}()

			args[0] = strings.ToLower(args[0])
			switch args[0] {
			case "manual":
				decommType = 0
			case "auto":
				decommType = 1
			case "all":
				decommType = 2
			default:
				err = fmt.Errorf("unknown decommission type %v, not \"auto\", \"manual\" and \"and\"", args[0])
				return
			}

			diskInfo, err := client.AdminAPI().QueryDecommissionFailedDisk(decommType)
			if err != nil {
				return
			}

			stdout("FailedDisks:\n")
			for i, d := range diskInfo {
				stdout("[%v/%v]\n%v", i+1, len(diskInfo), formatDecommissionFailedDiskInfo(d))
			}
		},
	}
	return cmd
}

func newClusterQueryDataNodeOpCmd(client *master.MasterClient) *cobra.Command {
	var (
		filterOp     string
		dataNodeName string
		logNum       int
		dimension    string
		volName      string
		addr         string
		dpId         string
		diskName     string
	)
	cmd := &cobra.Command{
		Use:   CliOpDataNodeOp,
		Short: cmdQueryDataNodeOpShort,
		RunE: func(cmd *cobra.Command, args []string) error {
			dimension = proto.Node
			opv, err := client.AdminAPI().GetOpLog(dimension, volName, addr, dpId, diskName)
			if err != nil {
				return err
			}
			stdoutln(fmt.Sprintf("%-30v %-20v %v", "Ip", "OpType", "Count"))
			stdoutln(formatDataNodeOp(opv, logNum, dataNodeName, filterOp))
			return nil
		},
	}
	cmd.Flags().IntVar(&logNum, "num", 50, "Number of logs to display")
	cmd.Flags().StringVar(&dataNodeName, "dataNode", "", "Filter logs by dataNode name")
	cmd.Flags().StringVar(&filterOp, "filter-op", "", "Filter logs by op type")
	cmd.Flags().StringVar(&addr, "addr", "", "Filter logs by data node address")
	return cmd
}

func newClusterQueryDpOpCmd(client *master.MasterClient) *cobra.Command {
	var (
		filterOp  string
		dpId      string
		logNum    int
		dimension string
		volName   string
		addr      string
		diskName  string
	)
	cmd := &cobra.Command{
		Use:   CliOpDpOp,
		Short: cmdQueryDpOpShort,
		RunE: func(cmd *cobra.Command, args []string) error {
			dimension = proto.Dp
			opv, err := client.AdminAPI().GetOpLog(dimension, volName, addr, dpId, diskName)
			if err != nil {
				return err
			}
			stdoutln(fmt.Sprintf("%-30v %-20v %v", "DpId", "OpType", "Count"))
			stdoutln(formatClusterDpOp(opv, logNum, filterOp))
			return nil
		},
	}
	cmd.Flags().IntVar(&logNum, "num", 50, "Number of logs to display")
	cmd.Flags().StringVar(&dpId, "dp", "", "Filter logs by dp id")
	cmd.Flags().StringVar(&filterOp, "filter-op", "", "Filter logs by op type")
	cmd.Flags().StringVar(&addr, "addr", "", "Filter logs by data node address")
	return cmd
}

func newClusterQueryDiskOpCmd(client *master.MasterClient) *cobra.Command {
	var (
		filterOp  string
		diskName  string
		logNum    int
		dimension string
		volName   string
		addr      string
		dpId      string
	)
	cmd := &cobra.Command{
		Use:   CliOpDiskOp,
		Short: cmdQueryDiskOpShort,
		RunE: func(cmd *cobra.Command, args []string) error {
			dimension = proto.Disk
			opv, err := client.AdminAPI().GetOpLog(dimension, volName, addr, dpId, diskName)
			if err != nil {
				return err
			}
			if diskName == "" {
				stdoutln(fmt.Sprintf("%-45v %-20v %v", "DiskName", "OpType", "Count"))
			} else {
				stdoutln(fmt.Sprintf("%-45v %-20v %v", "DpId", "OpType", "Count"))
			}
			stdoutln(formatClusterDiskOp(opv, logNum, filterOp))
			return nil
		},
	}
	cmd.Flags().IntVar(&logNum, "num", 50, "Number of logs to display")
	cmd.Flags().StringVar(&diskName, "disk", "", "Filter logs by disk name")
	cmd.Flags().StringVar(&filterOp, "filter-op", "", "Filter logs by op type")
	cmd.Flags().StringVar(&addr, "addr", "", "Filter logs by data node address")
	return cmd
}

// func newClusterSetDecommissionDiskLimitCmd(client *master.MasterClient) *cobra.Command {
//	cmd := &cobra.Command{
//		Use:   CliOpSetDecommissionDiskLimit + " [LIMIT]",
//		Short: cmdSetDecommissionDiskLimit,
//		Args:  cobra.MinimumNArgs(1),
//		Run: func(cmd *cobra.Command, args []string) {
//			var (
//				err   error
//				limit uint32
//			)
//
//			defer func() {
//				errout(err)
//			}()
//
//			tmp, err := strconv.ParseUint(args[0], 10, 32)
//			if err != nil {
//				return
//			}
//			limit = uint32(tmp)
//
//			err = client.AdminAPI().SetDecommissionDiskLimit(limit)
//			if err != nil {
//				return
//			}
//			stdout("Set decommission disk limit to %v successfully\n", limit)
//		},
//	}
//	return cmd
// }

func newClusterChangeMasterLeaderCmd(client *master.MasterClient) *cobra.Command {
	var leaderAddr string
	cmd := &cobra.Command{
		Use:   CliOpToLeader,
		Short: "Change master leader",
		Run: func(cmd *cobra.Command, args []string) {
			if err := client.AdminAPI().ChangeMasterLeader(leaderAddr); err != nil {
				errout(err)
				return
			}
			stdout("Change master leader successfully, but need to check\n")
		},
	}
	cmd.Flags().StringVar(&leaderAddr, CliFlagAddress, "", "The address of the new master leader")
	return cmd
}
