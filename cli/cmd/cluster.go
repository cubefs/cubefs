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
		newClusterQueryDistributionOptimizationStatusCmd(client),
		newClusterQueryDpDecommissionStatusCmd(client),
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
	dpLimitSsdBaseCountKey                 = "dpLimitSsdBaseCount"
	dpLimitSsdFactorKey                    = "dpLimitSsdFactor"
	dpLimitHddBaseCountKey                 = "dpLimitHddBaseCount"
	dpLimitHddFactorKey                    = "dpLimitHddFactor"
	cmdForbidMpDecommission                = "forbid meta partition decommission"
	// cmdSetDecommissionLimitShort           = "set cluster decommission limit"
	cmdQueryDecommissionStatus = "query decommission status"
	// cmdEnableAutoDecommissionDiskShort  = "enable auto decommission disk"
	cmdQueryDecommissionFailedDiskShort = "query auto or manual decommission failed disk"
	// cmdSetDecommissionDiskLimit            = "set decommission disk limit"
	cmdQueryDataNodeOpShort                      = "query DataNode_op information of a cluster"
	cmdQueryDpOpShort                            = "query Dp_op information of a cluster"
	cmdQueryDiskOpShort                          = "query Disk_op information of a cluster"
	cmdQueryClusterDistributionOptimizationShort = "query distribution optimization status"
	cmdQueryDpDecommissionStatusShort            = "query data partition decommission status by type"
)

func newClusterInfoCmd(client *master.MasterClient) *cobra.Command {
	var volStorageClass bool
	var volPool bool
	cmd := &cobra.Command{
		Use:   CliOpInfo,
		Short: cmdClusterInfoShort,
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var cv *proto.ClusterView
			var cn *proto.ClusterNodeInfo
			var cp *proto.ClusterIP
			var clusterPara map[string]string
			if cv, err = client.AdminAPI().GetClusterWithPool(volStorageClass, volPool); err != nil {
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

			// Display default pool information
			var pools []*proto.StoragePoolInfo
			if pools, err = client.AdminAPI().ListStoragePools(); err == nil {
				for _, pool := range pools {
					if pool.Id == cv.DefaultPoolId {
						stdout(fmt.Sprintf("  DefaultPoolId       : %v\n", cv.DefaultPoolId))
						stdout(fmt.Sprintf("  DefaultPoolName     : %v\n", pool.Name))
						stdout(fmt.Sprintf("  DefaultPoolClass    : %v\n", proto.StorageClassString(uint32(pool.StorageClass))))
						break
					}
				}
			} else {
				// If failed to get pools, just show the ID
				stdout(fmt.Sprintf("  DefaultPoolId       : %v\n", cv.DefaultPoolId))
			}

			if clusterPara, err = client.AdminAPI().GetClusterParas(); err != nil {
				errout(err)
			}

			stdout(fmt.Sprintf("  BatchCount         : %v\n", clusterPara[nodeDeleteBatchCountKey]))
			stdout(fmt.Sprintf("  MarkDeleteRate     : %v\n", clusterPara[nodeMarkDeleteRateKey]))
			stdout(fmt.Sprintf("  DeleteWorkerSleepMs: %v\n", clusterPara[nodeDeleteWorkerSleepMs]))
			stdout(fmt.Sprintf("  AutoRepairRate     : %v\n", clusterPara[nodeAutoRepairRateKey]))
			stdout(fmt.Sprintf("  MaxDpCntLimit      : %v\n", clusterPara[nodeMaxDpCntLimit]))
			stdout(fmt.Sprintf("  MaxMpCntLimit      : %v\n", clusterPara[nodeMaxMpCntLimit]))
			stdout(fmt.Sprintf("  DpLimitSsdBaseCount: %v\n", clusterPara[dpLimitSsdBaseCountKey]))
			stdout(fmt.Sprintf("  DpLimitSsdFactor   : %v\n", clusterPara[dpLimitSsdFactorKey]))
			stdout(fmt.Sprintf("  DpLimitHddBaseCount: %v\n", clusterPara[dpLimitHddBaseCountKey]))
			stdout(fmt.Sprintf("  DpLimitHddFactor   : %v\n", clusterPara[dpLimitHddFactorKey]))

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

			if volPool {
				// Get pool name map for display
				var pools []*proto.StoragePoolInfo
				poolNameMap := make(map[uint8]string)
				if pools, err = client.AdminAPI().ListStoragePools(); err == nil {
					for _, pool := range pools {
						poolNameMap[pool.Id] = pool.Name
					}
				}

				stdout("Usage by pool:\n")
				stdout("%v\n", hybridCloudPoolTableHeader)
				for _, view := range cv.StatOfPool {
					stdout("%v\n", formatHybridCloudPoolTableRow(view, poolNameMap))
				}

				stdout("\nMigration Usage by pool:\n")
				stdout("%v\n", hybridCloudPoolTableHeader)
				for _, view := range cv.StatMigratePool {
					stdout("%v\n", formatHybridCloudPoolTableRow(view, poolNameMap))
				}
			}
		},
	}
	cmd.Flags().BoolVarP(&volStorageClass, "storage-class", "s", false, "Display hybrid cloud storage info")
	cmd.Flags().BoolVarP(&volPool, "pool", "p", false, "Display cluster usage statistics by pool")
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
	var tmp int64
	// dataNodesetSelector := ""
	// metaNodesetSelector := ""
	// dataNodeSelector := ""
	// metaNodeSelector := ""
	// markBrokenDiskThreshold := ""
	autoDecommissionDisk := ""
	autoDecommissionDiskInterval := ""
	autoDpMetaRepair := ""
	autoDpMetaRepairParallelCnt := ""
	autoMpMetaRepair := ""
	autoMpMetaRepairParallelCnt := ""
	autoDistributionOptimization := ""
	enableMpDecommissionByLearner := ""
	opMaxDpCntLimit := ""
	opMaxMpCntLimit := ""
	dpRepairTimeout := ""
	dpTimeout := ""
	mpTimeout := ""
	dpBackupTimeout := ""
	decommissionDpLimit := ""
	decommissionDiskLimit := ""
	decommissionFirstHostDiskParallelLimit := ""
	metaAutoAddReplicaLimit := ""
	metaManualDecommissionLimit := ""
	metaBalanceLimit := ""
	metaManualAddReplicaLimit := ""
	forbidWriteOpOfProtoVersion0 := ""
	dataMediaType := ""
	handleTimeout := ""
	readDataNodeTimeout := ""
	rackAware := ""
	learnerRecoverTimeoutSeconds := ""
	// Distribution optimization parameters
	distributionOptimizationConDpCnt := ""
	distributionOptimizationThreshold := ""

	optRcTTL := ""
	optRcReadTimeout := ""
	optFlashHotKeyMissCount := ""
	optRemoteCacheMultiRead := ""
	optFlashNodeTimeoutCount := ""
	optRemoteCacheSameZoneTimeout := ""
	optRemoteCacheSameRegionTimeout := ""
	optFlashReadFlowLimit := ""
	optFlashWriteFlowLimit := ""
	optFlashKeyFlowLimit := ""
	optRemoteClientFlowLimit := ""
	optDpLimitSsdBaseCount := ""
	optDpLimitSsdFactor := ""
	optDpLimitHddBaseCount := ""
	optDpLimitHddFactor := ""
	optAutoFixTag := ""
	optDefaultDpTag := ""
	optDefaultMpTag := ""
	optDefaultPoolId := ""
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
				var parsed int64
				if parsed, err = strconv.ParseInt(autoDpMetaRepairParallelCnt, 10, 64); err != nil {
					return
				}
				if parsed < int64(proto.MinAutoDpMetaRepairParallelCnt) || parsed > int64(proto.MaxAutoDpMetaRepairParallelCnt) {
					err = fmt.Errorf("autoDpMetaRepairParallelCnt must be between %d and %d",
						proto.MinAutoDpMetaRepairParallelCnt, proto.MaxAutoDpMetaRepairParallelCnt)
					return
				}
			}

			if autoMpMetaRepair != "" {
				if _, err = strconv.ParseBool(autoMpMetaRepair); err != nil {
					return
				}
			}
			if autoMpMetaRepairParallelCnt != "" {
				var parsed int64
				if parsed, err = strconv.ParseInt(autoMpMetaRepairParallelCnt, 10, 64); err != nil {
					return
				}
				if parsed < int64(proto.MinAutoMpMetaRepairParallelCnt) || parsed > int64(proto.MaxAutoMpMetaRepairParallelCnt) {
					err = fmt.Errorf("autoMpMetaRepairParallelCnt must be between %d and %d",
						proto.MinAutoMpMetaRepairParallelCnt, proto.MaxAutoMpMetaRepairParallelCnt)
					return
				}
			}

			if autoDistributionOptimization != "" {
				if _, err = strconv.ParseBool(autoDistributionOptimization); err != nil {
					return
				}
			}

			if enableMpDecommissionByLearner != "" {
				if _, err = strconv.ParseBool(enableMpDecommissionByLearner); err != nil {
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
			if mpTimeout != "" {
				var mpHeartbeatTimeout time.Duration
				mpHeartbeatTimeout, err = time.ParseDuration(mpTimeout)
				if err != nil {
					return
				}
				if mpHeartbeatTimeout < time.Second {
					err = fmt.Errorf("dp timeout %v smaller than 1s", mpHeartbeatTimeout)
					return
				}

				mpTimeout = strconv.FormatInt(int64(mpHeartbeatTimeout.Seconds()), 10)
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
			if handleTimeout != "" {
				if tmp, err = strconv.ParseInt(handleTimeout, 10, 64); err != nil {
					err = fmt.Errorf("param (%v) failed, should be int", handleTimeout)
					return
				}
				if tmp <= 0 {
					err = fmt.Errorf("handleTimeout (%v) should grater than 0", handleTimeout)
					return
				}
			}
			if readDataNodeTimeout != "" {
				if tmp, err = strconv.ParseInt(readDataNodeTimeout, 10, 64); err != nil {
					err = fmt.Errorf("param (%v) failed, should be int", readDataNodeTimeout)
					return
				}
				if tmp <= 0 {
					err = fmt.Errorf("readDataNodeTimeout (%v) should grater than 0", readDataNodeTimeout)
					return
				}
			}

			if rackAware != "" {
				var rackAwareLevel int64
				rackAwareLevel, err = strconv.ParseInt(rackAware, 10, 64)
				if err != nil {
					err = fmt.Errorf("param rackAware(%v) should be int", rackAware)
					return
				}
				if rackAwareLevel < 0 || rackAwareLevel > 2 {
					err = fmt.Errorf("rackAware (%v) should be 0(nolimit), 1(weak), or 2(strong)", rackAware)
					return
				}
			}

			if distributionOptimizationConDpCnt != "" {
				var count int64
				count, err = strconv.ParseInt(distributionOptimizationConDpCnt, 10, 64)
				if err != nil {
					err = fmt.Errorf("param distributionOptimizationConcurrentDpCount(%v) should be int", distributionOptimizationConDpCnt)
					return
				}
				if count <= 0 {
					err = fmt.Errorf("distributionOptimizationConcurrentDpCount (%v) should be greater than 0", count)
					return
				}
			}

			if distributionOptimizationThreshold != "" {
				var threshold float64
				threshold, err = strconv.ParseFloat(distributionOptimizationThreshold, 64)
				if err != nil {
					err = fmt.Errorf("param distributionOptimizationThreshold(%v) should be float", distributionOptimizationThreshold)
					return
				}
				if threshold < 0 || threshold > 1 {
					err = fmt.Errorf("distributionOptimizationThreshold (%v) should be between 0 and 1", threshold)
					return
				}
			}

			if optRcTTL != "" {
				if tmp, err = strconv.ParseInt(optRcTTL, 10, 64); err != nil {
					err = fmt.Errorf("param (%v) failed, should be int", optRcTTL)
					return
				}
				if tmp <= cmdVolMinRemoteCacheTTL {
					err = fmt.Errorf("param remoteCacheTTL(%v) must greater than or equal to %v", optRcTTL, cmdVolMinRemoteCacheTTL)
					return
				}
			}
			if optRcReadTimeout != "" {
				if tmp, err = strconv.ParseInt(optRcReadTimeout, 10, 64); err != nil {
					err = fmt.Errorf("param (%v) failed, should be int", optRcReadTimeout)
					return
				}
				if tmp <= 0 {
					err = fmt.Errorf("param remoteCacheReadTimeout(%v) must greater than 0", optRcReadTimeout)
					return
				}
			}
			if optFlashHotKeyMissCount != "" {
				if tmp, err = strconv.ParseInt(optFlashHotKeyMissCount, 10, 64); err != nil {
					err = fmt.Errorf("param (%v) failed, should be int", optFlashHotKeyMissCount)
					return
				}
				if tmp <= 0 {
					err = fmt.Errorf("param flashHotKeyMissCount(%v) must greater than 0", optFlashHotKeyMissCount)
					return
				}
			}

			if optFlashReadFlowLimit != "" {
				if tmp, err = strconv.ParseInt(optFlashReadFlowLimit, 10, 64); err != nil {
					err = fmt.Errorf("param (%v) failed, should be int", optFlashReadFlowLimit)
					return
				}
				if tmp < 0 {
					err = fmt.Errorf("param flashReadFlowLimit(%v) must greater than or equal to 0", optFlashReadFlowLimit)
					return
				}
			}

			if optFlashWriteFlowLimit != "" {
				if tmp, err = strconv.ParseInt(optFlashWriteFlowLimit, 10, 64); err != nil {
					err = fmt.Errorf("param (%v) failed, should be int", optFlashWriteFlowLimit)
					return
				}
				if tmp < 0 {
					err = fmt.Errorf("param flashWriteFlowLimit(%v) must greater than or equal to 0", optFlashWriteFlowLimit)
					return
				}
			}

			if optFlashKeyFlowLimit != "" {
				if tmp, err = strconv.ParseInt(optFlashKeyFlowLimit, 10, 64); err != nil {
					err = fmt.Errorf("param (%v) failed, should be int", optFlashKeyFlowLimit)
					return
				}
				if tmp < 0 {
					err = fmt.Errorf("param flashKeyFlowLimit(%v) must greater than or equal to 0", optFlashKeyFlowLimit)
					return
				}
			}

			if optRemoteClientFlowLimit != "" {
				if tmp, err = strconv.ParseInt(optRemoteClientFlowLimit, 10, 64); err != nil {
					err = fmt.Errorf("param (%v) failed, should be int", optRemoteClientFlowLimit)
					return
				}
				if tmp < 0 {
					err = fmt.Errorf("param remoteClientFlowLimit(%v) must greater than or equal to 0", optRemoteClientFlowLimit)
					return
				}
			}

			if optDpLimitSsdBaseCount != "" {
				if _, err = strconv.ParseUint(optDpLimitSsdBaseCount, 10, 64); err != nil {
					err = fmt.Errorf("param dpLimitSsdBaseCount(%v) should be uint", optDpLimitSsdBaseCount)
					return
				}
			}
			if optDpLimitSsdFactor != "" {
				if _, err = strconv.ParseUint(optDpLimitSsdFactor, 10, 64); err != nil {
					err = fmt.Errorf("param dpLimitSsdFactor(%v) should be uint", optDpLimitSsdFactor)
					return
				}
			}
			if optDpLimitHddBaseCount != "" {
				if _, err = strconv.ParseUint(optDpLimitHddBaseCount, 10, 64); err != nil {
					err = fmt.Errorf("param dpLimitHddBaseCount(%v) should be uint", optDpLimitHddBaseCount)
					return
				}
			}
			if optDpLimitHddFactor != "" {
				if _, err = strconv.ParseUint(optDpLimitHddFactor, 10, 64); err != nil {
					err = fmt.Errorf("param dpLimitHddFactor(%v) should be uint", optDpLimitHddFactor)
					return
				}
			}
			if optDefaultPoolId != "" {
				if _, err = strconv.ParseUint(optDefaultPoolId, 10, 8); err != nil {
					err = fmt.Errorf("param defaultPoolId(%v) should be uint8", optDefaultPoolId)
					return
				}
			}

			if optRemoteCacheMultiRead != "" {
				if _, err = strconv.ParseBool(optRemoteCacheMultiRead); err != nil {
					err = fmt.Errorf("param remoteCacheMultiRead(%v) should be true or false", optRemoteCacheMultiRead)
					return
				}
			}
			if optFlashNodeTimeoutCount != "" {
				if tmp, err = strconv.ParseInt(optFlashNodeTimeoutCount, 10, 64); err != nil {
					err = fmt.Errorf("param (%v) failed, should be int", optFlashNodeTimeoutCount)
					return
				}
				if tmp <= 0 {
					err = fmt.Errorf("param flashNodeTimeoutCount(%v) must greater than 0", optFlashNodeTimeoutCount)
					return
				}
			}
			if optRemoteCacheSameZoneTimeout != "" {
				if tmp, err = strconv.ParseInt(optRemoteCacheSameZoneTimeout, 10, 64); err != nil {
					err = fmt.Errorf("param (%v) failed, should be int", optRemoteCacheSameZoneTimeout)
					return
				}
				if tmp <= 0 {
					err = fmt.Errorf("param remoteCacheSameZoneTimeout(%v) must greater than 0", optRemoteCacheSameZoneTimeout)
					return
				}
			}
			if optRemoteCacheSameRegionTimeout != "" {
				if tmp, err = strconv.ParseInt(optRemoteCacheSameRegionTimeout, 10, 64); err != nil {
					err = fmt.Errorf("param (%v) failed, should be int", optRemoteCacheSameRegionTimeout)
					return
				}
				if tmp <= 0 {
					err = fmt.Errorf("param remoteCacheSameRegionTimeout(%v) must greater than 0", optRemoteCacheSameRegionTimeout)
					return
				}
			}
			if optAutoFixTag != "" {
				if _, err = strconv.ParseBool(optAutoFixTag); err != nil {
					err = fmt.Errorf("param autoFixTag(%v) should be true or false", optAutoFixTag)
					return
				}
			}
			if optDefaultDpTag != "" {
				if !proto.ValidateTag(optDefaultDpTag) {
					err = fmt.Errorf("tag invalid: length must be < 50 and only [0-9a-zA-Z] allowed")
					return
				}
			}
			if optDefaultMpTag != "" {
				if !proto.ValidateTag(optDefaultMpTag) {
					err = fmt.Errorf("tag invalid: length must be < 50 and only [0-9a-zA-Z] allowed")
					return
				}
			}

			if err = client.AdminAPI().SetClusterParas(&master.ClusterParas{
				BatchCount:                             optDelBatchCount,
				MarkDeleteRate:                         optMarkDeleteRate,
				DeleteWorkerSleepMs:                    optDelWorkerSleepMs,
				AutoRepairRate:                         optAutoRepairRate,
				LoadFactor:                             optLoadFactor,
				MaxDpCntLimit:                          opMaxDpCntLimit,
				MaxMpCntLimit:                          opMaxMpCntLimit,
				ClientIDKey:                            clientIDKey,
				EnableAutoDecommissionDisk:             autoDecommissionDisk,
				AutoDecommissionDiskInterval:           autoDecommissionDiskInterval,
				EnableAutoDpMetaRepair:                 autoDpMetaRepair,
				AutoDpMetaRepairParallelCnt:            autoDpMetaRepairParallelCnt,
				EnableAutoMpMetaRepair:                 autoMpMetaRepair,
				AutoMpMetaRepairParallelCnt:            autoMpMetaRepairParallelCnt,
				AutoDistributionOptimization:           autoDistributionOptimization,
				DpRepairTimeout:                        dpRepairTimeout,
				DpTimeout:                              dpTimeout,
				MpTimeout:                              mpTimeout,
				DpBackupTimeout:                        dpBackupTimeout,
				DecommissionDpLimit:                    decommissionDpLimit,
				DecommissionDiskLimit:                  decommissionDiskLimit,
				DecommissionFirstHostDiskParallelLimit: decommissionFirstHostDiskParallelLimit,
				ForbidWriteOpOfProtoVersion0:           forbidWriteOpOfProtoVersion0,
				MediaType:                              dataMediaType,
				HandleTimeout:                          handleTimeout,
				ReadDataNodeTimeout:                    readDataNodeTimeout,
				RackAware:                              rackAware,
				DistributionOptimizationConDpCnt:       distributionOptimizationConDpCnt,
				DistributionOptimizationThreshold:      distributionOptimizationThreshold,
				RemoteCacheTTL:                         optRcTTL,
				RemoteCacheReadTimeout:                 optRcReadTimeout,
				RemoteCacheMultiRead:                   optRemoteCacheMultiRead,
				FlashNodeTimeoutCount:                  optFlashNodeTimeoutCount,
				RemoteCacheSameZoneTimeout:             optRemoteCacheSameZoneTimeout,
				RemoteCacheSameRegionTimeout:           optRemoteCacheSameRegionTimeout,
				FlashHotKeyMissCount:                   optFlashHotKeyMissCount,
				FlashReadFlowLimit:                     optFlashReadFlowLimit,
				FlashWriteFlowLimit:                    optFlashWriteFlowLimit,
				FlashKeyFlowLimit:                      optFlashKeyFlowLimit,
				RemoteClientFlowLimit:                  optRemoteClientFlowLimit,
				EnableMpDecommissionByLearner:          enableMpDecommissionByLearner,
				LearnerRecoverTimeoutSeconds:           learnerRecoverTimeoutSeconds,
				MetaAutoAddReplicaLimit:                metaAutoAddReplicaLimit,
				MetaManualDecommissionLimit:            metaManualDecommissionLimit,
				MetaBalanceLimit:                       metaBalanceLimit,
				MetaManualAddReplicaLimit:              metaManualAddReplicaLimit,
				DpLimitSsdBaseCount:                    optDpLimitSsdBaseCount,
				DpLimitSsdFactor:                       optDpLimitSsdFactor,
				DpLimitHddBaseCount:                    optDpLimitHddBaseCount,
				DpLimitHddFactor:                       optDpLimitHddFactor,
				AutoFixTag:                             optAutoFixTag,
				DefaultDpTag:                           optDefaultDpTag,
				DefaultMpTag:                           optDefaultMpTag,
				PoolId:                                 optDefaultPoolId,
			}); err != nil {
				return
			}
			stdout("Cluster parameters have been set successfully.")
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
	cmd.Flags().StringVar(&autoDpMetaRepairParallelCnt, CliFlagAutoDpMetaRepairParallelCnt, "",
		fmt.Sprintf("Parallel count of auto data partition meta repair (%d-%d)",
			proto.MinAutoDpMetaRepairParallelCnt, proto.MaxAutoDpMetaRepairParallelCnt))
	cmd.Flags().StringVar(&autoMpMetaRepair, CliFlagAutoMpMetaRepair, "", "Enable or disable auto meta partition meta repair")
	cmd.Flags().StringVar(&autoMpMetaRepairParallelCnt, CliFlagAutoMpMetaRepairParallelCnt, "",
		fmt.Sprintf("Parallel count of auto meta partition meta repair (%d-%d)",
			proto.MinAutoMpMetaRepairParallelCnt, proto.MaxAutoMpMetaRepairParallelCnt))
	cmd.Flags().StringVar(&autoDistributionOptimization, CliFlagAutoDistributionOptimization, "", "Enable or disable distribution optimization")
	cmd.Flags().StringVar(&enableMpDecommissionByLearner, "enableMpDecommissionByLearner", "", "Enable or disable mp decommission by learner: [true | false]")
	cmd.Flags().StringVar(&dpRepairTimeout, CliFlagDpRepairTimeout, "", "Data partition repair timeout(example: 1h)")
	cmd.Flags().StringVar(&dpTimeout, CliFlagDpTimeout, "", "Data partition heartbeat timeout(example: 10s)")
	cmd.Flags().StringVar(&mpTimeout, CliFlagMpTimeout, "", "Meta partition heartbeat timeout(example: 10s)")
	cmd.Flags().StringVar(&autoDecommissionDisk, CliFlagAutoDecommissionDisk, "", "Enable or disable auto decommission disk")
	cmd.Flags().StringVar(&autoDecommissionDiskInterval, CliFlagAutoDecommissionDiskInterval, "", "Interval of auto decommission disk(example: 10s)")
	cmd.Flags().StringVar(&dpBackupTimeout, CliFlagDpBackupTimeout, "", "Data partition backup directory timeout(example: 1h)")
	cmd.Flags().StringVar(&decommissionDpLimit, CliFlagDecommissionDpLimit, "", "Limit for parallel  decommission dp")
	cmd.Flags().StringVar(&decommissionDiskLimit, CliFlagDecommissionDiskLimit, "", "Limit for parallel decommission disk")
	cmd.Flags().StringVar(&decommissionFirstHostDiskParallelLimit, CliFlagDecommissionFirstHostDiskParallelLimit, "", "Limit for parallel decommission dp in first host disk")
	cmd.Flags().StringVar(&metaAutoAddReplicaLimit, CliFlagMetaAutoAddReplicaLimit, "", "Limit for parallel meta partition auto add replica decommission")
	cmd.Flags().StringVar(&metaManualDecommissionLimit, CliFlagMetaManualDecommissionLimit, "", "Limit for parallel meta partition manual decommission")
	cmd.Flags().StringVar(&metaBalanceLimit, CliFlagMetaBalanceLimit, "", "Limit for parallel meta partition balance decommission")
	cmd.Flags().StringVar(&metaManualAddReplicaLimit, CliFlagMetaManualAddReplicaLimit, "", "Limit for parallel meta partition manual add replica decommission")
	cmd.Flags().StringVar(&forbidWriteOpOfProtoVersion0, CliForbidWriteOpOfProtoVersion0, "",
		"set datanode and metanode whether forbid write operate of packet whose protocol version is version-0: [true | false]")
	cmd.Flags().StringVar(&dataMediaType, "clusterDataMediaType", "", "set cluster media type, 1(ssd), 2(hdd)")
	cmd.Flags().StringVar(&handleTimeout, "flashNodeHandleReadTimeout", "", "Specify flash node handle read timeout (example:1000ms)")
	cmd.Flags().StringVar(&readDataNodeTimeout, "flashNodeReadDataNodeTimeout", "", "Specify flash node read data node timeout (example:3000ms)")
	cmd.Flags().StringVar(&rackAware, CliFlagRackAware, "", "Set rack aware level: 0(nolimit), 1(weak), 2(strong)")
	cmd.Flags().StringVar(&learnerRecoverTimeoutSeconds, "learnerRecoverTimeoutSeconds", "", "Set learner mode recovery timeout in seconds (must > 0)")
	// Distribution optimization parameters
	cmd.Flags().StringVar(&distributionOptimizationConDpCnt, CliFlagDistributionOptimizationConDpCnt, "", "Concurrent data partition count for distribution optimization")
	cmd.Flags().StringVar(&distributionOptimizationThreshold, CliFlagDistributionOptimizationThreshold, "", "Threshold for distribution optimization (0.0-1.0)")
	cmd.Flags().StringVar(&optRcTTL, CliFlagRemoteCacheTTL, "", "Remote cache ttl[Unit: s](must >= 10min, default 5day)")
	cmd.Flags().StringVar(&optRcReadTimeout, CliFlagRemoteCacheReadTimeout, "", "Remote cache read timeout millisecond(must > 0)")
	cmd.Flags().StringVar(&optRemoteCacheMultiRead, CliFlagRemoteCacheMultiRead, "", "Remote cache follower read(true|false)")
	cmd.Flags().StringVar(&optFlashNodeTimeoutCount, CliFlagFlashNodeTimeoutCount, "", "FlashNode timeout count, flashNode will be removed by client if it's timeout count exceeds this value")
	// cmd.Flags().StringVar(&optRemoteCacheSameZoneTimeout, CliFlagRemoteCacheSameZoneTimeout, "", "Remote cache same zone timeout microsecond(must > 0)")
	// cmd.Flags().StringVar(&optRemoteCacheSameRegionTimeout, CliFlagRemoteCacheSameRegionTimeout, "", "Remote cache same region timeout millisecond(must > 0)")
	cmd.Flags().StringVar(&optFlashHotKeyMissCount, CliFlagFlashHotKeyMissCount, "", "Flash hot key miss count(must > 0)")
	cmd.Flags().StringVar(&optFlashReadFlowLimit, CliFlagFlashReadFlowLimit, "", "Flash read flow limit(must >= 0)")
	cmd.Flags().StringVar(&optFlashWriteFlowLimit, CliFlagFlashWriteFlowLimit, "", "Flash write flow limit(must >= 0)")
	cmd.Flags().StringVar(&optFlashKeyFlowLimit, CliFlagFlashKeyFlowLimit, "", "Flash key flow limit(must >= 0)")
	cmd.Flags().StringVar(&optRemoteClientFlowLimit, CliFlagRemoteClientFlowLimit, "", "Remote client flow limit(must >= 0)")
	cmd.Flags().StringVar(&optDpLimitSsdBaseCount, CliFlagDpLimitSsdBaseCount, "", "DP limit SSD base count")
	cmd.Flags().StringVar(&optDpLimitSsdFactor, CliFlagDpLimitSsdFactor, "", "DP limit SSD factor per 120GB")
	cmd.Flags().StringVar(&optDpLimitHddBaseCount, CliFlagDpLimitHddBaseCount, "", "DP limit HDD base count")
	cmd.Flags().StringVar(&optDpLimitHddFactor, CliFlagDpLimitHddFactor, "", "DP limit HDD factor per 120GB")
	cmd.Flags().StringVar(&optDefaultPoolId, CliFlagDefaultPoolId, "", "set cluster default pool id")
	cmd.Flags().StringVar(&optAutoFixTag, CliFlagAutoFixTag, "", "Auto fix and migrate by tag(true|false)")
	cmd.Flags().StringVar(&optDefaultDpTag, CliFlagDefaultDpTag, "", "Default DP select tag. split with ','. 'null' means null string")
	cmd.Flags().StringVar(&optDefaultMpTag, CliFlagDefaultMpTag, "", "Default MP select tag. split with ','. 'null' means null string")

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

func newClusterQueryDistributionOptimizationStatusCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "query-distribution-optimization-status",
		Short: cmdQueryClusterDistributionOptimizationShort,
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				if err != nil {
					errout(err)
				}
			}()

			status, err := client.AdminAPI().QueryDistributionOptimizationStatus()
			if err != nil {
				return
			}

			stdout("%v", formatDistributionOptimizationStatus(status))
		},
	}
	return cmd
}

func newClusterQueryDpDecommissionStatusCmd(client *master.MasterClient) *cobra.Command {
	var decommissionType int
	cmd := &cobra.Command{
		Use:   "query-dp-decommission-status",
		Short: cmdQueryDpDecommissionStatusShort,
		Long: `Query data partition decommission status by decommission type.
Decommission types:
  1 - ManualDecommission
  2 - AutoDecommission
  4 - AutoAddReplica
  5 - ManualAddReplica
  6 - DistributionOptimization`,
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				if err != nil {
					errout(err)
				}
			}()

			response, err := client.AdminAPI().QueryDpDecommissionStatus(decommissionType)
			if err != nil {
				return
			}

			stdout("%v", formatQueryDecommissionStatusResponse(response))
		},
	}
	cmd.Flags().IntVar(&decommissionType, "type", 0, "Decommission type (required)")
	cmd.MarkFlagRequired("type")
	return cmd
}
