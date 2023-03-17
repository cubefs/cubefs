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
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/chubaofs/chubaofs/sdk/meta"
	"github.com/chubaofs/chubaofs/storage"
	"github.com/chubaofs/chubaofs/util/bitset"
	"github.com/chubaofs/chubaofs/util/log"

	"github.com/chubaofs/chubaofs/cli/api"
	"github.com/chubaofs/chubaofs/metanode"

	"github.com/chubaofs/chubaofs/util/errors"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/master"
	"github.com/spf13/cobra"
)

const (
	cmdVolUse   = "volume [COMMAND]"
	cmdVolShort = "Manage cluster volumes"
	line        = "\n---------------------------------------------------\n"
)

func newVolCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:     cmdVolUse,
		Short:   cmdVolShort,
		Args:    cobra.MinimumNArgs(0),
		Aliases: []string{"vol"},
	}
	cmd.AddCommand(
		newVolListCmd(client),
		newVolCreateCmd(client),
		newVolInfoCmd(client),
		newVolDeleteCmd(client),
		newVolTransferCmd(client),
		newVolAddDPCmd(client),
		newVolSetCmd(client),
		newVolShrinkCapacityCmd(client),
		newVolPartitionCheckCmd(client),
		newVolSetMinRWPartitionCmd(client),
		newVolConvertTaskCmd(client),
		newVolAddMPCmd(client),
		newVolEcInfoCmd(client),
		newVolEcSetCmd(client),
		newVolEcPartitionsConsistency(client),
		newVolEcPartitionsChunkConsistency(client),
		newVolCheckEKDeletedByMistake(client),
		newVolSetChildFileMaxCountCmd(client),
	)
	return cmd
}

const (
	cmdVolListShort = "List cluster volumes"
)

func newVolListCmd(client *master.MasterClient) *cobra.Command {
	var optKeyword string
	var optDetailMod bool
	var optMediumType string
	var cmd = &cobra.Command{
		Use:     CliOpList,
		Short:   cmdVolListShort,
		Aliases: []string{"ls"},
		Run: func(cmd *cobra.Command, args []string) {
			var vols []*proto.VolInfo
			var err error
			defer func() {
				if err != nil {
					errout("List cluster volume failed:\n%v\n", err)
				}
			}()
			if vols, err = client.AdminAPI().ListVolsByKeywordsAndSmart(optKeyword, optMediumType); err != nil {
				return
			}
			sort.Slice(vols, func(i, j int) bool { return vols[i].Name < vols[j].Name })
			if optDetailMod {
				stdout("%v\n", volumeDetailInfoTableHeader)
			} else {
				stdout("%v\n", volumeInfoTableHeader)
			}
			for _, vol := range vols {
				var vv *proto.SimpleVolView
				if vv, err = client.AdminAPI().GetVolumeSimpleInfo(vol.Name); err != nil {
					return
				}
				if optDetailMod {
					stdout("%v\n", formatVolDetailInfoTableRow(vv, vol))
				} else {
					stdout("%v\n", formatVolInfoTableRow(vol))
				}
			}
		},
	}
	cmd.Flags().BoolVarP(&optDetailMod, "detail-mod", "d", false, "list the volumes with empty zone name")
	cmd.Flags().StringVar(&optKeyword, "keyword", "", "Specify keyword of volume name to filter")
	cmd.Flags().StringVar(&optMediumType, "medium-type", "", "Specify the volume smart")

	return cmd
}

const (
	cmdVolCreateUse             = "create [VOLUME NAME] [USER ID]"
	cmdVolCreateShort           = "Create a new volume"
	cmdVolDefaultMPCount        = 3
	cmdVolDefaultDPSize         = 120
	cmdVolDefaultCapacity       = 10 // 100GB
	cmdVolDefaultReplicas       = 3
	cmdVolDefaultTrashDays      = 0
	cmdVolDefaultFollowerReader = true
	cmdVolDefaultForceROW       = false
	cmdVolDefaultIsSmart        = false
	cmdVolDefaultNearReader     = false
	cmdVolDefaultWriteCache     = false
	cmdVolDefaultCrossRegionHA  = 0
	cmdVolDefaultZoneName       = "default"
	cmdVolDefaultStoreMode      = int(proto.StoreModeMem)
	cmdVolDefMetaLayout         = "0,0"
	cmdVolDefaultCompact        = false
	cmdVolDefaultEcDataNum      = 4
	cmdVolDefaultEcParityNum    = 2
	cmdVolDefaultEcEnable       = false
	cmdVolDefaultMaxChildrenCnt = 100 * 10000
	cmdVolDefaultReuseMP        = false
)

func newVolCreateCmd(client *master.MasterClient) *cobra.Command {
	var optMPCount int
	var optDPSize uint64
	var optCapacity uint64
	var optReplicas int
	var optMpReplicas int
	var optFollowerRead bool
	var optForceROW bool
	var optEnableWriteCache bool
	var optCrossRegionHAType uint8
	var optEcDataNum uint8
	var optEcParityNum uint8
	var optEcEnable bool
	var optAutoRepair bool
	var optVolWriteMutex bool
	var optYes bool
	var optZoneName string
	var optTrashDays int
	var optStoreMode int
	var optLayout string
	var optIsSmart bool
	var smartRules []string
	var optCompactTag bool
	var optFolReadDelayInterval int64
	var optBatchDelInodeCnt uint64
	var optDelInodeInterval uint64
	var optReuseMP bool
	var optEnableBitMapAllocator bool
	var cmd = &cobra.Command{
		Use:   cmdVolCreateUse,
		Short: cmdVolCreateShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var volumeName = args[0]
			var userID = args[1]
			var num1, num2, total int

			total, err = fmt.Sscanf(optLayout, "%d,%d", &num1, &num2)
			if total != 2 || err != nil || (num1 < 0 || num1 > 100) || (num2 < 0 || num2 > 100) {
				errout("input layout format err\n")
			}

			if optStoreMode < int(proto.StoreModeMem) || optStoreMode > int(proto.StoreModeMax-1) {
				errout("input store mode err, need[%d~%d], but input:%d\n", int(proto.StoreModeMem), int(proto.StoreModeMax-1), optStoreMode)
			}

			if optTrashDays > 30 {
				errout("error: the max trash days is 30\n")
			}

			storeMode := proto.StoreMode(optStoreMode)

			if !optForceROW && optCompactTag {
				errout("error: compact cannot be opened when force row is closed. Please open force row first\n")
			}
			// ask user for confirm
			if !optYes {
				stdout("Create a new volume:\n")
				stdout("  Name                : %v\n", volumeName)
				stdout("  Owner               : %v\n", userID)
				stdout("  Dara partition size : %v GB\n", optDPSize)
				stdout("  Meta partition count: %v\n", optMPCount)
				stdout("  Capacity            : %v GB\n", optCapacity)
				stdout("  Replicas            : %v\n", optReplicas)
				stdout("  MpReplicas          : %v\n", optMpReplicas)
				stdout("  TrashDays           : %v\n", optTrashDays)
				stdout("  Ec data units num   : %v\n", optEcDataNum)
				stdout("  Ec parity units num : %v\n", optEcParityNum)
				stdout("  Ec is enabled       : %v\n", optEcEnable)
				stdout("  Allow follower read : %v\n", formatEnabledDisabled(optFollowerRead))
				stdout("  Force ROW           : %v\n", formatEnabledDisabled(optForceROW))
				stdout("  Enable Write Cache  : %v\n", formatEnabledDisabled(optEnableWriteCache))
				stdout("  Cross Region HA     : %s\n", proto.CrossRegionHAType(optCrossRegionHAType))
				stdout("  Auto repair         : %v\n", formatEnabledDisabled(optAutoRepair))
				stdout("  Volume write mutex  : %v\n", formatEnabledDisabled(optVolWriteMutex))
				stdout("  ZoneName            : %v\n", optZoneName)
				stdout("  Store mode          : %v\n", storeMode.Str())
				stdout("  Meta layout         : %v - %v\n", num1, num2)
				stdout("  Smart Enable        : %s\n", formatEnabledDisabled(optIsSmart))
				stdout("  Smart Rules         : %v\n", strings.Join(smartRules, ","))
				stdout("  Compact             : %v\n", formatEnabledDisabled(optCompactTag))
				stdout("  FolReadDelayInterval: %v\n", optFolReadDelayInterval)
				stdout("  BatchDelInodeCnt    : %v\n", optBatchDelInodeCnt)
				stdout("  DelInodeInterval    : %v\n", optDelInodeInterval)
				stdout("  ReuseMP             : %v\n", formatEnabledDisabled(optReuseMP))
				stdout("  BitMapAllocator     : %v\n", formatEnabledDisabled(optEnableBitMapAllocator))
				stdout("\nConfirm (yes/no)[yes]: ")
				var userConfirm string
				_, _ = fmt.Scanln(&userConfirm)
				if userConfirm != "yes" && len(userConfirm) != 0 {
					stdout("Abort by user.\n")
					return
				}
			}

			err = client.AdminAPI().CreateVolume(volumeName, userID, optMPCount, optDPSize, optCapacity, optReplicas,
				optMpReplicas, optTrashDays, optStoreMode, optFollowerRead, optAutoRepair, optVolWriteMutex, optForceROW, optIsSmart, optEnableWriteCache, optReuseMP,
				optZoneName, optLayout, strings.Join(smartRules, ","), optCrossRegionHAType, formatEnabledDisabled(optCompactTag), optEcDataNum,
				optEcParityNum, optEcEnable, optFolReadDelayInterval, optBatchDelInodeCnt, optDelInodeInterval, optEnableBitMapAllocator)
			if err != nil {
				errout("Create volume failed case:\n%v\n", err)
			}
			stdout("Create volume success.\n")
			return
		},
	}
	cmd.Flags().IntVar(&optMPCount, CliFlagMetaPartitionCount, cmdVolDefaultMPCount, "Specify init meta partition count")
	cmd.Flags().Uint64Var(&optDPSize, CliFlagDataPartitionSize, cmdVolDefaultDPSize, "Specify size of data partition size [Unit: GB]")
	cmd.Flags().Uint64Var(&optCapacity, CliFlagCapacity, cmdVolDefaultCapacity, "Specify volume capacity [Unit: GB]")
	cmd.Flags().IntVar(&optReplicas, CliFlagReplicas, cmdVolDefaultReplicas, "Specify data partition replicas number")
	cmd.Flags().IntVar(&optMpReplicas, CliFlagMpReplicas, cmdVolDefaultReplicas, "Specify meta partition replicas number")
	cmd.Flags().IntVar(&optTrashDays, CliFlagTrashDays, cmdVolDefaultTrashDays, "Specify trash remaining days ")
	cmd.Flags().BoolVar(&optFollowerRead, CliFlagEnableFollowerRead, cmdVolDefaultFollowerReader, "Enable read from replica follower")
	cmd.Flags().Int64Var(&optFolReadDelayInterval, CliFlagFollReadDelayInterval, 0, "Specify host delay update interval [Unit: second]")
	cmd.Flags().BoolVar(&optForceROW, CliFlagEnableForceROW, cmdVolDefaultForceROW, "Use ROW instead of overwrite")
	cmd.Flags().BoolVar(&optEnableWriteCache, CliFlagEnableWriteCache, cmdVolDefaultWriteCache, "Enable write back cache when mounting FUSE")
	cmd.Flags().Uint8Var(&optCrossRegionHAType, CliFlagEnableCrossRegionHA, cmdVolDefaultCrossRegionHA,
		"Set cross region high available type(0 for default, 1 for quorum)")
	cmd.Flags().BoolVar(&optAutoRepair, CliFlagAutoRepair, false, "Enable auto balance partition distribution according to zoneName")
	cmd.Flags().BoolVar(&optVolWriteMutex, CliFlagVolWriteMutexEnable, false, "Enable only one client have volume write permission")
	cmd.Flags().StringVar(&optZoneName, CliFlagZoneName, cmdVolDefaultZoneName, "Specify volume zone name")
	cmd.Flags().BoolVarP(&optYes, "yes", "y", false, "Answer yes for all questions")
	cmd.Flags().IntVar(&optStoreMode, CliFlagStoreMode, cmdVolDefaultStoreMode, "Specify volume store mode[1:Mem, 2:RocksDb]")
	cmd.Flags().StringVar(&optLayout, CliFlagMetaLayout, cmdVolDefMetaLayout, "Specify volume mp layout num1,num2 [num1:rocks db mp percent, num2:rocks db replica percent]")
	cmd.Flags().BoolVar(&optIsSmart, CliFlagIsSmart, cmdVolDefaultIsSmart, "Enable the smart vol or not")
	cmd.Flags().StringSliceVar(&smartRules, CliSmartRulesMode, []string{}, "Specify volume smart rules")
	cmd.Flags().BoolVar(&optCompactTag, CliFlagCompactTag, cmdVolDefaultCompact, "Specify volume compact")
	cmd.Flags().Uint8Var(&optEcDataNum, CliFlagEcDataNum, cmdVolDefaultEcDataNum, "Specify ec data units number")
	cmd.Flags().Uint8Var(&optEcParityNum, CliFlagEcParityNum, cmdVolDefaultEcParityNum, "Specify ec parity units number")
	cmd.Flags().BoolVar(&optEcEnable, CliFlagEcEnable, cmdVolDefaultEcEnable, "Enable ec partiton backup")
	cmd.Flags().Uint64Var(&optBatchDelInodeCnt, CliOpVolBatchDelInodeCnt, 0, "Specify batch del inode cnt [default :0 use meta node default 128]")
	cmd.Flags().Uint64Var(&optDelInodeInterval, CliOpVolDelInodeInterval, 0, "Specify del inodes interval  [Unit: ms, default 0]")
	cmd.Flags().BoolVar(&optReuseMP, CliFlagReuseMP, cmdVolDefaultReuseMP, "reuse mp when add mp")
	cmd.Flags().BoolVar(&optEnableBitMapAllocator, CliFlagBitMapAllocatorSt, false, "bit map allocator enable state")
	return cmd
}

const (
	cmdVolInfoUse      = "info [VOLUME NAME]"
	cmdVolInfoShort    = "Show volume information"
	cmdVolSetShort     = "Set configuration of the volume"
	cmdVolEcInfoUse    = "ec-info [VOLUME NAME]"
	cmdVolEcInfoShort  = "Show volume ec information"
	cmdVolEcSetShort   = "Set ec configuration of the volume"
	cmdVolEcPartitions = "Check Consistency of ec partition and data partition in volume"
	cmdVolEcChunkData  = "Check ecPartition Consistency of chunk data"
)

func newVolSetCmd(client *master.MasterClient) *cobra.Command {
	var (
		optCapacity              uint64
		optReplicas              int
		optMpReplicas            int
		optTrashDays             int
		optStoreMode             int
		optFollowerRead          string
		optVolWriteMutex         string
		optNearRead              string
		optForceROW              string
		optEnableWriteCache      string
		optAuthenticate          string
		optEnableToken           string
		optAutoRepair            string
		optBucketPolicy          string
		optCrossRegionHAType     string
		optZoneName              string
		optLayout                string
		optExtentCacheExpireSec  int64
		optYes                   bool
		confirmString            = strings.Builder{}
		vv                       *proto.SimpleVolView
		optIsSmart               string
		smartRules               []string
		optCompactTag            string
		optFolReadDelayInterval  int64
		optLowestDelayHostWeight int
		optTrashCleanInterval    int
		optBatchDelInodeCnt      int
		optDelInodeInterval      int
		optUmpCollectWay         int
		optReuseMP               string
		optEnableBitMapAllocator string
		optTrashCleanDuration    int32
		optTrashCleanMaxCount    int32
		optCursorSkipStep        int64
	)
	var cmd = &cobra.Command{
		Use:   CliOpSet + " [VOLUME NAME]",
		Short: cmdVolSetShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var volumeName = args[0]
			var isChange = false
			var num1, num2, total int

			var forceRowChange = false
			var compactTagChange = false
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			fmt.Printf("%v\n", smartRules)

			if vv, err = client.AdminAPI().GetVolumeSimpleInfo(volumeName); err != nil {
				return
			}
			confirmString.WriteString("Volume configuration changes:\n")
			confirmString.WriteString(fmt.Sprintf("  Name                : %v\n", vv.Name))
			if optCapacity > 0 {
				isChange = true
				confirmString.WriteString(fmt.Sprintf("  Capacity            : %v GB -> %v GB\n", vv.Capacity, optCapacity))
				vv.Capacity = optCapacity
			} else {
				confirmString.WriteString(fmt.Sprintf("  Capacity            : %v GB\n", vv.Capacity))
			}
			if optReplicas > 0 {
				isChange = true
				confirmString.WriteString(fmt.Sprintf("  Dp Replicas         : %v -> %v\n", vv.DpReplicaNum, optReplicas))
				vv.DpReplicaNum = uint8(optReplicas)
			} else {
				confirmString.WriteString(fmt.Sprintf("  Dp Replicas         : %v\n", vv.DpReplicaNum))
			}
			if optMpReplicas > 0 {
				isChange = true
				confirmString.WriteString(fmt.Sprintf("  Mp Replicas         : %v -> %v\n", vv.MpReplicaNum, optMpReplicas))
				vv.MpReplicaNum = uint8(optMpReplicas)
			} else {
				confirmString.WriteString(fmt.Sprintf("  Mp Replicas         : %v\n", vv.MpReplicaNum))
			}
			if optExtentCacheExpireSec != 0 {
				isChange = true
				confirmString.WriteString(fmt.Sprintf("  ExtentCache Expire  : %v -> %v\n",
					time.Duration(vv.ExtentCacheExpireSec)*time.Second, time.Duration(optExtentCacheExpireSec)*time.Second))
				vv.ExtentCacheExpireSec = optExtentCacheExpireSec
			} else {
				confirmString.WriteString(fmt.Sprintf("  ExtentCache Expire  : %v\n", time.Duration(vv.ExtentCacheExpireSec)*time.Second))
			}

			if optFollowerRead != "" {
				isChange = true
				var enable bool
				if enable, err = strconv.ParseBool(optFollowerRead); err != nil {
					return
				}
				confirmString.WriteString(fmt.Sprintf("  Allow follower read : %v -> %v\n", formatEnabledDisabled(vv.FollowerRead), formatEnabledDisabled(enable)))
				vv.FollowerRead = enable
			} else {
				confirmString.WriteString(fmt.Sprintf("  Allow follower read : %v\n", formatEnabledDisabled(vv.FollowerRead)))
			}
			if optFolReadDelayInterval >= 0 {
				isChange = true
				confirmString.WriteString(fmt.Sprintf("  Host Delay Interval : %v sec -> %v sec\n", vv.DpFolReadDelayConfig.DelaySummaryInterval, optFolReadDelayInterval))
				vv.DpFolReadDelayConfig.DelaySummaryInterval = optFolReadDelayInterval
			} else {
				confirmString.WriteString(fmt.Sprintf("  Host Delay Interval : %v sec\n", vv.DpFolReadDelayConfig.DelaySummaryInterval))
			}
			if optLowestDelayHostWeight > 0 && optLowestDelayHostWeight <= 100 && optLowestDelayHostWeight != vv.FolReadHostWeight {
				isChange = true
				confirmString.WriteString(fmt.Sprintf("  LowestDelay Host Weight  : %v -> %v\n", vv.FolReadHostWeight, optLowestDelayHostWeight))
				vv.FolReadHostWeight = optLowestDelayHostWeight
			} else {
				confirmString.WriteString(fmt.Sprintf("  LowestDelay Host Weight  : %v\n", vv.FolReadHostWeight))
			}
			if optNearRead != "" {
				isChange = true
				var enable bool
				if enable, err = strconv.ParseBool(optNearRead); err != nil {
					return
				}
				confirmString.WriteString(fmt.Sprintf("  Allow near read     : %v -> %v\n", formatEnabledDisabled(vv.NearRead), formatEnabledDisabled(enable)))
				vv.NearRead = enable
			} else {
				confirmString.WriteString(fmt.Sprintf("  Allow near read     : %v\n", formatEnabledDisabled(vv.NearRead)))
			}
			if optForceROW != "" {
				isChange = true
				var enable bool
				if enable, err = strconv.ParseBool(optForceROW); err != nil {
					return
				}
				confirmString.WriteString(fmt.Sprintf("  Force ROW           : %v -> %v\n", formatEnabledDisabled(vv.ForceROW), formatEnabledDisabled(enable)))
				if vv.ForceROW != enable {
					forceRowChange = true
				}
				vv.ForceROW = enable
			} else {
				confirmString.WriteString(fmt.Sprintf("  Force ROW           : %v\n", formatEnabledDisabled(vv.ForceROW)))
			}
			if optEnableWriteCache != "" {
				isChange = true
				var enable bool
				if enable, err = strconv.ParseBool(optEnableWriteCache); err != nil {
					return
				}
				confirmString.WriteString(fmt.Sprintf("  Enable Write Cache  : %v -> %v\n", formatEnabledDisabled(vv.EnableWriteCache), formatEnabledDisabled(enable)))
				vv.EnableWriteCache = enable
			} else {
				confirmString.WriteString(fmt.Sprintf("  Enable Write Cache  : %v\n", formatEnabledDisabled(vv.EnableWriteCache)))
			}

			if optAuthenticate != "" {
				isChange = true
				var enable bool
				if enable, err = strconv.ParseBool(optAuthenticate); err != nil {
					return
				}
				confirmString.WriteString(fmt.Sprintf("  Authenticate        : %v -> %v\n", formatEnabledDisabled(vv.Authenticate), formatEnabledDisabled(enable)))
				vv.Authenticate = enable
			} else {
				confirmString.WriteString(fmt.Sprintf("  Authenticate        : %v\n", formatEnabledDisabled(vv.Authenticate)))
			}
			if optEnableToken != "" {
				isChange = true
				var enable bool
				if enable, err = strconv.ParseBool(optEnableToken); err != nil {
					return
				}
				confirmString.WriteString(fmt.Sprintf("  EnableToken         : %v -> %v\n", formatEnabledDisabled(vv.EnableToken), formatEnabledDisabled(enable)))
				vv.EnableToken = enable
			} else {
				confirmString.WriteString(fmt.Sprintf("  EnableToken         : %v\n", formatEnabledDisabled(vv.EnableToken)))
			}
			if optAutoRepair != "" {
				isChange = true
				var enable bool
				if enable, err = strconv.ParseBool(optAutoRepair); err != nil {
					return
				}
				confirmString.WriteString(fmt.Sprintf("  AutoRepair          : %v -> %v\n", formatEnabledDisabled(vv.AutoRepair), formatEnabledDisabled(enable)))
				vv.AutoRepair = enable
			} else {
				confirmString.WriteString(fmt.Sprintf("  AutoRepair          : %v\n", formatEnabledDisabled(vv.AutoRepair)))
			}
			if optVolWriteMutex != "" {
				isChange = true
				var enable bool
				if enable, err = strconv.ParseBool(optVolWriteMutex); err != nil {
					return
				}
				confirmString.WriteString(fmt.Sprintf("  Enable write mutex  : %v -> %v\n", formatEnabledDisabled(vv.VolWriteMutexEnable), formatEnabledDisabled(enable)))
				vv.VolWriteMutexEnable = enable
			} else {
				confirmString.WriteString(fmt.Sprintf("  Enable write mutex  : %v\n", formatEnabledDisabled(vv.VolWriteMutexEnable)))
			}
			if optBucketPolicy != "" {
				isChange = true
				var bucketPolicyUint uint64
				var bucketPolicy proto.BucketAccessPolicy
				if bucketPolicyUint, err = strconv.ParseUint(optBucketPolicy, 10, 64); err != nil {
					return
				}
				bucketPolicy = proto.BucketAccessPolicy(bucketPolicyUint)
				confirmString.WriteString(fmt.Sprintf("  OSSBucketPolicy     : %s -> %s\n", vv.OSSBucketPolicy, bucketPolicy))
				vv.OSSBucketPolicy = bucketPolicy
			} else {
				confirmString.WriteString(fmt.Sprintf("  OSSBucketPolicy     : %s\n", vv.OSSBucketPolicy))
			}
			if optCrossRegionHAType != "" {
				isChange = true
				var crossRegionHATypeUint uint64
				var crossRegionHAType proto.CrossRegionHAType
				if crossRegionHATypeUint, err = strconv.ParseUint(optCrossRegionHAType, 10, 64); err != nil {
					return
				}
				crossRegionHAType = proto.CrossRegionHAType(crossRegionHATypeUint)
				confirmString.WriteString(fmt.Sprintf("  CrossRegionHAType   : %s -> %s\n", vv.CrossRegionHAType, crossRegionHAType))
				vv.CrossRegionHAType = crossRegionHAType
			} else {
				confirmString.WriteString(fmt.Sprintf("  CrossRegionHAType   : %s\n", vv.CrossRegionHAType))
			}
			if "" != optZoneName {
				isChange = true
				confirmString.WriteString(fmt.Sprintf("  ZoneName            : %v -> %v\n", vv.ZoneName, optZoneName))
				vv.ZoneName = optZoneName
			} else {
				confirmString.WriteString(fmt.Sprintf("  ZoneName            : %v\n", vv.ZoneName))
			}

			if optTrashDays > -1 && uint32(optTrashDays) != vv.TrashRemainingDays {
				isChange = true
				confirmString.WriteString(fmt.Sprintf("  TrashRemainingDays  : %v -> %v\n", vv.TrashRemainingDays, optTrashDays))
				vv.TrashRemainingDays = uint32(optTrashDays)
				if optTrashDays > 30 {
					errout("error: the max trash days is 30\n")
				}
			}

			if optStoreMode != 0 {
				if optStoreMode < int(proto.StoreModeMem) || optStoreMode > int(proto.StoreModeMax-1) {
					errout("input store mode err\n")
				}

				storeMode := proto.StoreMode(optStoreMode)
				if vv.DefaultStoreMode == storeMode {
					confirmString.WriteString(fmt.Sprintf("  StoreMode           : %v\n", vv.DefaultStoreMode))
				} else {
					isChange = true
					confirmString.WriteString(fmt.Sprintf("  StoreMode           : %v  --> %v\n",
						vv.DefaultStoreMode.Str(), storeMode.Str()))
					vv.DefaultStoreMode = storeMode
				}
			}

			if optLayout != "" {
				total, err = fmt.Sscanf(optLayout, "%d,%d", &num1, &num2)
				if total != 2 || err != nil || (num1 < 0 || num1 > 100) || (num2 < 0 || num2 > 100) {
					errout("input layout format err\n")
				}

				if uint32(num1) == vv.MpLayout.PercentOfMP && uint32(num2) == vv.MpLayout.PercentOfReplica {
					confirmString.WriteString(fmt.Sprintf("  MetaLayout          : %v - %v\n", vv.MpLayout.PercentOfMP, vv.MpLayout.PercentOfReplica))
				} else {
					isChange = true
					confirmString.WriteString(fmt.Sprintf("  MetaLayout          : (%v - %v)--> (%v - %v)\n",
						vv.MpLayout.PercentOfMP, vv.MpLayout.PercentOfReplica, num1, num2))
					vv.MpLayout.PercentOfMP = uint32(num1)
					vv.MpLayout.PercentOfReplica = uint32(num2)
				}
			}

			if optIsSmart != "" {
				isChange = true
				var enable bool
				if enable, err = strconv.ParseBool(optIsSmart); err != nil {
					return
				}
				confirmString.WriteString(fmt.Sprintf("  smart               : %v -> %v\n", vv.IsSmart, enable))
				vv.IsSmart = enable
			} else {
				confirmString.WriteString(fmt.Sprintf("  smart               : %v\n", vv.IsSmart))
			}
			if len(smartRules) != 0 {
				isChange = true
				confirmString.WriteString(fmt.Sprintf("  SmartRules          : %s -> %s\n", strings.Join(vv.SmartRules, ","), strings.Join(smartRules, ",")))
				vv.SmartRules = smartRules
			} else {
				confirmString.WriteString(fmt.Sprintf("  SmartRules          : %s\n", strings.Join(vv.SmartRules, ",")))
			}

			if optCompactTag != "" {
				isChange = true
				var enable bool
				if enable, err = strconv.ParseBool(optCompactTag); err != nil {
					return
				}
				if enable {
					optCompactTag = proto.CompactOpenName
				} else {
					optCompactTag = proto.CompactCloseName
				}
				confirmString.WriteString(fmt.Sprintf("  compact             : %v -> %v\n", vv.CompactTag, optCompactTag))
				if vv.CompactTag != optCompactTag {
					compactTagChange = true
				}
				vv.CompactTag = optCompactTag
			} else {
				confirmString.WriteString(fmt.Sprintf("  compact             : %v\n", vv.CompactTag))
			}

			if optTrashCleanInterval > -1 && uint64(optTrashCleanInterval) != vv.TrashCleanInterval {
				isChange = true
				confirmString.WriteString(fmt.Sprintf("  TrashCleanInteval  : %v -> %v\n", vv.TrashCleanInterval, optTrashCleanInterval))
				vv.TrashCleanInterval = uint64(optTrashCleanInterval)
			}

			if optBatchDelInodeCnt > -1 && uint32(optBatchDelInodeCnt) != vv.BatchDelInodeCnt {
				isChange = true
				confirmString.WriteString(fmt.Sprintf("  BatchDelInodeCnt   : %v -> %v\n", vv.BatchDelInodeCnt, optBatchDelInodeCnt))
				vv.BatchDelInodeCnt = uint32(optBatchDelInodeCnt)
			}

			if optDelInodeInterval > -1 && uint32(optDelInodeInterval) != vv.DelInodeInterval {
				isChange = true
				confirmString.WriteString(fmt.Sprintf("  DelInodeInterval   : %v -> %v\n", vv.DelInodeInterval, optDelInodeInterval))
				vv.DelInodeInterval = uint32(optDelInodeInterval)
			}

			if optUmpCollectWay >= 0 {
				isChange = true
				confirmString.WriteString(fmt.Sprintf("  UmpCollectWay       : %v -> %v\n", proto.UmpCollectByStr(vv.UmpCollectWay), proto.UmpCollectByStr(proto.UmpCollectBy(optUmpCollectWay))))
				vv.UmpCollectWay = proto.UmpCollectBy(optUmpCollectWay)
			} else {
				confirmString.WriteString(fmt.Sprintf("  UmpCollectWay       : %v\n", proto.UmpCollectByStr(vv.UmpCollectWay)))
			}

			if optReuseMP != "" {
				var enable bool
				if enable, err = strconv.ParseBool(optReuseMP); err != nil {
					return
				}
				if vv.ReuseMP != enable {
					isChange = true
					confirmString.WriteString(fmt.Sprintf("  Reuse MP            : %v -> %v\n", formatEnabledDisabled(vv.ReuseMP), formatEnabledDisabled(enable)))
					vv.ReuseMP = enable
				} else {
					confirmString.WriteString(fmt.Sprintf("  Reuse MP            : %v\n", formatEnabledDisabled(vv.ReuseMP)))
				}
			} else {
				confirmString.WriteString(fmt.Sprintf("  Reuse MP            : %v\n", formatEnabledDisabled(vv.ReuseMP)))
			}

			if optEnableBitMapAllocator != "" {
				var enable bool
				if enable, err = strconv.ParseBool(optEnableBitMapAllocator); err != nil {
					return
				}
				if vv.EnableBitMapAllocator != enable {
					isChange = true
					confirmString.WriteString(fmt.Sprintf("  BitMapAllocator     : %v -> %v\n", formatEnabledDisabled(vv.EnableBitMapAllocator), formatEnabledDisabled(enable)))
					vv.EnableBitMapAllocator = enable
				} else {
					confirmString.WriteString(fmt.Sprintf("  BitMapAllocator     : %v\n", formatEnabledDisabled(vv.EnableBitMapAllocator)))
				}
			} else {
				confirmString.WriteString(fmt.Sprintf("  BitMapAllocator     : %v\n", formatEnabledDisabled(vv.EnableBitMapAllocator)))
                        }

			if optTrashCleanMaxCount >= 0 {
				isChange = true
				confirmString.WriteString(fmt.Sprintf("  TrashCleanMaxCount  : %v -> %v\n", vv.TrashCleanMaxCount, optTrashCleanMaxCount))
				vv.TrashCleanMaxCount = optTrashCleanMaxCount
			} else {
				confirmString.WriteString(fmt.Sprintf("  TrashCleanMaxCount  : %v\n", vv.TrashCleanMaxCount))
			}

			if optTrashCleanDuration >= 0 {
				isChange = true
				confirmString.WriteString(fmt.Sprintf("  TrashCleanDuration  : %v -> %v\n", vv.TrashCleanDuration, optTrashCleanDuration))
				vv.TrashCleanDuration = optTrashCleanDuration
			} else {
				confirmString.WriteString(fmt.Sprintf("  TrashCleanDuration  : %v\n", vv.TrashCleanDuration))
			}

			if optCursorSkipStep >= 0 {
				if vv.CursorSkipStep != uint64(optCursorSkipStep) {
					isChange = true
					confirmString.WriteString(fmt.Sprintf("  CursorSkipStep      : %v -> %v\n", vv.CursorSkipStep, optCursorSkipStep))
					vv.CursorSkipStep = uint64(optCursorSkipStep)
				} else {
					confirmString.WriteString(fmt.Sprintf("  CursorSkipStep      : %v\n", vv.CursorSkipStep))
				}
			} else {
				confirmString.WriteString(fmt.Sprintf("  CursorSkipStep      : %v\n", vv.CursorSkipStep))
			}

			if err != nil {
				return
			}
			if !isChange {
				stdout("No changes has been set.\n")
				return
			}
			checkForceRowAndCompact(vv, forceRowChange, compactTagChange)
			// ask user for confirm
			if !optYes {
				stdout(confirmString.String())
				stdout("\nConfirm (yes/no)[yes]: ")
				var userConfirm string
				_, _ = fmt.Scanln(&userConfirm)
				if userConfirm != "yes" && len(userConfirm) != 0 {
					err = fmt.Errorf("Abort by user.\n")
					return
				}
			}
			err = client.AdminAPI().UpdateVolume(vv.Name, vv.Capacity, int(vv.DpReplicaNum), int(vv.MpReplicaNum), int(vv.TrashRemainingDays),
				int(vv.DefaultStoreMode), vv.FollowerRead, vv.VolWriteMutexEnable, vv.NearRead, vv.Authenticate, vv.EnableToken, vv.AutoRepair,
				vv.ForceROW, vv.IsSmart, vv.EnableWriteCache, vv.ReuseMP, calcAuthKey(vv.Owner), vv.ZoneName, optLayout, strings.Join(smartRules, ","), uint8(vv.OSSBucketPolicy), uint8(vv.CrossRegionHAType), vv.ExtentCacheExpireSec, vv.CompactTag,
				vv.DpFolReadDelayConfig.DelaySummaryInterval, vv.FolReadHostWeight, vv.TrashCleanInterval, vv.BatchDelInodeCnt, vv.DelInodeInterval, vv.UmpCollectWay,
				vv.TrashCleanDuration, vv.TrashCleanMaxCount, vv.EnableBitMapAllocator, vv.CursorSkipStep)
			if err != nil {
				return
			}
			stdout("Volume configuration has been set successfully.\n")
			return
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validVols(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	cmd.Flags().Uint64Var(&optCapacity, CliFlagCapacity, 0, "Specify volume capacity [Unit: GB]")
	cmd.Flags().IntVar(&optReplicas, CliFlagReplicas, 0, "Specify data partition replicas number")
	cmd.Flags().IntVar(&optMpReplicas, CliFlagMpReplicas, 0, "Specify meta partition replicas number")
	cmd.Flags().IntVar(&optTrashDays, CliFlagTrashDays, -1, "Specify trash remaining days")
	cmd.Flags().StringVar(&optFollowerRead, CliFlagEnableFollowerRead, "", "Enable read from replica follower")
	cmd.Flags().Int64Var(&optFolReadDelayInterval, CliFlagFollReadDelayInterval, -1, "Specify host delay update interval [Unit: second]")
	cmd.Flags().IntVar(&optLowestDelayHostWeight, CliFlagFollReadHostWeight, 0, "assign weight for the lowest delay host when enable FollowerRead,(0,100]")
	cmd.Flags().StringVar(&optNearRead, CliFlagEnableNearRead, "", "Enable read from ip near replica")
	cmd.Flags().StringVar(&optForceROW, CliFlagEnableForceROW, "", "Enable only row instead of overwrite")
	cmd.Flags().StringVar(&optEnableWriteCache, CliFlagEnableWriteCache, "", "Enable write back cache when mounting FUSE")
	cmd.Flags().StringVar(&optCrossRegionHAType, CliFlagEnableCrossRegionHA, "", "Set cross region high available type(0 for default, 1 for quorum)")
	cmd.Flags().StringVar(&optAuthenticate, CliFlagAuthenticate, "", "Enable authenticate")
	cmd.Flags().StringVar(&optEnableToken, CliFlagEnableToken, "", "ReadOnly/ReadWrite token validation for fuse client")
	cmd.Flags().StringVar(&optZoneName, CliFlagZoneName, "", "Specify volume zone name")
	cmd.Flags().BoolVarP(&optYes, "yes", "y", false, "Answer yes for all questions")
	cmd.Flags().StringVar(&optAutoRepair, CliFlagAutoRepair, "", "Enable auto balance partition distribution according to zoneName")
	cmd.Flags().StringVar(&optVolWriteMutex, CliFlagVolWriteMutexEnable, "", "Enable only one client have volume write permission")
	cmd.Flags().StringVar(&optBucketPolicy, CliFlagOSSBucketPolicy, "", "Set bucket access policy for S3(0 for private 1 for public-read)")
	cmd.Flags().Int64Var(&optExtentCacheExpireSec, CliFlagExtentCacheExpireSec, 0, "Specify the expiration second of the extent cache (-1 means never expires)")
	cmd.Flags().StringVar(&optLayout, CliFlagMetaLayout, "", "specify volume meta layout num1,num2 [num1:rocks db mp percent, num2:rocks db replicas percent]")
	cmd.Flags().IntVar(&optStoreMode, CliFlagStoreMode, 0, "specify volume default store mode [1:Mem, 2:Rocks]")
	cmd.Flags().StringVar(&optIsSmart, CliFlagIsSmart, "", "Enable the smart vol or not")
	cmd.Flags().StringSliceVar(&smartRules, CliSmartRulesMode, []string{}, "Specify volume smart rules")
	cmd.Flags().StringVar(&optCompactTag, CliFlagCompactTag, "", "Specify volume compact")
	cmd.Flags().IntVar(&optTrashCleanInterval, CliOpVolTrashCleanInterval, -1, "specify trash clean interval, unit:min")
	cmd.Flags().IntVar(&optBatchDelInodeCnt, CliOpVolBatchDelInodeCnt, -1, "specify batch del inode count")
	cmd.Flags().IntVar(&optDelInodeInterval, CliOpVolDelInodeInterval, -1, "specify del inode interval, unit:ms")
	cmd.Flags().IntVar(&optUmpCollectWay, CliFlagUmpCollectWay, -1, "Set ump collect way: 0 unknown 1 file 2 jmtp client")
	cmd.Flags().StringVar(&optReuseMP, CliFlagReuseMP, "", "Reuse meta partition when add meta partition")
	cmd.Flags().StringVar(&optEnableBitMapAllocator, CliFlagBitMapAllocatorSt, "", "enable/disable bit map allocator")
	cmd.Flags().Int32Var(&optTrashCleanDuration, CliFlagTrashCleanDuration, -1, "Trash clean duration, unit:min")
	cmd.Flags().Int32Var(&optTrashCleanMaxCount, CliFlagTrashCleanMaxCount, -1, "Trash clean max count")
	cmd.Flags().Int64Var(&optCursorSkipStep, CliFlagCusorSkipStep, -1, "cursor skip step when meta partition change leader")
	return cmd
}

func newVolShrinkCapacityCmd(client *master.MasterClient) *cobra.Command {
	var (
		optCapacity   uint64
		optYes        bool
		confirmString = strings.Builder{}
		vv            *proto.SimpleVolView
	)
	var cmd = &cobra.Command{
		Use:   "shrinkCapacity [VOLUME NAME]",
		Short: "Shrink capacity of the volume",
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var volumeName = args[0]
			var isChange = false
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()

			if vv, err = client.AdminAPI().GetVolumeSimpleInfo(volumeName); err != nil {
				return
			}
			confirmString.WriteString("Volume configuration changes:\n")
			confirmString.WriteString(fmt.Sprintf("  Name                : %v\n", vv.Name))
			if optCapacity > 0 {
				isChange = true
				confirmString.WriteString(fmt.Sprintf("  Capacity            : %v GB -> %v GB\n", vv.Capacity, optCapacity))
				vv.Capacity = optCapacity
			}
			if !isChange {
				stdout("No changes has been set.\n")
				return
			}
			// ask user for confirm
			if !optYes {
				stdout(confirmString.String())
				stdout("\nConfirm (yes/no)[yes]: ")
				var userConfirm string
				_, _ = fmt.Scanln(&userConfirm)
				if userConfirm != "yes" && len(userConfirm) != 0 {
					err = fmt.Errorf("Abort by user.\n")
					return
				}
			}
			err = client.AdminAPI().ShrinkVolCapacity(vv.Name, calcAuthKey(vv.Owner), vv.Capacity)
			if err != nil {
				return
			}
			stdout("Volume capacity has been set successfully.\n")
			return
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validVols(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	cmd.Flags().Uint64Var(&optCapacity, CliFlagCapacity, 0, "Specify volume capacity [Unit: GB]")
	return cmd
}

func newVolInfoCmd(client *master.MasterClient) *cobra.Command {
	var (
		optMetaDetail bool
		optDataDetail bool
		optEcDetail   bool
	)

	var cmd = &cobra.Command{
		Use:   cmdVolInfoUse,
		Short: cmdVolInfoShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var volumeName = args[0]
			var svv *proto.SimpleVolView
			var memMpCnt, rocksMpCnt, mixMpCnt, beyondConfCnt int

			if svv, err = client.AdminAPI().GetVolumeSimpleInfo(volumeName); err != nil {
				errout("Get volume info failed:\n%v\n", err)
			}
			// print summary info
			stdout("Summary:\n%s\n", formatSimpleVolView(svv))

			// print metadata detail
			if optMetaDetail {
				var views []*proto.MetaPartitionView
				if views, err = client.ClientAPI().GetMetaPartitions(volumeName); err != nil {
					errout("Get volume metadata detail information failed:\n%v\n", err)
					os.Exit(1)
				}
				for _, view := range views {
					switch view.StoreMode {
					case proto.StoreModeMem:
						memMpCnt++
					case proto.StoreModeRocksDb:
						rocksMpCnt++
					default:
						mixMpCnt++
					}

					if len(view.Members) > int(svv.MpReplicaNum) {
						beyondConfCnt++
					}
				}
				stdout("  Mem mp count         : %v\n", memMpCnt)
				stdout("  Rocks db mp count    : %v\n", rocksMpCnt)
				stdout("  Mix mp count         : %v\n", mixMpCnt)
				stdout("  Beyond conf mp count : %v\n", beyondConfCnt)
				stdout("Meta partitions:\n")
				stdout("%v\n", metaPartitionTableHeader)
				sort.SliceStable(views, func(i, j int) bool {
					return views[i].PartitionID < views[j].PartitionID
				})
				for _, view := range views {
					stdout("%v\n", formatMetaPartitionTableRow(view))
				}
			}

			// print data detail
			if optDataDetail {
				var view *proto.DataPartitionsView
				if view, err = client.ClientAPI().GetDataPartitions(volumeName); err != nil {
					errout("Get volume data detail information failed:\n%v\n", err)
					os.Exit(1)
				}
				stdout("Data partitions:\n")
				stdout("%v\n", dataPartitionTableHeader)
				sort.SliceStable(view.DataPartitions, func(i, j int) bool {
					return view.DataPartitions[i].PartitionID < view.DataPartitions[j].PartitionID
				})
				for _, dp := range view.DataPartitions {
					stdout("%v\n", formatDataPartitionTableRow(dp))
				}
			}

			// print ec detail
			if optEcDetail {
				var view *proto.EcPartitionsView
				if view, err = client.ClientAPI().GetEcPartitions(volumeName); err != nil {
					errout("Get volume data detail information failed:\n%v\n", err)
					os.Exit(1)
				}
				stdout("ec partitions count         : %v\n", len(view.EcPartitions))
				stdout("Ec partitions:\n")
				stdout("%v\n", ecPartitionTableHeader)
				sort.SliceStable(view.EcPartitions, func(i, j int) bool {
					return view.EcPartitions[i].PartitionID < view.EcPartitions[j].PartitionID
				})
				for _, ep := range view.EcPartitions {
					stdout("%v\n", formatEcPartitionTableRow(ep))
				}
			}
			return
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validVols(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	cmd.Flags().BoolVarP(&optMetaDetail, "meta-partition", "m", false, "Display meta partition detail information")
	cmd.Flags().BoolVarP(&optDataDetail, "data-partition", "d", false, "Display data partition detail information")
	cmd.Flags().BoolVarP(&optEcDetail, "ec-partition", "e", false, "Display ec partition detail information")
	return cmd
}

const (
	cmdVolDeleteUse   = "delete [VOLUME NAME]"
	cmdVolDeleteShort = "Delete a volume from cluster"
)

func newVolDeleteCmd(client *master.MasterClient) *cobra.Command {
	var (
		optYes bool
	)
	var cmd = &cobra.Command{
		Use:   cmdVolDeleteUse,
		Short: cmdVolDeleteShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var volumeName = args[0]
			// ask user for confirm
			if !optYes {
				stdout("Delete volume [%v] (yes/no)[no]:", volumeName)
				var userConfirm string
				_, _ = fmt.Scanln(&userConfirm)
				if userConfirm != "yes" {
					stdout("Abort by user.\n")
					return
				}
			}

			var svv *proto.SimpleVolView
			if svv, err = client.AdminAPI().GetVolumeSimpleInfo(volumeName); err != nil {
				errout("Delete volume failed:\n%v\n", err)
			}

			if err = client.AdminAPI().DeleteVolume(volumeName, calcAuthKey(svv.Owner)); err != nil {
				errout("Delete volume failed:\n%v\n", err)
			}
			stdout("Delete volume success.\n")
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validVols(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	cmd.Flags().BoolVarP(&optYes, "yes", "y", false, "Answer yes for all questions")
	return cmd
}

func newVolPartitionCheckCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "partitionCheck [VOLUME NAME] [DIFF LIST(true/false)]",
		Short: "check a volume's partitions count",
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var volumeName = args[0]
			var isList = strings.ToLower(args[1])

			info, err := client.AdminAPI().GetVolumeSimpleInfo(volumeName)
			if err != nil {
				errout("get volume failed:\n%v\n", err)
				return
			}

			volume, err := client.ClientAPI().GetVolume(volumeName, calcAuthKey(info.Owner))
			if err != nil {
				errout("get volume list failed:\n%v\n", err)
				return
			}

			diffInode := func(i, v *metanode.Inode) bool {
				flag := i.Inode == v.Inode && i.Type == v.Type && i.Uid == v.Uid && i.Gid == v.Gid && i.Size == v.Size && i.Generation == v.Generation && i.CreateTime == v.CreateTime && i.ModifyTime == v.ModifyTime && i.NLink == v.NLink && i.Flag == v.Flag && i.Reserved == v.Reserved
				if !flag {
					return flag
				}

				if v.Extents.Size() != i.Extents.Size() {
					return false
				}

				extentMap := make(map[uint64]*proto.ExtentKey)

				v.Extents.Range(func(ek proto.ExtentKey) bool {
					extentMap[ek.ExtentId] = &ek
					return true
				})

				i.Extents.Range(func(ek proto.ExtentKey) bool {
					vek := extentMap[ek.ExtentId]
					return vek.ExtentId == ek.ExtentId && vek.Size == ek.Size && vek.PartitionId == ek.PartitionId && vek.FileOffset == ek.FileOffset && vek.CRC == ek.CRC && vek.ExtentOffset == ek.ExtentOffset
				})

				return flag
			}

			diffDentry := func(i, v *metanode.Dentry) bool {
				return i.Type == v.Type && i.Inode == v.Inode && i.Name == v.Name && i.ParentId == v.ParentId
			}

			for _, mp := range volume.MetaPartitions {

				var preAddr string
				flag := true
				var dentryCount, inodeCount, multipartCount float64 = -1, -1, -1
				var preAllInode map[uint64]*metanode.Inode
				var preAllDentry map[string]*metanode.Dentry

				for _, addr := range mp.Members {
					addr := strings.Split(addr, ":")[0]

					resp, err := http.Get(fmt.Sprintf("http://%s:%d/getPartitionById?pid=%d", addr, client.MetaNodeProfPort, mp.PartitionID))
					if err != nil {
						errout("get partition list failed:\n%v\n", err)
						return
					}
					all, err := ioutil.ReadAll(resp.Body)
					if err != nil {
						errout("get partition list failed:\n%v\n", err)
						return
					}

					value := make(map[string]interface{})
					err = json.Unmarshal(all, &value)
					if err != nil {
						errout("get partition info failed:\n%v\n", err)
						return
					}

					if value["code"].(float64) != float64(200) {
						errout("get partition info has err:[addr:%s , code:%d] \n", addr, value["code"])
					}

					data := value["data"].(map[string]interface{})

					if preAddr == "" {
						dentryCount, inodeCount, multipartCount = data["dentry_count"].(float64), data["inode_count"].(float64), data["multipart_count"].(float64)
						preAddr = addr
					} else {
						if dentryCount != data["dentry_count"].(float64) {
							flag = false
							errout("partition:%d dentryCount not same source:%s target:%s\n", mp.PartitionID, preAddr, addr)
						}
						if inodeCount != data["inode_count"].(float64) {
							flag = false
							errout("partition:%d inodeCount not same source:%s target:%s\n", mp.PartitionID, preAddr, addr)
						}
						if multipartCount != data["multipart_count"].(float64) {
							flag = false
							errout("partition:%d multipartCount not same source:%s target:%s\n", mp.PartitionID, preAddr, addr)
						}
					}

					if isList != "true" {
						continue
					}

					metaClient := api.NewMetaHttpClient(fmt.Sprintf("%s:%d", addr, client.MetaNodeProfPort), false)

					allInode, err := metaClient.GetAllInodes(mp.PartitionID)
					if err != nil {
						flag = false
						errout("partition:%d list inode has err:%s target:%s \n", mp.PartitionID, err.Error(), addr)
					}

					if len(preAllInode) == 0 {
						preAllInode = allInode
					} else {
						if len(preAllInode) != len(allInode) {
							errout("partition:%d list inode len not same source:%s count:%d target:%s count:%d\n", mp.PartitionID, preAddr, len(preAllInode), addr, len(allInode))
						}
						for k, v := range allInode {
							i := preAllInode[k]
							if !diffInode(v, i) {
								errout("partition:%d list inode not same %s:%v %s:%v\n", mp.PartitionID, preAddr, i, addr, v)
							}
						}
					}

					allDentry, err := metaClient.GetAllDentry(mp.PartitionID)
					if preAllDentry == nil {
						preAllDentry = allDentry
					} else {
						if len(preAllDentry) != len(allDentry) {
							errout("partition:%d list dentry len not same source:%s count:%d target:%s count:%d\n", mp.PartitionID, preAddr, len(preAllDentry), addr, len(allDentry))
						}
						for k, v := range allDentry {
							i := preAllDentry[k]
							if !diffDentry(v, i) {
								errout("partition:%d list dentry not same %s:%v %s:%v\n", mp.PartitionID, preAddr, i, addr, v)
							}
						}
					}
				}

				if flag {
					stdout("partition:%d is health."+line, mp.PartitionID)
				} else {
					stdout("partition:%d is not health."+line, mp.PartitionID)
				}
			}

			stdout("Check volume partition end is list:%v.\n", isList == "true")
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validVols(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	return cmd
}

const (
	cmdVolTransferUse   = "transfer [VOLUME NAME] [USER ID]"
	cmdVolTransferShort = "Transfer volume to another user. (Change owner of volume)"
)

func newVolTransferCmd(client *master.MasterClient) *cobra.Command {
	var optYes bool
	var optForce bool
	var cmd = &cobra.Command{
		Use:     cmdVolTransferUse,
		Short:   cmdVolTransferShort,
		Aliases: []string{"trans"},
		Args:    cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var volume = args[0]
			var userID = args[1]

			defer func() {
				if err != nil {
					errout("Transfer volume [%v] to user [%v] failed: %v\n", volume, userID, err)
				}
			}()

			// ask user for confirm
			if !optYes {
				stdout("Transfer volume [%v] to user [%v] (yes/no)[no]:", volume, userID)
				var confirm string
				_, _ = fmt.Scanln(&confirm)
				if confirm != "yes" {
					stdout("Abort by user.\n")
					return
				}
			}

			// check target user and volume
			var volSimpleView *proto.SimpleVolView
			if volSimpleView, err = client.AdminAPI().GetVolumeSimpleInfo(volume); err != nil {
				return
			}
			if volSimpleView.Status != proto.VolStNormal {
				err = fmt.Errorf("volume status abnormal")
				return
			}
			var userInfo *proto.UserInfo
			if userInfo, err = client.UserAPI().GetUserInfo(userID); err != nil {
				return
			}
			var param = proto.UserTransferVolParam{
				Volume:  volume,
				UserSrc: volSimpleView.Owner,
				UserDst: userInfo.UserID,
				Force:   optForce,
			}
			if _, err = client.UserAPI().TransferVol(&param); err != nil {
				return
			}
		},
	}
	return cmd
}

const (
	cmdVolAddDPCmdUse   = "add-dp [VOLUME] [NUMBER]"
	cmdVolAddDPCmdShort = "Create and add more data partition to a volume"
)

func newVolAddDPCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   cmdVolAddDPCmdUse,
		Short: cmdVolAddDPCmdShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var volume = args[0]
			var number = args[1]
			var err error
			defer func() {
				if err != nil {
					errout("Create data partition failed: %v\n", err)
				}
			}()
			var count int64
			if count, err = strconv.ParseInt(number, 10, 64); err != nil {
				return
			}
			if count < 1 {
				err = errors.New("number must be larger than 0")
				return
			}
			if err = client.AdminAPI().CreateDataPartition(volume, int(count)); err != nil {
				return
			}
			return
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validVols(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	return cmd
}

const (
	cmdVolSetMinRWPartitionCmdUse   = "set-min-rw-partition [VOLUME]"
	cmdVolSetMinRWPartitionCmdShort = "Set min writable dp/mp num of the volume"
)

func newVolSetMinRWPartitionCmd(client *master.MasterClient) *cobra.Command {
	var (
		optRwMpNum    int
		optRwDpNum    int
		optYes        bool
		confirmString = strings.Builder{}
		vv            *proto.SimpleVolView
	)
	var cmd = &cobra.Command{
		Use:   cmdVolSetMinRWPartitionCmdUse,
		Short: cmdVolSetMinRWPartitionCmdShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var volumeName = args[0]
			var isChange = false
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			if vv, err = client.AdminAPI().GetVolumeSimpleInfo(volumeName); err != nil {
				return
			}
			confirmString.WriteString("Volume configuration changes:\n")
			confirmString.WriteString(fmt.Sprintf("  Name                : %v\n", vv.Name))

			if optRwMpNum > 0 {
				isChange = true
				confirmString.WriteString(fmt.Sprintf("  min writable mp num : %v -> %v\n", vv.MinWritableMPNum, optRwMpNum))
				vv.MinWritableMPNum = optRwMpNum
			} else {
				confirmString.WriteString(fmt.Sprintf("  min writable mp num : %v\n", vv.MinWritableMPNum))
			}
			if optRwDpNum > 0 {
				isChange = true
				confirmString.WriteString(fmt.Sprintf("  min writable dp num : %v -> %v\n", vv.MinWritableDPNum, optRwDpNum))
				vv.MinWritableDPNum = optRwDpNum
			} else {
				confirmString.WriteString(fmt.Sprintf("  min writable dp num : %v\n", vv.MinWritableDPNum))
			}
			if !isChange {
				stdout("No changes has been set.\n")
				return
			}
			// ask user for confirm
			if !optYes {
				stdout(confirmString.String())
				stdout("\nConfirm (yes/no)[yes]: ")
				var userConfirm string
				_, _ = fmt.Scanln(&userConfirm)
				if userConfirm != "yes" && len(userConfirm) != 0 {
					err = fmt.Errorf("Abort by user.\n")
					return
				}
			}
			err = client.AdminAPI().SetVolMinRWPartition(vv.Name, vv.MinWritableMPNum, vv.MinWritableDPNum)
			if err != nil {
				return
			}
			stdout("Volume configuration has been set successfully.\n")
			return
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validVols(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	cmd.Flags().IntVar(&optRwMpNum, "minRwMP", 0, "Specify min writable mp num")
	cmd.Flags().IntVar(&optRwDpNum, "minRwDP", 0, "Specify min writable dp num")
	cmd.Flags().BoolVarP(&optYes, "yes", "y", false, "Answer yes for all questions")
	return cmd
}

func newVolConvertTaskCmd(client *master.MasterClient) *cobra.Command {
	var (
		vv     *proto.SimpleVolView
		optYes bool
	)
	var cmd = &cobra.Command{
		Use:   "convertTask [VOLUME NAME]  [start/stop]",
		Short: "start/stop volume convert task",
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var volumeName = args[0]
			var action = args[1]
			var st = proto.VolConvertStStopped

			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			if vv, err = client.AdminAPI().GetVolumeSimpleInfo(volumeName); err != nil {
				return
			}

			if action != "start" && action != "stop" {
				errout("unknown operation [%v]", action)
			}

			if !optYes {
				stdout("%s volume[%s] convert task?\n", action, volumeName)
				stdout("\nConfirm (yes/no)[yes]: ")
				var userConfirm string
				_, _ = fmt.Scanln(&userConfirm)
				if userConfirm != "yes" && len(userConfirm) != 0 {
					stdout("Abort by user.\n")
					return
				}
			}

			if action == "start" {
				st = proto.VolConvertStRunning
			}
			err = client.AdminAPI().SetVolumeConvertTaskState(volumeName, calcAuthKey(vv.Owner), int(st))
			if err != nil {
				errout("%s volume[%s] convert task failed.err[%v].\n", action, volumeName, err.Error())
			}
			stdout("%s volume[%s] convert task successfully.\n", action, volumeName)
			return
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validVols(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	cmd.Flags().BoolVarP(&optYes, "yes", "y", false, "Answer yes for all questions")

	return cmd
}

const (
	cmdVolAddMPCmdUse   = "add-mp [VOLUME]"
	cmdVolAddMPCmdShort = "Create and add more meta partition to a volume"
)

func newVolAddMPCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   cmdVolAddMPCmdUse,
		Short: cmdVolAddMPCmdShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var volume = args[0]
			var err error
			defer func() {
				if err != nil {
					errout("Create meta partition failed: %v\n", err)
				}
			}()
			if err = client.AdminAPI().CreateMetaPartition(volume, 0); err != nil {
				return
			}
			return
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validVols(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	return cmd
}

func calcAuthKey(key string) (authKey string) {
	h := md5.New()
	_, _ = h.Write([]byte(key))
	cipherStr := h.Sum(nil)
	return strings.ToLower(hex.EncodeToString(cipherStr))
}

func checkForceRowAndCompact(vv *proto.SimpleVolView, forceRowChange, compactTagChange bool) {
	if forceRowChange && !compactTagChange {
		if !vv.ForceROW && vv.CompactTag == proto.CompactOpenName {
			errout("error: force row cannot be closed when compact is opened, Please close compact first\n")
		}
		curTime := time.Now().Unix()
		if !vv.ForceROW &&
			(vv.CompactTag == proto.CompactCloseName || vv.CompactTag == proto.CompactDefaultName) &&
			(curTime-vv.CompactTagModifyTime) < proto.CompatTagClosedTimeDuration {
			errout("error: force row cannot be closed when compact is closed for less than %v minutes, now diff time %v minutes\n", proto.CompatTagClosedTimeDuration/60, (curTime-vv.CompactTagModifyTime)/60)
		}
	}

	if !forceRowChange && compactTagChange {
		if !vv.ForceROW && vv.CompactTag == proto.CompactOpenName {
			errout("error: compact cannot be opened when force row is closed, Please open force row first\n")
		}
	}

	if forceRowChange && compactTagChange {
		if !vv.ForceROW && vv.CompactTag == proto.CompactOpenName {
			errout("error: compact cannot be opened when force row is closed, Please open force row first\n")
		}
		if !vv.ForceROW &&
			(vv.CompactTag == proto.CompactCloseName || vv.CompactTag == proto.CompactDefaultName) {
			errout("error: force row cannot be closed when compact is closed for less than %v minutes, Please close compact first\n", proto.CompatTagClosedTimeDuration/60)
		}
	}
}

func newVolEcInfoCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   cmdVolEcInfoUse,
		Short: cmdVolEcInfoShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var volumeName = args[0]
			var svv *proto.SimpleVolView

			if svv, err = client.AdminAPI().GetVolumeSimpleInfo(volumeName); err != nil {
				errout("Get volume info failed:\n%v\n", err)
			}
			// print summary info
			stdout("Summary:\n%s\n", formatSimpleVolEcView(svv))
			return
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validVols(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	return cmd
}

func newVolEcSetCmd(client *master.MasterClient) *cobra.Command {
	var (
		optEcSaveTime    int64
		optEcWaitTime    int64
		optEcTimeOut     int64
		optEcRetryWait   int64
		optEcMaxUnitSize uint64
		optEcEnable      string
		confirmString    = strings.Builder{}
		vv               *proto.SimpleVolView
	)
	var cmd = &cobra.Command{
		Use:   CliOpEcSet + " [VOLUME NAME]",
		Short: cmdVolEcSetShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var volumeName = args[0]
			var isChange = false
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			if vv, err = client.AdminAPI().GetVolumeSimpleInfo(volumeName); err != nil {
				return
			}
			confirmString.WriteString("Volume configuration changes:\n")
			confirmString.WriteString(fmt.Sprintf("  Name                : %v\n", vv.Name))

			if optEcWaitTime > 0 {
				isChange = true
				confirmString.WriteString(fmt.Sprintf("  waitTime            : %v days -> %v days\n", vv.EcWaitTime, optEcWaitTime))
				vv.EcWaitTime = optEcWaitTime
			} else {
				confirmString.WriteString(fmt.Sprintf("  waitTime            : %v days -> %v days\n", vv.EcWaitTime, optEcWaitTime))
			}
			if optEcSaveTime > 0 {
				isChange = true
				confirmString.WriteString(fmt.Sprintf("  optEcSaveTime            : %v days -> %v days\n", vv.EcSaveTime, optEcSaveTime))
				vv.EcSaveTime = optEcSaveTime
			} else {
				confirmString.WriteString(fmt.Sprintf("  optEcSaveTime            : %v days -> %v days\n", vv.EcSaveTime, optEcSaveTime))
			}

			if optEcTimeOut > 0 {
				isChange = true
				confirmString.WriteString(fmt.Sprintf("  optEcTimeOut           : %v mins -> %v mins\n", vv.EcTimeOut, optEcTimeOut))
				vv.EcTimeOut = optEcTimeOut
			} else {
				confirmString.WriteString(fmt.Sprintf("  optEcTimeOut           : %v mins -> %v mins\n", vv.EcTimeOut, optEcTimeOut))
			}

			if optEcRetryWait > 0 {
				isChange = true
				confirmString.WriteString(fmt.Sprintf("  optEcRetryWait            : %v -> %v \n", vv.EcRetryWait, optEcRetryWait))
				vv.EcRetryWait = optEcRetryWait
			} else {
				confirmString.WriteString(fmt.Sprintf("  optEcRetryWait            : %v -> %v \n", vv.EcRetryWait, optEcRetryWait))
			}

			if optEcMaxUnitSize > 0 {
				isChange = true
				confirmString.WriteString(fmt.Sprintf("  optEcMaxUnitSize            : %v -> %v \n", vv.EcMaxUnitSize, optEcMaxUnitSize))
				vv.EcMaxUnitSize = optEcMaxUnitSize
			} else {
				confirmString.WriteString(fmt.Sprintf("  optEcMaxUnitSize            : %v -> %v \n", vv.EcMaxUnitSize, optEcMaxUnitSize))
			}

			if optEcEnable != "" {
				isChange = true
				var enable bool
				if enable, err = strconv.ParseBool(optEcEnable); err != nil {
					return
				}
				confirmString.WriteString(fmt.Sprintf("  EnableEc         : %v -> %v\n", formatEnabledDisabled(vv.EcEnable), formatEnabledDisabled(enable)))
				vv.EcEnable = enable
			} else {
				confirmString.WriteString(fmt.Sprintf("  EnableEc         : %v\n", formatEnabledDisabled(vv.EcEnable)))
			}

			if !isChange {
				stdout("No changes has been set.\n")
				return
			}
			err = client.AdminAPI().UpdateVolumeEcInfo(vv.Name, vv.EcEnable, int(vv.EcDataNum), int(vv.EcParityNum), vv.EcSaveTime, vv.EcWaitTime, vv.EcTimeOut, vv.EcRetryWait, vv.EcMaxUnitSize)
			if err != nil {
				return
			}
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validVols(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	cmd.Flags().StringVar(&optEcEnable, CliFlagEcEnable, "", "Enable ec")
	cmd.Flags().Int64Var(&optEcSaveTime, CliFlagEcSaveTime, 0, "Specify ec save replicate data time [Unit: min]")
	cmd.Flags().Int64Var(&optEcWaitTime, CliFlagEcWaitTime, 0, "Specify ec wait time to migrate when replica dp full[Unit: min]")
	cmd.Flags().Int64Var(&optEcTimeOut, CliFlagEcTimeOut, 0, "Specify ec timeOut [Unit: min]")
	cmd.Flags().Int64Var(&optEcRetryWait, CliFlagEcRetryWait, 0, "Specify ec migration fail retry wait time [Unit: min]")
	cmd.Flags().Uint64Var(&optEcMaxUnitSize, CliFlagEcMaxUnitSize, 0, "Specify ec max unit size (Unit: byte)")
	return cmd
}

func newVolEcPartitionsConsistency(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpCheckConsistency + " [VOLUME]",
		Short: cmdVolEcPartitions,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var volumeName = args[0]
			var epv *proto.EcPartitionsView

			if epv, err = client.ClientAPI().GetEcPartitions(volumeName); err != nil {
				errout("Get EcPartitions failed:\n%v\n", err)
			}
			for _, ep := range epv.EcPartitions {
				ep, err := client.AdminAPI().GetEcPartition("", ep.PartitionID)
				if err != nil {
					stdout("GetEcPartition(%v) err:%v\n", ep.PartitionID, err)
					continue
				}
				if ep.EcMigrateStatus != proto.FinishEC {
					continue
				}
				dp, err := client.AdminAPI().GetDataPartition("", ep.PartitionID)
				if err != nil {
					stdout("GetDataPartition(%v) err:%v\n", ep.PartitionID, err)
					continue
				}
				stdout("Start Partition(%v) Check ...", ep.PartitionID)
				errExtent, err := startCheckEcPartitionConsistency(dp, ep, client, false)
				if err != nil {
					stdoutRed("    FAIL")
					continue
				}
				if len(errExtent) == 0 {
					stdoutGreen("   PASS")
				} else {
					stdoutRed("    FAIL")
					stdout("Partition(%v) errExtent(%v)", ep.PartitionID, errExtent)
				}
			}
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validVols(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	return cmd
}

func newVolEcPartitionsChunkConsistency(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpCheckEcData + " [VOLUME]",
		Short: cmdVolEcChunkData,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var volumeName = args[0]
			var epv *proto.EcPartitionsView

			if epv, err = client.ClientAPI().GetEcPartitions(volumeName); err != nil {
				errout("Get EcPartitions failed:\n%v\n", err)
			}
			for _, ep := range epv.EcPartitions {
				ep, err := client.AdminAPI().GetEcPartition(volumeName, ep.PartitionID)
				if err != nil {
					stdout("GetEcPartition(%v) err:%v\n", ep.PartitionID, err)
					continue
				}
				if !proto.IsEcFinished(ep.EcMigrateStatus) {
					continue
				}
				stdout("Start Partition(%v) Check chunk data ...", ep.PartitionID)
				err = checkEcPartitionChunkData(ep)
				if err != nil {
					stdout("%v\n", err)
				} else {
					stdout("%v ", ep.PartitionID)
					stdoutGreen("  PASS\n")
				}
			}
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validVols(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	return cmd
}

const (
	cmdVolSetChildFileMaxCountCmdUse   = "set-child-file-max-count [VOLUME]"
	cmdVolSetChildFileMaxCountCmdShort = "set child file max count"
)

func newVolSetChildFileMaxCountCmd(client *master.MasterClient) *cobra.Command {
	var (
		optMaxCount   int
		optYes        bool
		confirmString = strings.Builder{}
		vv            *proto.SimpleVolView
	)
	var cmd = &cobra.Command{
		Use:   cmdVolSetChildFileMaxCountCmdUse,
		Short: cmdVolSetChildFileMaxCountCmdShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var volumeName = args[0]
			var isChange = false
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			if vv, err = client.AdminAPI().GetVolumeSimpleInfo(volumeName); err != nil {
				return
			}
			confirmString.WriteString("Volume configuration changes:\n")
			confirmString.WriteString(fmt.Sprintf("  Name                 : %v\n", vv.Name))

			if optMaxCount > 0 && vv.ChildFileMaxCount != uint32(optMaxCount) {
				isChange = true
				confirmString.WriteString(fmt.Sprintf("  child file max count : %v -> %v\n", vv.ChildFileMaxCount, optMaxCount))
				vv.ChildFileMaxCount = uint32(optMaxCount)
			} else {
				confirmString.WriteString(fmt.Sprintf("  child file max count : %v\n", vv.ChildFileMaxCount))
			}
			if !isChange {
				stdout("No changes has been set.\n")
				return
			}
			// ask user for confirm
			if !optYes {
				stdout(confirmString.String())
				stdout("\nConfirm (yes/no)[yes]: ")
				var userConfirm string
				_, _ = fmt.Scanln(&userConfirm)
				if userConfirm != "yes" && len(userConfirm) != 0 {
					err = fmt.Errorf("Abort by user.\n")
					return
				}
			}
			_, err = client.AdminAPI().SetVolChildFileMaxCount(vv.Name, vv.ChildFileMaxCount)
			if err != nil {
				return
			}
			stdout("Volume configuration has been set successfully.\n")
			return
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validVols(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	cmd.Flags().IntVar(&optMaxCount, "maxCount", 0, "child file max count")
	cmd.Flags().BoolVarP(&optYes, "yes", "y", false, "Answer yes for all questions")
	return cmd
}

var (
	optParallelMpCnt, optParallelInodeCnt int64
	mistakeDeleteEKInfoPattern            = "%-20v    %-20v\n"
	mistakeDeleteEKInfoHeader             = fmt.Sprintf(mistakeDeleteEKInfoPattern, "DP ID", "EXTENTS ID")
)

type ExtentInfo struct {
	DataPartitionID uint64
	ExtentID        uint64
}

func newVolCheckEKDeletedByMistake(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "check-mistake-delete-eks" + " [VOLUME]",
		Short: "check ek deleted by mistake, ek exist in meta data but not exist in data node",
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var volumeName = args[0]
			var dataExtentsMap, metaExtentsMap, ekDeletedByMistake map[uint64]*bitset.ByteSliceBitSet
			defer func() {
				if err != nil {
					errout(err.Error())
				}
			}()
			if metaExtentsMap, err = getExtentsByMPs(client, volumeName); err != nil {
				err = fmt.Errorf("volume: %s , meta failed: %s", volumeName, err.Error())
				return
			}

			if dataExtentsMap, err = getExtentsByDPs(client, volumeName); err != nil {
				err = fmt.Errorf("volume: %s , data failed: %s", volumeName, err.Error())
				return
			}

			//first
			firstCheckResult := checkEKsDeletedByMistake(dataExtentsMap, metaExtentsMap)

			//re get meta data
			if metaExtentsMap, err = getExtentsByMPs(client, volumeName); err != nil {
				err = fmt.Errorf("volume: %s , meta failed: %s", volumeName, err.Error())
				return
			}

			//recheck
			ekDeletedByMistake = reCheckEKsDeletedByMistake(firstCheckResult, metaExtentsMap)
			if len(ekDeletedByMistake) == 0 {
				fmt.Printf("volume %s not exist mistake delete eks\n", volumeName)
				return
			}
			fmt.Printf("volume %s mistake delete ek result:\n", volumeName)
			formatDeletedEKByMistake(ekDeletedByMistake)
			errout("volume %s has mistake delete ek, please check!", volumeName)
			return

		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validVols(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	cmd.Flags().Int64Var(&optParallelMpCnt, "parallel-mp", 5, "mp parallel count, default 5")
	cmd.Flags().Int64Var(&optParallelInodeCnt, "parallel-inode", 10, "inode info parallel count, default is 10")
	return cmd
}

func formatDeletedEKByMistake(eks map[uint64]*bitset.ByteSliceBitSet) {
	fmt.Printf(mistakeDeleteEKInfoHeader)
	for dpID, extentIDBitSet := range eks {
		var extentsStr = make([]string, 0, extentIDBitSet.Cap())
		for index := 0; index <= extentIDBitSet.MaxNum(); index++ {
			if extentIDBitSet.Get(index) {
				extentsStr = append(extentsStr, fmt.Sprintf("%v", index))
			}
		}
		if len(extentsStr) == 0 {
			continue
		}
		log.LogInfof("dp is:%v, extents id:%s", dpID, strings.Join(extentsStr, ","))
		fmt.Printf(mistakeDeleteEKInfoPattern, dpID, strings.Join(extentsStr, ","))
	}
}

func checkEKsDeletedByMistake(dataExtentsMap, metaExtentsMap map[uint64]*bitset.ByteSliceBitSet) (result map[uint64]*bitset.ByteSliceBitSet) {
	result = make(map[uint64]*bitset.ByteSliceBitSet, len(metaExtentsMap))
	for dpid, metaEKsBitSet := range metaExtentsMap {
		if _, ok := dataExtentsMap[dpid]; !ok {
			result[dpid] = metaEKsBitSet
			continue
		}
		r := metaEKsBitSet.Xor(dataExtentsMap[dpid]).And(metaEKsBitSet)
		if r.IsNil() {
			continue
		}
		result[dpid] = r
	}
	return
}

func reCheckEKsDeletedByMistake(firstResult, metaExtentsMap map[uint64]*bitset.ByteSliceBitSet) (result map[uint64]*bitset.ByteSliceBitSet) {
	result = make(map[uint64]*bitset.ByteSliceBitSet, 0)
	for dpID, extentsID := range firstResult {
		if _, ok := metaExtentsMap[dpID]; !ok {
			log.LogInfof("reCheckEKsDeletedByMistake dpID:%v, eks:%v not exist in meta data", dpID, extentsID)
			continue
		}
		r := extentsID.And(metaExtentsMap[dpID])
		if r.IsNil() {
			continue
		}
		result[dpID] = r

	}
	return
}

func getExtentsByMPs(client *master.MasterClient, volumeName string) (
	metaExtentsMap map[uint64]*bitset.ByteSliceBitSet, err error) {
	metaExtentsMap = make(map[uint64]*bitset.ByteSliceBitSet, 0)
	var mps []*proto.MetaPartitionView
	mps, err = client.ClientAPI().GetPhysicalMetaPartitions(volumeName)
	if err != nil {
		err = fmt.Errorf("get volume(%s) metapartitions failed:%v", volumeName, err)
		return
	}

	errorCh := make(chan error, len(mps))
	defer func() {
		close(errorCh)
	}()

	var resultWaitGroup sync.WaitGroup
	extCh := make(chan *ExtentInfo, 1024)
	resultWaitGroup.Add(1)
	go func() {
		defer resultWaitGroup.Done()
		for {
			e, ok := <-extCh
			if e == nil && !ok {
				break
			}
			if e == nil {
				log.LogInfof("receive nil")
				continue
			}
			log.LogInfof("receive dpid:%v, extent id:%v", e.DataPartitionID, e.ExtentID)
			if _, ok := metaExtentsMap[e.DataPartitionID]; !ok {
				metaExtentsMap[e.DataPartitionID] = bitset.NewByteSliceBitSet()
			}
			metaExtentsMap[e.DataPartitionID].Set(int(e.ExtentID))
			if !metaExtentsMap[e.DataPartitionID].Get(int(e.ExtentID)) {
				log.LogErrorf("set dp id:%v extent id:%v to bit set failed", e.DataPartitionID, e.ExtentID)
			}
		}
	}()

	mpIDCh := make(chan uint64, 512)
	go func() {
		defer close(mpIDCh)
		for _, mp := range mps {
			mpIDCh <- mp.PartitionID
		}
	}()

	var wg sync.WaitGroup
	for index := 0; index < int(optParallelMpCnt); index++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				mpID, ok := <-mpIDCh
				if !ok {
					break
				}

				log.LogInfof("volume:%s, mp:%v", volumeName, mpID)
				metaPartitionInfo, errGetMPInfo := client.ClientAPI().GetMetaPartition(mpID, "")
				if errGetMPInfo != nil {
					errorCh <- errGetMPInfo
					log.LogErrorf("action[getExtentsByMPs] get cluster volume[%s] mp[%v] info failed: %v",
						volumeName, mpID, errGetMPInfo)
					continue
				}

				if len(metaPartitionInfo.Replicas) == 0 {
					log.LogErrorf("action[getExtentsByMPs] volume[%s] mp[%v] replica count is 0",
						volumeName, mpID)
					errorCh <- fmt.Errorf("volume[%s] mp[%v] replica count is 0", volumeName, mpID)
					continue
				}

				addrs := make([]string, 0, len(metaPartitionInfo.Replicas))
				for _, replica := range metaPartitionInfo.Replicas {
					if replica.IsLeader {
						addrs = append(addrs, replica.Addr)
					}
				}

				if len(addrs) == 0 {
					var (
						maxInodeCount uint64 = 0
						leaderAddr           = ""
					)

					for _, replica := range metaPartitionInfo.Replicas {
						if replica.InodeCount >= maxInodeCount {
							maxInodeCount = replica.InodeCount
							leaderAddr = replica.Addr
						}
					}

					addrs = append(addrs, leaderAddr)
				}

				for _, replica := range metaPartitionInfo.Replicas {
					if replica.Addr != addrs[0] {
						addrs = append(addrs, replica.Addr)
					}
				}

				var errorInfo error
				for _, addr := range addrs {
					if addr == "" {
						continue
					}
					errorInfo = getExtentsFromMetaPartition(mpID, addr, extCh)
					if errorInfo == nil {
						break
					} else {
						log.LogInfof("action[getExtentsByMPs] get volume [%s] extent id list from mp(%s:%v) failed:%v",
							volumeName, addr, mpID, errorInfo)
					}
				}
				if errorInfo != nil {
					errorCh <- errorInfo
					log.LogErrorf("action[getExtentsByMPs] get volume [%s] extent id list "+
						"from mp[%v] failed: %v", volumeName, mpID, errorInfo)
					continue
				}
			}
		}()
	}
	wg.Wait()
	close(extCh)
	resultWaitGroup.Wait()

	select {
	case e := <-errorCh:
		//meta info must be complete
		log.LogErrorf("get extent id list from meta partition failed:%v", e)
		err = errors.NewErrorf("get extent id list from meta partition failed:%v", e)
		return
	default:
	}
	log.LogInfof("meta extent map count:%v", len(metaExtentsMap))
	return
}

func getExtentsFromMetaPartition(mpId uint64, leaderAddr string, ExtentInfoCh chan *ExtentInfo) (err error) {
	leaderInfo := strings.Split(leaderAddr, ":")
	if len(leaderInfo) < 2 {
		return fmt.Errorf("error leader addr(%s)", leaderAddr)
	}
	host := leaderInfo[0] + ":" + strconv.Itoa(int(client.MetaNodeProfPort))
	metaHttpClient := meta.NewMetaHttpClient(host, false)
	var (
		inodesID    []uint64
		allInodeIDs *proto.MpAllInodesId
		wg          sync.WaitGroup
	)
	allInodeIDs, err = metaHttpClient.ListAllInodesId(mpId, 0, 0, 0)
	if err != nil {
		log.LogErrorf("action[getExtentsFromMetaPartition] get mp[%v] all inode info failed:%v", mpId, err)
		return
	}

	log.LogInfof("mp id:%v, inode count:%v", mpId, len(allInodeIDs.Inodes))

	if len(allInodeIDs.Inodes) != 0 {
		inodesID = append(inodesID, allInodeIDs.Inodes...)
	}

	inodeIDCh := make(chan uint64, 256)
	go func() {
		for _, inoID := range inodesID {
			inodeIDCh <- inoID
		}
		close(inodeIDCh)
	}()

	errorCh := make(chan error, len(inodesID))
	defer func() {
		close(errorCh)
	}()
	for i := 0; i < int(optParallelInodeCnt); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				inodeID, ok := <-inodeIDCh
				if !ok {
					break
				}

				getExtentsResp, errInfo := metaHttpClient.GetExtentKeyByInodeId(mpId, inodeID)
				if errInfo != nil {
					errorCh <- errInfo
					log.LogErrorf("action[getExtentsFromMetaPartition] get mp[%v] extent "+
						"key by inode id[%v] failed: %v", mpId, inodeID, errInfo)
					continue
				}
				log.LogInfof("inode id:%v, eks cnt:%v, eks:%v", inodeID, len(getExtentsResp.Extents), getExtentsResp.Extents)
				for _, ek := range getExtentsResp.Extents {
					ExtentInfoCh <- &ExtentInfo{
						DataPartitionID: ek.PartitionId,
						ExtentID:        ek.ExtentId,
					}
				}
			}
		}()
	}
	wg.Wait()

	select {
	case <-errorCh:
		err = errors.NewErrorf("get extent key by inode id failed")
		return
	default:
	}
	return
}

func getExtentsByDPs(client *master.MasterClient, volumeName string) (dataExtentsMap map[uint64]*bitset.ByteSliceBitSet, err error) {
	var dpsView *proto.DataPartitionsView
	dpsView, err = client.ClientAPI().GetDataPartitions(volumeName)
	if err != nil {
		log.LogErrorf("action[getExtentsByDPs] get volume[%s] data partition failed: %v", volumeName, err)
		return
	}

	dataExtentsMap = make(map[uint64]*bitset.ByteSliceBitSet, len(dpsView.DataPartitions))
	var extentsID []uint64
	for _, dp := range dpsView.DataPartitions {
		if dp == nil {
			continue
		}
		extentsID, err = getExtentsByDataPartition(dp.PartitionID, dp.Hosts[0])
		if err != nil {
			log.LogErrorf("action[getExtentsByDPs] get volume[%s] extent id list from dp[%v] failed: %v",
				volumeName, dp.PartitionID, err)
			return
		}
		if len(extentsID) == 0 {
			log.LogErrorf("action[getExtentsByDPs] get volume[%s] dp[%v] extents count is 0",
				volumeName, dp.PartitionID)
			return
		}
		bitSet := bitset.NewByteSliceBitSet()
		for _, extentID := range extentsID {
			bitSet.Set(int(extentID))
		}
		dataExtentsMap[dp.PartitionID] = bitSet
	}
	log.LogInfof("volume:%s, data extents map count:%v", volumeName, len(dataExtentsMap))
	return
}

func getExtentsByDataPartition(dpId uint64, dataNodeAddr string) (extentsID []uint64, err error) {
	var dpView *DataPartition
	dpView, err = getExtentsByDp(dpId, dataNodeAddr)
	if err != nil {
		return
	}
	extentsID = make([]uint64, 0, len(dpView.Files))
	for _, file := range dpView.Files {
		extentsID = append(extentsID, file[storage.FileID])
	}
	return
}
