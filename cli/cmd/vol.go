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
	"time"

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
		newVolPartitionCheckCmd(client),
		newVolSetMinRWPartitionCmd(client),
		newVolConvertTaskCmd(client),
		newVolAddMPCmd(client),
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
	cmdVolDefaultCrossRegionHA  = 0
	cmdVolDefaultZoneName       = "default"
	cmdVolDefaultStoreMode      = int(proto.StoreModeMem)
	cmdVolDefMetaLayout         = "0,0"
)

func newVolCreateCmd(client *master.MasterClient) *cobra.Command {
	var optMPCount int
	var optDPSize uint64
	var optCapacity uint64
	var optReplicas int
	var optMpReplicas int
	var optFollowerRead bool
	var optForceROW bool
	var optCrossRegionHAType uint8
	var optAutoRepair bool
	var optVolWriteMutex bool
	var optYes bool
	var optZoneName string
	var optTrashDays int
	var optStoreMode int
	var optLayout string
	var optIsSmart bool
	var smartRules []string
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
				stdout("  Allow follower read : %v\n", formatEnabledDisabled(optFollowerRead))
				stdout("  Force ROW           : %v\n", formatEnabledDisabled(optForceROW))
				stdout("  Cross Region HA     : %s\n", proto.CrossRegionHAType(optCrossRegionHAType))
				stdout("  Auto repair         : %v\n", formatEnabledDisabled(optAutoRepair))
				stdout("  Volume write mutex  : %v\n", formatEnabledDisabled(optVolWriteMutex))

				stdout("  ZoneName            : %v\n", optZoneName)
				stdout("  Store mode          : %v\n", storeMode.Str())
				stdout("  Meta layout         : %v - %v\n", num1, num2)
				stdout("  Smart Enable       : %s\n", formatEnabledDisabled(optIsSmart))
				stdout("  Smart Rules         : %v\n", strings.Join(smartRules, ","))
				stdout("\nConfirm (yes/no)[yes]: ")
				var userConfirm string
				_, _ = fmt.Scanln(&userConfirm)
				if userConfirm != "yes" && len(userConfirm) != 0 {
					stdout("Abort by user.\n")
					return
				}
			}

			err = client.AdminAPI().CreateVolume(volumeName, userID, optMPCount, optDPSize, optCapacity, optReplicas,
				optMpReplicas, optTrashDays, optStoreMode, optFollowerRead, optAutoRepair, optVolWriteMutex, optForceROW, optIsSmart,
				optZoneName, optLayout, strings.Join(smartRules, ","), optCrossRegionHAType)
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
	cmd.Flags().BoolVar(&optForceROW, CliFlagEnableForceROW, cmdVolDefaultForceROW, "Use ROW instead of overwrite")
	cmd.Flags().Uint8Var(&optCrossRegionHAType, CliFlagEnableCrossRegionHA, cmdVolDefaultCrossRegionHA,
		"Set cross region high available type(0 for default, 1 for quorum)")
	cmd.Flags().BoolVar(&optAutoRepair, CliFlagAutoRepair, false, "Enable auto balance partition distribution according to zoneName")
	cmd.Flags().BoolVar(&optVolWriteMutex, CliFlagVolWriteMutexEnable, false, "Enable only one client have volume exclusive write permission")
	cmd.Flags().StringVar(&optZoneName, CliFlagZoneName, cmdVolDefaultZoneName, "Specify volume zone name")
	cmd.Flags().BoolVarP(&optYes, "yes", "y", false, "Answer yes for all questions")
	cmd.Flags().IntVar(&optStoreMode, CliFlagStoreMode, cmdVolDefaultStoreMode, "Specify volume store mode[1:Mem, 2:RocksDb]")
	cmd.Flags().StringVar(&optLayout, CliFlagMetaLayout, cmdVolDefMetaLayout, "Specify volume mp layout num1,num2 [num1:rocks db mp percent, num2:rocks db replica percent]")
	cmd.Flags().BoolVar(&optIsSmart, CliFlagIsSmart, cmdVolDefaultIsSmart, "Enable the smart vol or not")
	cmd.Flags().StringSliceVar(&smartRules, CliSmartRulesMode, []string{}, "Specify volume smart rules")
	return cmd
}

const (
	cmdVolInfoUse   = "info [VOLUME NAME]"
	cmdVolInfoShort = "Show volume information"
	cmdVolSetShort  = "Set configuration of the volume"
)

func newVolSetCmd(client *master.MasterClient) *cobra.Command {
	var (
		optCapacity             uint64
		optReplicas             int
		optMpReplicas           int
		optTrashDays            int
		optStoreMode            int
		optFollowerRead         string
		optNearRead             string
		optForceROW             string
		optAuthenticate         string
		optEnableToken          string
		optAutoRepair           string
		optBucketPolicy         string
		optCrossRegionHAType    string
		optZoneName             string
		optLayout               string
		optExtentCacheExpireSec int64
		optYes                  bool
		confirmString           = strings.Builder{}
		vv                      *proto.SimpleVolView
		optIsSmart              string
		smartRules              []string
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
			if optNearRead != "" {
				isChange = true
				var enable bool
				if enable, err = strconv.ParseBool(optNearRead); err != nil {
					return
				}
				confirmString.WriteString(fmt.Sprintf("  Allow near read : %v -> %v\n", formatEnabledDisabled(vv.NearRead), formatEnabledDisabled(enable)))
				vv.NearRead = enable
			} else {
				confirmString.WriteString(fmt.Sprintf("  Allow near read : %v\n", formatEnabledDisabled(vv.NearRead)))
			}
			if optForceROW != "" {
				isChange = true
				var enable bool
				if enable, err = strconv.ParseBool(optForceROW); err != nil {
					return
				}
				confirmString.WriteString(fmt.Sprintf("  Force ROW           : %v -> %v\n", formatEnabledDisabled(vv.ForceROW), formatEnabledDisabled(enable)))
				vv.ForceROW = enable
			} else {
				confirmString.WriteString(fmt.Sprintf("  Force ROW           : %v\n", formatEnabledDisabled(vv.ForceROW)))
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
				confirmString.WriteString(fmt.Sprintf("  smart:  %v -> %v\n", vv.IsSmart, enable))
				vv.IsSmart = enable
			} else {
				confirmString.WriteString(fmt.Sprintf("  smart          :  %v\n", vv.IsSmart))
			}
			if len(smartRules) != 0 {
				isChange = true
				confirmString.WriteString(fmt.Sprintf("  SmartRules          :  %s -> %s\n", strings.Join(vv.SmartRules, ","), strings.Join(smartRules, ",")))
				vv.SmartRules = smartRules
			} else {
				confirmString.WriteString(fmt.Sprintf("  SmartRules          :  %s\n", strings.Join(vv.SmartRules, ",")))
			}
			if err != nil {
				return
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
			err = client.AdminAPI().UpdateVolume(vv.Name, vv.Capacity, int(vv.DpReplicaNum), int(vv.MpReplicaNum), int(vv.TrashRemainingDays),
				int(vv.DefaultStoreMode), vv.FollowerRead, vv.NearRead, vv.Authenticate, vv.EnableToken, vv.AutoRepair,
				vv.ForceROW, vv.IsSmart, calcAuthKey(vv.Owner), vv.ZoneName, optLayout, strings.Join(smartRules, ","), uint8(vv.OSSBucketPolicy), uint8(vv.CrossRegionHAType), vv.ExtentCacheExpireSec)
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
	cmd.Flags().StringVar(&optNearRead, CliFlagEnableNearRead, "", "Enable read from ip near replica")
	cmd.Flags().StringVar(&optForceROW, CliFlagEnableForceROW, "", "Enable only row instead of overwrite")
	cmd.Flags().StringVar(&optCrossRegionHAType, CliFlagEnableCrossRegionHA, "",
		"Set cross region high available type(0 for default, 1 for quorum)")
	cmd.Flags().StringVar(&optAuthenticate, CliFlagAuthenticate, "", "Enable authenticate")
	cmd.Flags().StringVar(&optEnableToken, CliFlagEnableToken, "", "ReadOnly/ReadWrite token validation for fuse client")
	cmd.Flags().StringVar(&optZoneName, CliFlagZoneName, "", "Specify volume zone name")
	cmd.Flags().BoolVarP(&optYes, "yes", "y", false, "Answer yes for all questions")
	cmd.Flags().StringVar(&optAutoRepair, CliFlagAutoRepair, "", "Enable auto balance partition distribution according to zoneName")
	cmd.Flags().StringVar(&optBucketPolicy, CliFlagOSSBucketPolicy, "", "Set bucket access policy for S3(0 for private 1 for public-read)")
	cmd.Flags().Int64Var(&optExtentCacheExpireSec, CliFlagExtentCacheExpireSec, 0, "Specify the expiration second of the extent cache (-1 means never expires)")
	cmd.Flags().StringVar(&optLayout, CliFlagMetaLayout, "", "specify volume meta layout num1,num2 [num1:rocks db mp percent, num2:rocks db replicas percent]")
	cmd.Flags().IntVar(&optStoreMode, CliFlagStoreMode, 0, "specify volume default store mode [1:Mem, 2:Rocks]")
	cmd.Flags().StringVar(&optIsSmart, CliFlagIsSmart, "", "Enable the smart vol or not")
	cmd.Flags().StringSliceVar(&smartRules, CliSmartRulesMode, []string{}, "Specify volume smart rules")
	return cmd
}
func newVolInfoCmd(client *master.MasterClient) *cobra.Command {
	var (
		optMetaDetail bool
		optDataDetail bool
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
			if volSimpleView.Status != 0 {
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
