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
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/chubaofs/chubaofs/util/errors"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/master"
	"github.com/spf13/cobra"
)

const (
	cmdVolUse   = "volume [COMMAND]"
	cmdVolShort = "Manage cluster volumes"
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
	)
	return cmd
}

const (
	cmdVolListUse   = "list"
	cmdVolListShort = "List cluster volumes"
)

func newVolListCmd(client *master.MasterClient) *cobra.Command {
	var optKeyword string
	var cmd = &cobra.Command{
		Use:     cmdVolListUse,
		Short:   cmdVolListShort,
		Aliases: []string{"ls"},
		Run: func(cmd *cobra.Command, args []string) {
			var vols []*proto.VolInfo
			var err error
			defer func() {
				if err != nil {
					errout("List cluster volume failed:\n%v\n", err)
					os.Exit(1)
				}
			}()
			if vols, err = client.AdminAPI().ListVols(optKeyword); err != nil {
				return
			}
			stdout("%v\n", volumeInfoTableHeader)
			for _, vol := range vols {
				stdout("%v\n", formatVolInfoTableRow(vol))
			}
		},
	}
	cmd.Flags().StringVar(&optKeyword, "keyword", "", "Specify keyword of volume name to filter")
	return cmd
}

const (
	cmdVolCreateUse             = "create [VOLUME NAME] [USER ID]"
	cmdVolCreateShort           = "Create a new volume"
	cmdVolDefaultMPCount        = 3
	cmdVolDefaultDPSize         = 120
	cmdVolDefaultCapacity       = 10 // 100GB
	cmdVolDefaultReplicas       = 3
	cmdVolDefaultFollowerReader = true
)

func newVolCreateCmd(client *master.MasterClient) *cobra.Command {
	var optMPCount int
	var optDPSize uint64
	var optCapacity uint64
	var optReplicas int
	var optFollowerRead bool
	var optYes bool
	var cmd = &cobra.Command{
		Use:   cmdVolCreateUse,
		Short: cmdVolCreateShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var volumeName = args[0]
			var userID = args[1]

			// ask user for confirm
			if !optYes {
				stdout("Create a new volume:\n")
				stdout("  Name                : %v\n", volumeName)
				stdout("  Owner               : %v\n", userID)
				stdout("  Dara partition size : %v GB\n", optDPSize)
				stdout("  Meta partition count: %v\n", optMPCount)
				stdout("  Capacity            : %v GB\n", optCapacity)
				stdout("  Replicas            : %v\n", optReplicas)
				stdout("  Allow follower read : %v\n", formatEnabledDisabled(optFollowerRead))
				stdout("\nConfirm (yes/no)[yes]: ")
				var userConfirm string
				_, _ = fmt.Scanln(&userConfirm)
				if userConfirm != "yes" && len(userConfirm) != 0 {
					stdout("Abort by user.\n")
					return
				}
			}

			err = client.AdminAPI().CreateVolume(
				volumeName, userID, optMPCount, optDPSize,
				optCapacity, optReplicas, optFollowerRead)
			if err != nil {
				errout("Create volume failed case:\n%v\n", err)
				os.Exit(1)
			}
			stdout("Create volume success.\n")
			return
		},
	}
	cmd.Flags().IntVar(&optMPCount, "mp-count", cmdVolDefaultMPCount, "Specify init meta partition count")
	cmd.Flags().Uint64Var(&optDPSize, "dp-size", cmdVolDefaultDPSize, "Specify size of data partition size [Unit: GB]")
	cmd.Flags().Uint64Var(&optCapacity, "capacity", cmdVolDefaultCapacity, "Specify volume capacity [Unit: GB]")
	cmd.Flags().IntVar(&optReplicas, "replicas", cmdVolDefaultReplicas, "Specify volume replicas number")
	cmd.Flags().BoolVar(&optFollowerRead, "follower-read", cmdVolDefaultFollowerReader, "Enable read form replica follower")
	cmd.Flags().BoolVarP(&optYes, "yes", "y", false, "Answer yes for all questions")
	return cmd
}

const (
	cmdVolInfoUse   = "info [VOLUME NAME]"
	cmdVolInfoShort = "Show volume information"
)

func newVolInfoCmd(client *master.MasterClient) *cobra.Command {
	var optMetaDetail bool
	var optDataDetail bool
	var cmd = &cobra.Command{
		Use:   cmdVolInfoUse,
		Short: cmdVolInfoShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var volumeName = args[0]
			var svv *proto.SimpleVolView
			if svv, err = client.AdminAPI().GetVolumeSimpleInfo(volumeName); err != nil {
				errout("Get volume info failed:\n%v\n", err)
				os.Exit(1)
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
	var optYes bool
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
				os.Exit(1)
			}

			if err = client.AdminAPI().DeleteVolume(volumeName, calcAuthKey(svv.Owner)); err != nil {
				errout("Delete volume failed:\n%v\n", err)
				os.Exit(1)
			}
			stdout("Delete volume success.\n")
		},
	}
	cmd.Flags().BoolVarP(&optYes, "yes", "y", false, "Answer yes for all questions")
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
					os.Exit(1)
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
	cmd.Flags().BoolVarP(&optYes, "yes", "y", false, "Answer yes for all questions")
	cmd.Flags().BoolVarP(&optForce, "force", "f", false, "Force transfer without current owner check")
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
					os.Exit(1)
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
	}
	return cmd
}

func calcAuthKey(key string) (authKey string) {
	h := md5.New()
	_, _ = h.Write([]byte(key))
	cipherStr := h.Sum(nil)
	return strings.ToLower(hex.EncodeToString(cipherStr))
}
