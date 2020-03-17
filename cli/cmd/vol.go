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
	"strings"

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
		Use:   cmdVolListUse,
		Short: cmdVolListShort,
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
			stdout("\n[Volumes]\n")
			stdout("\n%v\n", volumeInfoTableHeader)
			for _, vol := range vols {
				stdout("%v\n", formatVolInfoTableRow(vol))
			}
			stdout("\n")
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
			stdout("Create a new volume:\n")
			stdout("  Name                : %v\n", volumeName)
			stdout("  Owner               : %v\n", userID)
			stdout("  Dara partition size : %v GB\n", optDPSize)
			stdout("  Meta partition count: %v\n", optMPCount)
			stdout("  Capacity            : %v GB\n", optCapacity)
			stdout("  Replicas            : %v\n", optReplicas)
			stdout("  Allow follower read : %v\n", formatEnabledDisabled(optFollowerRead))

			// ask user for confirm
			if !optYes {
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
	cmdVolInfoUse               = "info [VOLUME NAME]"
	cmdVolInfoShort             = "Show volume information"
	cmdVolInfoDefaultMetaDetail = false
	cmdVolInfoDefaultDataDetail = false
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
			stdout("\n[Summary]\n%s\n", formatSimpleVolView(svv))

			// print metadata detail
			if optMetaDetail {
				var views []*proto.MetaPartitionView
				if views, err = client.ClientAPI().GetMetaPartitions(volumeName); err != nil {
					errout("Get volume metadata detail information failed:\n%v\n", err)
					os.Exit(1)
				}
				stdout("\n[Metadata detail]\n")
				stdout("%v\n", metaPartitionTableHeader)
				sort.SliceStable(views, func(i, j int) bool {
					return views[i].PartitionID < views[j].PartitionID
				})
				for _, view := range views {
					stdout("%v\n", formatMetaPartitionTableRow(view))
				}
				stdout("\n")
			}

			// print data detail
			if optDataDetail {
				var view *proto.DataPartitionsView
				if view, err = client.ClientAPI().GetDataPartitions(volumeName); err != nil {
					errout("Get volume data detail information failed:\n%v\n", err)
					os.Exit(1)
				}
				stdout("\n[Data detail]\n")
				stdout("%v\n", dataPartitionTableHeader)
				sort.SliceStable(view.DataPartitions, func(i, j int) bool {
					return view.DataPartitions[i].PartitionID < view.DataPartitions[j].PartitionID
				})
				for _, dp := range view.DataPartitions {
					stdout("%v\n", formatDataPartitionTableRow(dp))
				}
				stdout("\n")
			}
			return
		},
	}
	cmd.Flags().BoolVarP(&optMetaDetail, "meta-detail", "m", cmdVolInfoDefaultMetaDetail, "Display metadata detail information")
	cmd.Flags().BoolVarP(&optDataDetail, "data-detail", "d", cmdVolInfoDefaultDataDetail, "Display data detail information")
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

func calcAuthKey(key string) (authKey string) {
	h := md5.New()
	_, _ = h.Write([]byte(key))
	cipherStr := h.Sum(nil)
	return strings.ToLower(hex.EncodeToString(cipherStr))
}
