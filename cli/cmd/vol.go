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
	"math"
	"os"
	"sort"
	"strconv"
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
		Use:   cmdVolUse,
		Short: cmdVolShort,
		Args:  cobra.MinimumNArgs(0),
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
	var cmd = &cobra.Command{
		Use:   cmdVolListUse,
		Short: cmdVolListShort,
		Run: func(cmd *cobra.Command, args []string) {

		},
	}
	return cmd
}

const (
	cmdVolCreateUse             = "create [VOLUME NAME] [USER ID]"
	cmdVolCreateShort           = "Create a new volume"
	cmdVolDefaultMPCount        = 3
	cmdVolDefaultDPSize         = 10
	cmdVolDefaultCapacity       = 10 // 100GB
	cmdVolDefaultReplicas       = 3
	cmdVolDefaultFollowerReader = true
)

func newVolCreateCmd(client *master.MasterClient) *cobra.Command {
	var optMPCount int
	var optDPCount uint64
	var optCapacity uint64
	var optReplicas int
	var optFollowerRead bool
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
			stdout("  Meta partition count: %v\n", optMPCount)
			stdout("  Dara partition count: %v\n", optDPCount)
			stdout("  Capacity            : %v\n", optCapacity)
			stdout("  Replicas            : %v\n", optReplicas)
			stdout("  Allow follower read : %v\n", optFollowerRead)

			// confirm
			// ask user for confirm
			stdout("\nConfirm (yes/no)[yes]: ")
			var userConfirm string
			_, _ = fmt.Scanln(&userConfirm)
			if userConfirm != "yes" && len(userConfirm) != 0 {
				stdout("Abort by user.\n")
				return
			}
			err = client.AdminAPI().CreateVolume(
				volumeName, userID, optMPCount, optDPCount,
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
	cmd.Flags().Uint64Var(&optDPCount, "dp-count", cmdVolDefaultDPSize, "Specify init data partition count")
	cmd.Flags().Uint64Var(&optCapacity, "capacity", cmdVolDefaultCapacity, "Specify volume capacity [Unit: GB]")
	cmd.Flags().IntVar(&optReplicas, "replicas", cmdVolDefaultReplicas, "Specify volume replicas number")
	cmd.Flags().BoolVar(&optFollowerRead, "follower-read", cmdVolDefaultFollowerReader, "Allow read form replica follower")
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
				stdout("  %10v\t%12v\t%12v\t%12v\t%10v\t%18v\t%18v\n",
					"ID", "MAX INODE", "START", "END", "STATUS", "LEADER", "MEMBERS")
				sort.SliceStable(views, func(i, j int) bool {
					return views[i].PartitionID < views[j].PartitionID
				})
				for _, view := range views {
					stdout(formatMetaPartitionView(view))
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
				stdout("  %10v\t%8v\t%10v\t%18v\t%18v\n",
					"ID", "REPLICAS", "STATUS", "LEADER", "MEMBERS")
				sort.SliceStable(view.DataPartitions, func(i, j int) bool {
					return view.DataPartitions[i].PartitionID < view.DataPartitions[j].PartitionID
				})
				for _, dp := range view.DataPartitions {
					stdout(formatDataPartitionView(dp))
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
	var cmd = &cobra.Command{
		Use:   cmdVolDeleteUse,
		Short: cmdVolDeleteShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var volumeName = args[0]
			// ask user for confirm
			stdout("Delete volume [%v] (yes/no)[no]:", volumeName)
			var userConfirm string
			_, _ = fmt.Scanln(&userConfirm)
			if userConfirm != "yes" {
				stdout("Abort by user.\n")
				return
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
	return cmd
}

func formatSimpleVolView(svv *proto.SimpleVolView) string {
	var statusToString = func(status uint8) string {
		switch status {
		case 0:
			return "normal"
		case 1:
			return "marked delete"
		default:
			return "Unknown"
		}
	}
	var sb = strings.Builder{}
	sb.WriteString(fmt.Sprintf("  ID                  : %v\n", svv.ID))
	sb.WriteString(fmt.Sprintf("  Name                : %v\n", svv.Name))
	sb.WriteString(fmt.Sprintf("  Owner               : %v\n", svv.Owner))
	sb.WriteString(fmt.Sprintf("  Zone                : %v\n", svv.ZoneName))
	sb.WriteString(fmt.Sprintf("  Status              : %v\n", statusToString(svv.Status)))
	sb.WriteString(fmt.Sprintf("  Capacity            : %v GB\n", svv.Capacity))
	sb.WriteString(fmt.Sprintf("  Create time         : %v\n", svv.CreateTime))
	sb.WriteString(fmt.Sprintf("  Authenticate        : %v\n", svv.Authenticate))
	sb.WriteString(fmt.Sprintf("  Follower read       : %v\n", svv.FollowerRead))
	sb.WriteString(fmt.Sprintf("  Cross zone          : %v\n", svv.CrossZone))
	sb.WriteString(fmt.Sprintf("  Meta partition count: %v\n", svv.MpCnt))
	sb.WriteString(fmt.Sprintf("  Meta replicas       : %v\n", svv.MpReplicaNum))
	sb.WriteString(fmt.Sprintf("  Data partition count: %v\n", svv.DpCnt))
	sb.WriteString(fmt.Sprintf("  Data replicas       : %v\n", svv.DpReplicaNum))
	return sb.String()
}

func formatMetaPartitionView(view *proto.MetaPartitionView) string {
	var statusToString = func(status int8) string {
		switch status {
		case 1:
			return "read only"
		case 2:
			return "writable"
		case -1:
			return "unavailable"
		default:
			return "unknown"
		}
	}
	var rangeToString = func(num uint64) string {
		if num >= math.MaxInt64 {
			return "unlimited"
		}
		return strconv.FormatUint(num, 10)
	}
	return fmt.Sprintf("  %10v\t%12v\t%12v\t%12v\t%10v\t%18v\t%18v\n",
		view.PartitionID, view.MaxInodeID, view.Start, rangeToString(view.End), statusToString(view.Status), view.LeaderAddr, view.Members)
}

func formatDataPartitionView(view *proto.DataPartitionResponse) string {
	var statusToString = func(status int8) string {
		switch status {
		case 1:
			return "read only"
		case 2:
			return "writable"
		case -1:
			return "unavailable"
		default:
			return "unknown"
		}
	}
	return fmt.Sprintf("  %10v\t%8v\t%10v\t%18v\t%18v\n",
		view.PartitionID, view.ReplicaNum, statusToString(view.Status), view.LeaderAddr, view.Hosts)
}

func calcAuthKey(key string) (authKey string) {
	h := md5.New()
	_, _ = h.Write([]byte(key))
	cipherStr := h.Sum(nil)
	return strings.ToLower(hex.EncodeToString(cipherStr))
}
