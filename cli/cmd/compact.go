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
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/master"
	"github.com/spf13/cobra"
	"sort"
	"strings"
)

const (
	cmdCompactUse   = "compact [COMMAND]"
	cmdCompactShort = "Manage compact info"
)

func newCompactCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   cmdCompactUse,
		Short: cmdCompactShort,
	}
	cmd.AddCommand(
		newCompactVolList(client),
		newCompactBatchCloseCmd(client),
		newCompactBatchOpenCmd(client),
	)
	return cmd
}

const (
	cmdCompactVolList = "List all compacting volumes"
)

func newCompactVolList(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:     CliOpList,
		Short:   cmdCompactVolList,
		Aliases: []string{"ls"},
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var compactVolumes []*proto.CompactVolume
			compactVolumes, err = client.AdminAPI().ListCompactVolumes()
			if err != nil {
				errout("list all compacting volumes failed case:\n%v\n", err)
			}
			sort.Slice(compactVolumes, func(i, j int) bool { return compactVolumes[i].Name < compactVolumes[j].Name })
			stdout("[compacting volumes]\n")
			stdout("%v\n", formatCompactVolViewTableHeader())
			for _, cVolume := range compactVolumes {
				if cVolume.CompactTag != proto.CompactOpen {
					continue
				}
				stdout("%v\n", formatCompactVolView(cVolume))
			}
		},
	}
	return cmd
}

const (
	cmdCompactCloseUse   = "close"
	cmdCompactCloseShort = "close volume compact"
	all                  = "all"
)

type volumeOwner struct {
	volumeName string
	owner      string
}

func newCompactBatchCloseCmd(client *master.MasterClient) *cobra.Command {
	var optVolName string
	var optYes bool
	var cmd = &cobra.Command{
		Use:   cmdCompactCloseUse,
		Short: cmdCompactCloseShort,
		Args:  cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			if len(optVolName) == 0 {
				errout("Please input volume name")
			}

			// ask user for confirm
			if !optYes {
				stdout("  Volume  Name        : %v\n", optVolName)
				stdout("\nConfirm (yes/no)[yes]: ")
				var userConfirm string
				_, _ = fmt.Scanln(&userConfirm)
				if userConfirm != "yes" && len(userConfirm) != 0 {
					stdout("Abort by user.\n")
					return
				}
			}

			var compactVolumes []*proto.CompactVolume
			compactVolumes, err = client.AdminAPI().ListCompactVolumes()
			if err != nil {
				errout("close compact listCompactVolumes failed case:\n%v\n", err)
			}
			var compactVolumesMap = make(map[string]*proto.CompactVolume, len(compactVolumes))
			for _, cVolume := range compactVolumes {
				compactVolumesMap[cVolume.Name] = cVolume
			}
			var setCompactMsg string
			var volumeOwners []volumeOwner
			if optVolName == all {
				for _, cVolume := range compactVolumes {
					if cVolume.CompactTag != proto.CompactOpen {
						continue
					}
					volumeOwners = append(volumeOwners, volumeOwner{
						volumeName: cVolume.Name,
						owner:      cVolume.Owner,
					})
				}
			} else {
				volNames := strings.Split(optVolName, ",")
				for _, volName := range volNames {
					var cVolume *proto.CompactVolume
					var ok bool
					if cVolume, ok = compactVolumesMap[volName]; !ok {
						setCompactMsg += fmt.Sprintf("Volume(%v) does not need to close compact.\n", volName)
						continue
					}
					if cVolume.CompactTag != proto.CompactOpen {
						setCompactMsg += fmt.Sprintf("Volume(%v) has closed compact.\n", volName)
						continue
					}
					volumeOwners = append(volumeOwners, volumeOwner{
						volumeName: cVolume.Name,
						owner:      cVolume.Owner,
					})
				}
			}
			for _, vos := range volumeOwners {
				authKey := calcAuthKey(vos.owner)
				_, cErr := client.AdminAPI().SetCompact(vos.volumeName, proto.CompactCloseName, authKey)
				if cErr != nil {
					setCompactMsg += fmt.Sprintf("Volume(%v) close compact failed, err:%v\n", vos.volumeName, cErr)
				} else {
					setCompactMsg += fmt.Sprintf("Volume(%v) close compact succeeded\n", vos.volumeName)
				}
			}
			stdout(setCompactMsg)
			stdout("close volume compact end.\n")
			return
		},
	}

	cmd.Flags().StringVar(&optVolName, CliFlagVolName, "", "Specify volume name, can split by comma or use 'all' stop all volume compact")
	cmd.Flags().BoolVarP(&optYes, "yes", "y", false, "Answer yes for all questions")
	return cmd
}

const (
	cmdCompactOpenUse   = "open"
	cmdCompactOpenShort = "open volume compact"
)

func newCompactBatchOpenCmd(client *master.MasterClient) *cobra.Command {
	var optVolName string
	var optYes bool
	var cmd = &cobra.Command{
		Use:   cmdCompactOpenUse,
		Short: cmdCompactOpenShort,
		Args:  cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			if len(optVolName) == 0 {
				errout("Please input volume name")
			}

			// ask user for confirm
			if !optYes {
				stdout("  Volume  Name        : %v\n", optVolName)
				stdout("\nConfirm (yes/no)[yes]: ")
				var userConfirm string
				_, _ = fmt.Scanln(&userConfirm)
				if userConfirm != "yes" && len(userConfirm) != 0 {
					stdout("Abort by user.\n")
					return
				}
			}
			var volumeOwners []volumeOwner
			volNames := strings.Split(optVolName, ",")
			for _, volName := range volNames {
				var vv *proto.SimpleVolView
				if vv, err = client.AdminAPI().GetVolumeSimpleInfo(volName); err != nil {
					stdout("Volume(%v) open compact failed, err:%v\n", volName, err)
					continue
				}
				volumeOwners = append(volumeOwners, volumeOwner{
					volumeName: vv.Name,
					owner:      vv.Owner,
				})
			}
			var setCompactMsg string
			for _, vos := range volumeOwners {
				authKey := calcAuthKey(vos.owner)
				_, cErr := client.AdminAPI().SetCompact(vos.volumeName, proto.CompactOpenName, authKey)
				if cErr != nil {
					setCompactMsg += fmt.Sprintf("Volume(%v) open compact failed, err:%v\n", vos.volumeName, cErr)
				} else {
					setCompactMsg += fmt.Sprintf("Volume(%v) open compact successed\n", vos.volumeName)
				}
			}
			stdout(setCompactMsg)
			stdout("open volume compact end.\n")
			return
		},
	}

	cmd.Flags().StringVar(&optVolName, CliFlagVolName, "", "Specify volume name, can split by comma")
	cmd.Flags().BoolVarP(&optYes, "yes", "y", false, "Answer yes for all questions")
	return cmd
}
