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
	"github.com/cubefs/cubefs/cli/api"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/spf13/cobra"
)

const (
	cmdInodeUse   = "inode [command]"
	cmdInodeShort = "Manage inode"
)

func newInodeCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   cmdInodeUse,
		Short: cmdInodeShort,
	}
	cmd.AddCommand(
		newInodeInfoCmd(client),
		newInodeListCmd(client),
	)
	return cmd
}

const (
	cmdInodeListShort = "List all inodes in a meta partition"
	cmdInodeInfoShort = "Get inode info by inode id"
)

func newInodeListCmd(client *master.MasterClient) *cobra.Command {
	var optMetaAddr string
	var pid uint64
	var cmd = &cobra.Command{
		Use:   CliOpList,
		Short: cmdInodeListShort,
		Args:  cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			info, err := api.NewMetaHttpClient(optMetaAddr, false).GetAllInodesByPid(pid)
			if err != nil {
				return
			}
			stdout("[Inode List]\n")
			for _, v := range info {
				stdout("==============================\n")
				stdout(" Inode ID             : %v\n", v.Inode)
				stdout(" Type                 : %v\n", v.Type)
				stdout(" Uid                  : %v\n", v.Uid)
				stdout(" Gid                  : %v\n", v.Gid)
				stdout(" Size                 : %v\n", v.Size)
				stdout(" Generation           : %v\n", v.Generation)
				stdout(" CreateTime           : %v\n", v.CreateTime)
				stdout(" AccessTime           : %v\n", v.AccessTime)
				stdout(" ModifyTime           : %v\n", v.ModifyTime)
				stdout(" LinkTarget           : %v\n", v.LinkTarget)
				stdout(" NLink                : %v\n", v.NLink)
				stdout(" Flag                 : %v\n", v.Flag)
				stdout(" Reserved             : %v\n", v.Reserved)
			}
		},
	}
	cmd.Flags().StringVar(&optMetaAddr, "addr", "", "Meta server address")
	cmd.Flags().Uint64Var(&pid, "pid", 0, "Partition id")
	return cmd
}

func newInodeInfoCmd(client *master.MasterClient) *cobra.Command {
	var optMetaAddr string
	var pid uint64
	var inodeId uint64
	var cmd = &cobra.Command{
		Use:   CliOpInfo + " [inodeId]",
		Short: cmdInodeInfoShort,
		Args:  cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			info, err := api.NewMetaHttpClient(optMetaAddr, false).GetInodeInfo(pid, inodeId)
			if err != nil {
				return
			}
			stdout("[Inode info]\n")
			stdout(" Generation        : %v\n", info.Generation)
			stdout(" Size              : %v\n", info.Size)
			stdout(" Extents           :\n")

			/*// imitate 50 extents
			extent1 := proto.ExtentKey{
				FileOffset:   0,
				PartitionId:  0,
				ExtentId:     0,
				ExtentOffset: 0,
				Size:         0,
				CRC:          0,
			}
			extent2 := proto.ExtentKey{
				FileOffset:   1,
				PartitionId:  1,
				ExtentId:     1,
				ExtentOffset: 1,
				Size:         1,
				CRC:          1,
			}
			info.Extents = append(info.Extents, extent2)
			for i := 0; i < 50; i++ {
				info.Extents = append(info.Extents, extent1)
			}*/
			displayExtentsPage(info.Extents)
		},
	}
	cmd.Flags().StringVar(&optMetaAddr, "addr", "", "Meta server address")
	cmd.Flags().Uint64Var(&pid, "pid", 0, "Partition id")
	cmd.Flags().Uint64Var(&inodeId, "inodeId", 0, "Inode id")
	return cmd
}

const itemsPerPage = 3

func displayExtentsPage(extents []proto.ExtentKey) {
	pageIndex := 0
	displayColumns(extents, pageIndex)
	for {
		stdout("Press 'q' to quit, 'n' to next page, 'p' to previous page\n")
		var input string
		_, err := fmt.Scanln(&input)
		if err != nil {
			errout("Error: %v", err)
			return
		}
		switch input {
		case "q":
			return
		case "n":
			if pageIndex >= len(extents)/itemsPerPage {
				displayColumns(extents, pageIndex)
				stdout("This is the last page\n")
				continue
			}
			displayColumns(extents, pageIndex+1)
			pageIndex++
		case "p":
			if pageIndex > 0 {
				displayColumns(extents, pageIndex-1)
				pageIndex--
			} else {
				displayColumns(extents, pageIndex)
				stdout("This is the first page\n")
			}
		}
	}
}

func displayColumns(arr []proto.ExtentKey, pageIndex int) {
	start := pageIndex * itemsPerPage
	end := start + itemsPerPage
	if end > len(arr) {
		end = len(arr)
	}
	for i := start; i < end; i++ {
		stdout("==============================\n")
		stdout(" ExtentId         : %v\n", arr[i].ExtentId)
		stdout(" PartitionId      : %v\n", arr[i].PartitionId)
		stdout(" FileOffset       : %v\n", arr[i].FileOffset)
		stdout(" ExtentOffset     : %v\n", arr[i].ExtentOffset)
		stdout(" Size             : %v\n", arr[i].Size)
		stdout(" CRC              : %v\n", arr[i].CRC)
	}
}
