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
	"encoding/json"
	"fmt"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/cubefs/cubefs/datanode"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
)

const (
	cmdDataNodeShort = "Manage data nodes"
)

func newDataNodeCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:     CliResourceDataNode,
		Short:   cmdDataNodeShort,
		Aliases: []string{"dn"},
	}
	cmd.AddCommand(
		newDataNodeListCmd(client),
		newDataNodeInfoCmd(client),
		newDataNodeDecommissionCmd(client),
		newDataNodeFixMissingReplicasCmd(client),
	)
	return cmd
}

const (
	cmdDataNodeListShort               = "List information of data nodes"
	cmdDataNodeInfoShort               = "Show information of a data node"
	cmdDataNodeDecommissionInfoShort   = "Decommission partitions in a data node to others"
	cmdDataNodeFixMissingReplicasShort = "Fix missing local replicas so data node can start. Note that this command should be executed on data node"
)

func newDataNodeListCmd(client *master.MasterClient) *cobra.Command {
	var optFilterStatus string
	var optFilterWritable string
	var cmd = &cobra.Command{
		Use:     CliOpList,
		Short:   cmdDataNodeListShort,
		Aliases: []string{"ls"},
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			var view *proto.ClusterView
			if view, err = client.AdminAPI().GetCluster(); err != nil {
				return
			}
			sort.SliceStable(view.DataNodes, func(i, j int) bool {
				return view.DataNodes[i].ID < view.DataNodes[j].ID
			})
			stdout("[Data nodes]\n")
			stdout("%v\n", formatNodeViewTableHeader())
			for _, node := range view.DataNodes {
				if optFilterStatus != "" &&
					!strings.Contains(formatNodeStatus(node.Status), optFilterStatus) {
					continue
				}
				if optFilterWritable != "" &&
					!strings.Contains(formatYesNo(node.IsWritable), optFilterWritable) {
					continue
				}
				stdout("%v\n", formatNodeView(&node, true))
			}
		},
	}
	cmd.Flags().StringVar(&optFilterWritable, "filter-writable", "", "Filter node writable status")
	cmd.Flags().StringVar(&optFilterStatus, "filter-status", "", "Filter node status [Active, Inactive")
	return cmd
}

func newDataNodeInfoCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpInfo + " [NODE ADDRESS]",
		Short: cmdDataNodeInfoShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var nodeAddr string
			var datanodeInfo *proto.DataNodeInfo
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			nodeAddr = args[0]
			if datanodeInfo, err = client.NodeAPI().GetDataNode(nodeAddr); err != nil {
				return
			}
			stdout("[Data node info]\n")
			stdout(formatDataNodeDetail(datanodeInfo, false))

		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validDataNodes(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	return cmd
}

func newDataNodeDecommissionCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpDecommission + " [NODE ADDRESS]",
		Short: cmdDataNodeDecommissionInfoShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var nodeAddr string
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			nodeAddr = args[0]
			if err = client.NodeAPI().DataNodeDecommission(nodeAddr); err != nil {
				return
			}
			stdout("Decommission data node successfully\n")

		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validDataNodes(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	return cmd
}

func newDataNodeFixMissingReplicasCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpFixMissingReplicas + " [ADDRESS] [FILE CONTAINS DP ID]",
		Short: cmdDataNodeFixMissingReplicasShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err         error
				data        []byte
				partitionID uint64
			)
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			addr := args[0]
			dpFile := args[1]
			data, err = os.ReadFile(dpFile)
			if err != nil {
				return
			}
			dpStrings := strings.Split(strings.TrimSuffix(string(data), "\n"), " ")
			for _, dpString := range dpStrings {
				partitionID, err = strconv.ParseUint(dpString, 10, 64)
				stdout("Fix missing replica [%v] ... ", partitionID)
				if err != nil {
					return
				}
				var dpInfo *proto.DataPartitionInfo
				if dpInfo, err = client.AdminAPI().GetDataPartition("", partitionID); err != nil {
					return
				}
				for _, replica := range dpInfo.Replicas {
					if replica.Addr != addr {
						continue
					}
					if replica.DiskPath == "" {
						err = fmt.Errorf("Disk path is empty!")
						return
					}

					var diskInfo os.FileInfo
					diskInfo, err = os.Stat(replica.DiskPath)
					if err != nil || !diskInfo.IsDir() {
						err = fmt.Errorf("failed to stat disk path [%v] as a directory, err(%v)", replica.DiskPath, err)
						return
					}

					md := datanode.DataPartitionMetadata{
						VolumeID:                dpInfo.VolName,
						PartitionID:             dpInfo.PartitionID,
						PartitionSize:           int(replica.Total),
						Peers:                   dpInfo.Peers,
						Hosts:                   dpInfo.Hosts,
						DataPartitionCreateType: proto.NormalCreateDataPartition,
						CreateTime:              time.Now().Format(datanode.TimeLayout),
					}

					var (
						metadata []byte
						fmeta    *os.File
					)
					metadata, err = json.Marshal(md)
					if err != nil {
						return
					}

					filePath := path.Join(replica.DiskPath, fmt.Sprintf(datanode.DataPartitionPrefix+"_%v_%v", md.PartitionID, md.PartitionSize))
					stdout("[%v] ", filePath)
					if err = os.MkdirAll(filePath, 0755); err != nil {
						return
					}

					fileName := path.Join(filePath, datanode.DataPartitionMetadataFileName)
					fmeta, err = os.OpenFile(fileName, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0666)
					if err != nil {
						return
					}
					if _, err = fmeta.Write(metadata); err != nil {
						fmeta.Close()
						return
					}
					if err = fmeta.Sync(); err != nil {
						fmeta.Close()
						return
					}
					fmeta.Close()
					break
				}
				stdout("OK\n")
			}
			stdout("Fix missing replicas finished!\n")
			return
		},
	}
	return cmd
}
