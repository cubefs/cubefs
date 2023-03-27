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
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/data"
	"github.com/chubaofs/chubaofs/sdk/master"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/spf13/cobra"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	cmdDataNodeShort = "Manage data nodes"
)

func newDataNodeCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliResourceDataNode,
		Short: cmdDataNodeShort,
	}
	cmd.AddCommand(
		newDataNodeListCmd(client),
		newDataNodeInfoCmd(client),
		newDataNodeDecommissionCmd(client),
		newDataNodeDiskDecommissionCmd(client),
		newResetDataNodeCmd(client),
		newStopMigratingByDataNode(client),
		newCheckReplicaByDataNodeCmd(client),
	)
	return cmd
}

const (
	cmdDataNodeListShort                 = "List information of data nodes"
	cmdDataNodeInfoShort                 = "Show information of a data node"
	cmdDataNodeDecommissionInfoShort     = "decommission partitions in a data node to others"
	cmdDataNodeDiskDecommissionInfoShort = "decommission disk of partitions in a data node to others"
	cmdResetDataNodeShort                = "Reset corrupt data partitions related to this node"
	cmdStopMigratingEcByDataNode         = "stop migrating task by data node"
	cmdCheckReplicaByDataNodeShort       = "Check all normal extents which in this data node"
)

func newDataNodeListCmd(client *master.MasterClient) *cobra.Command {
	var optFilterStatus string
	var optFilterWritable string
	var optShowDp bool
	var cmd = &cobra.Command{
		Use:     CliOpList,
		Short:   cmdDataNodeListShort,
		Aliases: []string{"ls"},
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				if err != nil {
					errout("List cluster data nodes failed: %v\n", err)
				}
			}()
			var view *proto.ClusterView
			if view, err = client.AdminAPI().GetCluster(); err != nil {
				return
			}
			sort.SliceStable(view.DataNodes, func(i, j int) bool {
				return view.DataNodes[i].ID < view.DataNodes[j].ID
			})
			var info *proto.DataNodeInfo
			var nodeInfoSlice []*proto.DataNodeInfo
			if optShowDp {
				nodeInfoSlice = make([]*proto.DataNodeInfo, len(view.DataNodes), len(view.DataNodes))
				for index, node := range view.DataNodes {
					if info, err = client.NodeAPI().GetDataNode(node.Addr); err != nil {
						return
					}
					nodeInfoSlice[index] = info
				}
			}
			stdout("[Data nodes]\n")
			var header, row string
			if optShowDp {
				header = formatDataNodeViewTableHeader()
			} else {
				header = formatNodeViewTableHeader()
			}
			stdout("%v\n", header)
			for index, node := range view.DataNodes {
				if optFilterStatus != "" &&
					!strings.Contains(formatNodeStatus(node.Status), optFilterStatus) {
					continue
				}
				if optFilterWritable != "" &&
					!strings.Contains(formatYesNo(node.IsWritable), optFilterWritable) {
					continue
				}
				if optShowDp {
					info = nodeInfoSlice[index]
					row = fmt.Sprintf(dataNodeDetailViewTableRowPattern, node.ID, node.Addr,node.Version,
						formatYesNo(node.IsWritable), formatNodeStatus(node.Status), formatSize(info.Used), formatFloat(info.UsageRatio), info.ZoneName, info.DataPartitionCount)
				} else {
					row = formatNodeView(&node, true)
				}
				stdout("%v\n", row)
			}
		},
	}
	cmd.Flags().StringVar(&optFilterWritable, "filter-writable", "", "Filter node writable status")
	cmd.Flags().StringVar(&optFilterStatus, "filter-status", "", "Filter node status [Active, Inactive]")
	cmd.Flags().BoolVarP(&optShowDp, "detail", "d", false, "Show detail information")
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
					errout("Show data node info failed: %v\n", err)
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
					errout("decommission data node failed, err[%v]\n", err)
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

func newDataNodeDiskDecommissionCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpDecommissionDisk + " [NODE ADDRESS]" + "[DISK PATH]",
		Short: cmdDataNodeDiskDecommissionInfoShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var nodeAddr string
			var diskAddr string
			defer func() {
				if err != nil {
					errout("decommission disk failed, err[%v]\n", err)
				}
			}()
			nodeAddr = args[0]
			diskAddr = args[1]
			if err = client.NodeAPI().DataNodeDiskDecommission(nodeAddr, diskAddr); err != nil {
				return
			}
			stdout("Decommission disk successfully\n")
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

func newResetDataNodeCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpReset + " [ADDRESS]",
		Short: cmdResetDataNodeShort,
		Long: `If more than half replicas of a partition are on the corrupt nodes, the few remaining replicas can 
not reach an agreement with one leader. In this case, you can use the "reset" command to fix the problem. This command
is used to reset all the corrupt partitions related to a chosen corrupt node. However this action may lead to data 
loss, be careful to do this.`,
		Args: cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				address string
				confirm string
				err     error
			)
			defer func() {
				if err != nil {
					errout("Error:%v", err)
					OsExitWithLogFlush()
				}
			}()
			address = args[0]
			stdout(fmt.Sprintf("The action may risk the danger of losing data, please confirm(y/n):"))
			_, _ = fmt.Scanln(&confirm)
			if "y" != confirm && "yes" != confirm {
				return
			}
			if err = client.AdminAPI().ResetCorruptDataNode(address); err != nil {
				return
			}
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

func newStopMigratingByDataNode(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpStopMigratingEc + " [NODE ADDRESS]",
		Short: cmdStopMigratingEcByDataNode,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				nodeAddr string
			)
			nodeAddr = args[0]
			stdout("%v\n", client.NodeAPI().StopMigratingByDataNode(nodeAddr))
		},
	}
	return cmd
}

func newCheckReplicaByDataNodeCmd(client *master.MasterClient) *cobra.Command {
	var limitRate int
	var optCheckType int
	var fromTime string
	var checkTiny bool
	var cmd = &cobra.Command{
		Use:   CliOpCheckReplica + " [ADDRESS]",
		Short: cmdCheckReplicaByDataNodeShort,
		Args: cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var nodeAddr string
			defer func() {
				if err != nil {
					errout("Show data node info failed: %v\n", err)
				}
			}()
			if limitRate < 1 {
				limitRate = 1
			} else if limitRate > 200 {
				limitRate = 200
			}
			nodeAddr = args[0]
			CheckDataNodeCrc(nodeAddr, client, uint64(limitRate), optCheckType, fromTime, checkTiny)
			stdout("finish datanode replica crc check")
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validDataNodes(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	cmd.Flags().IntVar(&limitRate, "limit-rate",  10, "specify dp check limit rate, default:10, max:200")
	cmd.Flags().IntVar(&optCheckType, "check-type",  0, "specify check type : 0 all, 1 crc, 2 md5, 3 block")
	cmd.Flags().StringVar(&fromTime, "from-time", "1970-01-01 00:00:00", "specify extent modify from time to check, format:yyyy-mm-dd hh:mm:ss")
	cmd.Flags().BoolVar(&checkTiny, "check-tiny", false, "check tiny extent")
	return cmd
}

func parseTime(timeStr string) (t time.Time, err error) {
	if timeStr != "" {
		t, err = time.Parse("2006-01-02 15:04:05", timeStr)
		if err != nil {
			return
		}
	} else {
		t = time.Unix(0, 0)
	}
	return
}

func CheckDataNodeCrc(nodeAddr string, c *master.MasterClient, limitRate uint64, optCheckType int, fromTime string, checkTiny bool) (err error) {
	var (
		minParsedTime time.Time
		dpCount int
	)
	defer func() {
		if err != nil {
			fmt.Printf("CheckDataNodeCrc error:%v\n", err)
		}
	}()
	log.LogInfof("CheckDataNodeCrc begin, datanode:%v", nodeAddr)
	minParsedTime, err = parseTime(fromTime)
	if err != nil {
		return
	}
	rp := NewRepairPersist(c.Nodes()[0])
	go rp.persistResult()
	defer rp.close()
	dr, err := newDataNodeRepair(nodeAddr, c, limitRate, minParsedTime)
	if err != nil {
		return
	}
	wg := sync.WaitGroup{}
	dpCh := make(chan uint64, 1000)
	for _, dp := range dr.datanodeInfo.PersistenceDataPartitions {
		if idExist(dp, dr.excludeDPs) {
			continue
		}
		dpCount++
	}
	wg.Add(dpCount)
	go func() {
		for _, dp := range dr.datanodeInfo.PersistenceDataPartitions {
			if idExist(dp, dr.excludeDPs) {
				continue
			}
			dpCh <- dp
		}
		close(dpCh)
	}()

	for i := 0; i < int(limitRate); i++ {
		go func() {
			for dp := range dpCh {
				dr.doRepairPartition(dp, rp, optCheckType, checkTiny)
				wg.Done()
			}
		}()
	}
	wg.Wait()
	log.LogInfof("CheckDataNodeCrc end, datanode:%v", nodeAddr)
	return
}


type dataNodeRepair struct {
	nodeAddr      string
	persistDps    []uint64
	datanodeInfo  *proto.DataNodeInfo
	diskMap       map[string]chan bool
	excludeDPs    []uint64
	client        *master.MasterClient
	minParsedTime time.Time
}
func newDataNodeRepair(nodeAddr string, c *master.MasterClient, limitRate uint64, minParsedTime time.Time)(dr *dataNodeRepair, err error){
	dr = new(dataNodeRepair)
	dr.nodeAddr = nodeAddr
	dr.minParsedTime = minParsedTime
	if dr.datanodeInfo, err = c.NodeAPI().GetDataNode(nodeAddr); err != nil {
		return
	}
	dr.client = c
	dr.excludeDPs = loadSpecifiedPartitions()
	return
}

func (dr *dataNodeRepair) doRepairPartition(dp uint64, rp *RepairPersist, optCheckType int, checkTiny bool) {
	var (
		err error
	 	failedExtents []uint64
	)
	defer func() {
		rp.dpCounter.Add(1)
		log.LogInfof(" check datanode:%v dp:%v end, progress:(%d/%d)", dr.nodeAddr, dp, rp.dpCounter.Load(), len(dr.persistDps))
		if err != nil {
			log.LogErrorf(" check datanode:%v dp:%v end, progress:(%d/%d), err:%v", dr.nodeAddr, dp, rp.dpCounter.Load(), len(dr.persistDps), err)
		}
	}()
	if failedExtents, err = checkDataPartitionRelica(dr.client, dp, optCheckType, dr.minParsedTime, rp.rCh, checkTiny); err != nil {
		rp.persistFailedDp(dp)
	} else if len(failedExtents) > 0 {
		rp.persistFailedExtents(dp, failedExtents)
	} else {
		log.LogInfof(" check datanode:%v dp:%v finish\n", dr.nodeAddr, dp)
	}
}

func getDiskPath(nodeAddr string, prof uint16, dp uint64)(diskPath string, err error) {
	var dpDnInfo *proto.DNDataPartitionInfo
	dpDnInfo, err = getDataPartitionInfo(nodeAddr, prof, dp)
	if err != nil || dpDnInfo.RaftStatus == nil || dpDnInfo.RaftStatus.Stopped == true {
		err = fmt.Errorf("RaftStatus is Stopped PartitionId(%v) err(%v)\n", dp, err)
		return
	}
	diskPath = strings.Split(dpDnInfo.Path, "/datapartition")[0]
	return
}
func getDataPartitionInfo(nodeAddr string, prof uint16, dp uint64)(dn *proto.DNDataPartitionInfo, err error) {
	datanodeAddr := fmt.Sprintf("%s:%d", strings.Split(nodeAddr, ":")[0], prof)
	dataClient := data.NewDataHttpClient(datanodeAddr, false)
	dn, err = dataClient.GetPartitionFromNode(dp)
	return
}
func getDataNodeDiskMap(nodeAddr string, prof uint16, limit uint64) (diskMap map[string]chan bool, err error) {
	datanodeAddr := fmt.Sprintf("%s:%d", strings.Split(nodeAddr, ":")[0], prof)
	dataClient := data.NewDataHttpClient(datanodeAddr, false)

	diskInfo, err := dataClient.GetDisks()
	if err != nil {
		log.LogErrorf("err:%v", err)
	}

	diskMap = make(map[string]chan bool, len(diskInfo.Disks))
	for _, d := range diskInfo.Disks {
		diskMap[d.Path] = make(chan bool, limit)
	}
	return
}
