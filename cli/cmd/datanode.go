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
	"github.com/chubaofs/chubaofs/sdk/data"
	"github.com/chubaofs/chubaofs/sdk/master"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/spf13/cobra"
	atomic2 "go.uber.org/atomic"
	"os"
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
			CheckDataNodeCrc(nodeAddr, client, uint64(limitRate), optCheckType, fromTime, dealResultFunc)
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
	return cmd
}

func CheckDataNodeCrc(nodeAddr string, c *master.MasterClient, limitRate uint64, optCheckType int, fromTime string, dealResultFunc func(rExtent RepairExtentInfo, repairFD *os.File, canNotRepairFD *os.File)) (err error) {
	var datanodeInfo *proto.DataNodeInfo
	log.LogInfof("CheckDataNodeCrc begin, datanode:%v", nodeAddr)
	var minParsedTime time.Time
	if fromTime != "" {
		minParsedTime, err = time.Parse("2006-01-02 15:04:05", fromTime)
		if err != nil {
			fmt.Println(err)
			return
		}
	} else {
		minParsedTime = time.Unix(0, 0)
	}
	if datanodeInfo, err = c.NodeAPI().GetDataNode(nodeAddr); err != nil {
		return
	}
	datanodeAddr := fmt.Sprintf("%s:%d", strings.Split(nodeAddr, ":")[0], c.DataNodeProfPort)

	dataClient := data.NewDataHttpClient(datanodeAddr, false)

	diskInfo, err := dataClient.GetDisks()
	if err != nil {
		log.LogErrorf("err:%v", err)
	}
	diskMap := make(map[string]chan bool, len(diskInfo.Disks))
	for _, d := range diskInfo.Disks {
		diskMap[d.Path] = make(chan bool, limitRate)
	}

	idsExclude := loadSpecifiedPartitions()
	failedDpFd, _ := os.OpenFile(fmt.Sprintf("failedDp_%v.csv", time.Now().Format("2006010215")), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	failedExtentsFd, _ := os.OpenFile(fmt.Sprintf("failedExtents_%v.csv", time.Now().Format("2006010215")), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	defer failedDpFd.Close()
	defer failedExtentsFd.Close()
	wg := sync.WaitGroup{}
	var lock sync.Mutex
	rCh := make(chan RepairExtentInfo, 1024)
	defer close(rCh)
	go func() {
		repairFD, _ := os.OpenFile(fmt.Sprintf("repair_extents_%v_%v", c.Nodes()[0], time.Now().Format("2006010215")), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		canNotRepairFD, _ := os.OpenFile(fmt.Sprintf("can_not_repair_extents_%v_%v", c.Nodes()[0], time.Now().Format("2006010215")), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		defer repairFD.Close()
		defer canNotRepairFD.Close()
		for {
			select {
			case rExtent := <- rCh:
				if rExtent.PartitionID == 0 && rExtent.ExtentID == 0 {
					return
				}
				dealResultFunc(rExtent, repairFD, canNotRepairFD)
			}
		}
	}()
	counter := atomic2.Int64{}
	for index, id := range datanodeInfo.PersistenceDataPartitions {
		if idExist(id, idsExclude) {
			continue
		}
		var dpDnInfo *proto.DNDataPartitionInfo
		dpDnInfo, err = dataClient.GetPartitionFromNode(id)
		if err != nil || dpDnInfo.RaftStatus == nil || dpDnInfo.RaftStatus.Stopped == true {
			log.LogErrorf("RaftStatus is Stopped PartitionId(%v) err(%v)\n", id, err)
			continue
		}
		wg.Add(1)
		diskPath := strings.Split(dpDnInfo.Path, "/datapartition")[0]
		log.LogDebugf(" check datanode start [%v] dp:%v total:%d, cur:%d, limit:%v, path:%v", nodeAddr, id, len(datanodeInfo.PersistenceDataPartitions), index, limitRate, diskPath)
		go func(dp uint64, diskPath string) {
			diskMap[diskPath] <- true
			var err1 error
			var failedExtents []uint64
			defer func() {
				wg.Done()
				<- diskMap[diskPath]
				counter.Add(1)
				if err1 != nil {
					log.LogErrorf(" check datanode:%v dp:%v end, progress:(%d/%d), err:%v", nodeAddr, dp, counter.Load(), len(datanodeInfo.PersistenceDataPartitions), err)
				} else {
					log.LogInfof(" check datanode:%v dp:%v end, progress:(%d/%d)", nodeAddr, dp, counter.Load(), len(datanodeInfo.PersistenceDataPartitions))
				}
			}()
			if failedExtents, err1 = checkDataPartitionRelica(c, dp, optCheckType, minParsedTime, rCh); err1 != nil {
				lock.Lock()
				defer lock.Unlock()
				failedDpFd.WriteString(fmt.Sprintf("%v\n", dp))
				failedDpFd.Sync()
			} else if len(failedExtents) > 0 {
				lock.Lock()
				defer lock.Unlock()
				for _, e := range failedExtents {
					failedExtentsFd.WriteString(fmt.Sprintf("%v %v\n", dp, e))
				}
				failedExtentsFd.Sync()
			} else {
				log.LogInfof(" check datanode:%v dp:%v finish\n", nodeAddr, dp)
			}
		}(id, diskPath)
	}
	wg.Wait()
	log.LogInfof("CheckDataNodeCrc end, datanode:%v", nodeAddr)
	rCh <- RepairExtentInfo{
		PartitionID: 0,
		ExtentID: 0,
	}
	return
}