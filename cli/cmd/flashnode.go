// Copyright 2023 The CubeFS Authors.
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
	"sort"
	"strconv"
	"strings"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/httpclient"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/spf13/cobra"
)

const _flashnodeAddr = " [FlashNodeAddr]"

func newFlashNodeCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "flashnode [COMMAND]",
		Short: "cluster flashnode management",
	}
	cmd.AddCommand(
		newCmdFlashNodeSet(client),
		newCmdFlashNodeRemove(client),
		newCmdFlashNodeGet(client),
		newCmdFlashNodeList(client),
		newCmdFlashNodeRemoveAllInactive(client),
		newCmdFlashNodeHTTPStat(client),
		newCmdFlashNodeHTTPStatAll(client),
		newCmdFlashNodeHTTPEvict(client),
		newCmdFlashNodeHTTPInactiveDisk(client),
		newCmdFlashNodeHTTPSlotStat(client),
	)
	return cmd
}

func newCmdFlashNodeSet(client *master.MasterClient) *cobra.Command {
	var name string
	cmd := &cobra.Command{
		Use:   CliOpSet + _flashnodeAddr + " [IsEnable]",
		Short: "set flash node enable or not",
		Args:  cobra.MinimumNArgs(2),
		RunE: func(_ *cobra.Command, args []string) (err error) {
			addr := args[0]
			enable, err := strconv.ParseBool(args[1])
			if err != nil {
				return
			}
			if name == "" {
				name = proto.DefaultTopoName
			}
			if err = client.NodeAPI().SetFlashNodeByTopo(addr, enable, name); err != nil {
				return
			}
			stdoutlnf("set flashnode:%s enable:%v success", addr, enable)
			return
		},
	}
	cmd.Flags().StringVarP(&name, "topoName", "n", proto.DefaultTopoName, "flash topology name")
	return cmd
}

func newCmdFlashNodeRemove(client *master.MasterClient) *cobra.Command {
	var optYes bool
	var name string
	cmd := &cobra.Command{
		Use:   CliOpRemove + _flashnodeAddr,
		Short: "remove flash node by addr",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(_ *cobra.Command, args []string) (err error) {
			if !optYes {
				fmt.Printf("decommission flashnode:[%v]\n", args[0])
				stdout("\nConfirm (yes/no)[no]: ")
				var userConfirm string
				_, _ = fmt.Scanln(&userConfirm)
				if userConfirm != "yes" {
					err = fmt.Errorf("Abort by user.\n")
					return
				}
			}
			if name == "" {
				name = proto.DefaultTopoName
			}
			result, err := client.NodeAPI().RemoveFlashNodeByTopo(args[0], name)
			if err != nil {
				return
			}
			stdoutlnf("decommission flashnode:%s %s", args[0], result)
			return
		},
	}
	cmd.Flags().BoolVarP(&optYes, "yes", "y", false, "Answer yes for all questions")
	cmd.Flags().StringVarP(&name, "topoName", "n", proto.IdleTopoName, "flash topology name")
	return cmd
}

func newCmdFlashNodeRemoveAllInactive(client *master.MasterClient) *cobra.Command {
	var optYes bool
	var name string
	cmd := &cobra.Command{
		Use:   "removeAllInactive",
		Short: "remove all inactive flash nodes",
		Args:  cobra.MinimumNArgs(0),
		RunE: func(_ *cobra.Command, args []string) (err error) {
			if !optYes {
				fmt.Printf("remove all inactive flash nodes")
				stdout("\nConfirm (yes/no)[no]: ")
				var userConfirm string
				_, _ = fmt.Scanln(&userConfirm)
				if userConfirm != "yes" {
					err = fmt.Errorf("Abort by user.\n")
					return
				}
			}
			var rmNodes []string
			if name == "" {
				name = proto.DefaultTopoName
			}
			rmNodes, err = client.NodeAPI().RemoveAllInactiveFlashNodesByTopo(name)
			if err != nil {
				return
			}

			stdoutlnf("total remove %v flash nodes", len(rmNodes))
			for _, rmNode := range rmNodes {
				stdoutlnf("remove flashnode:%s", rmNode)
			}
			return
		},
	}
	cmd.Flags().BoolVarP(&optYes, "yes", "y", false, "Answer yes for all questions")
	cmd.Flags().StringVarP(&name, "topoName", "n", proto.DefaultTopoName, "flash topology name")
	return cmd
}

func newCmdFlashNodeGet(client *master.MasterClient) *cobra.Command {
	var name string
	cmd := &cobra.Command{
		Use:   CliOpInfo + _flashnodeAddr,
		Short: "get flash node by addr",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			if name == "" {
				name = proto.DefaultTopoName
			}
			fn, err := client.NodeAPI().GetFlashNodeByTopo(args[0], name)
			if err != nil {
				return
			}
			stdoutln(formatFlashNodeView(&fn))
			return
		},
	}
	cmd.Flags().StringVarP(&name, "topoName", "n", proto.DefaultTopoName, "flash topology name")
	return cmd
}

func newCmdFlashNodeList(client *master.MasterClient) *cobra.Command {
	var name string
	var showAllTopo bool
	var active int
	cmd := &cobra.Command{
		Use:   CliOpList,
		Short: "list all flash nodes or [active true/false] flash nodes",
		Args:  cobra.MinimumNArgs(0),
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			var filterStr string
			switch active {
			case 1:
				filterStr = "active:true"
			default:
				filterStr = "all"
			}
			if name == "" {
				name = proto.DefaultTopoName
			}
			zoneFlashNodes, err := client.NodeAPI().ListFlashNodesByTopo(active, name, showAllTopo)
			if err != nil {
				return
			}
			stdoutln(fmt.Sprintf("[FlashNodes] %s, showAllTopo[%v]", filterStr, showAllTopo))
			tbl := table{formatFlashNodeViewTableTitle}
			for _, flashNodeViewInfos := range zoneFlashNodes {
				tbl = showFlashNodesView(flashNodeViewInfos, true, nil, tbl)
			}
			stdoutln(alignTable(tbl...))
			return
		},
	}
	cmd.Flags().StringVarP(&name, "topoName", "n", proto.DefaultTopoName, "flash topology name")
	cmd.Flags().BoolVar(&showAllTopo, "showAllTopo", false, "list flash nodes across all topologies (default false)")
	cmd.Flags().IntVar(&active, "active", -1, "filter flash nodes by activity: 1(true), -1(all)")
	return cmd
}

func newCmdFlashNodeHTTPStat(client *master.MasterClient) *cobra.Command {
	var name string
	cmd := &cobra.Command{
		Use:   "httpStat" + _flashnodeAddr,
		Short: "show flashnode stat",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(_ *cobra.Command, args []string) (err error) {
			// check flashnode whether exist
			if name == "" {
				name = proto.DefaultTopoName
			}
			_, err = client.NodeAPI().GetFlashNodeByTopo(args[0], name)
			if err != nil {
				return
			}
			stat, err := httpclient.New().Addr(addr2Prof(args[0])).FlashNode().Stat()
			if err != nil {
				return
			}
			stdoutln(formatIndent(stat))
			return
		},
	}
	cmd.Flags().StringVarP(&name, "topoName", "n", proto.DefaultTopoName, "flash topology name")
	return cmd
}

func newCmdFlashNodeHTTPStatAll(client *master.MasterClient) *cobra.Command {
	var name string
	cmd := &cobra.Command{
		Use:   "httpStatAll" + _flashnodeAddr,
		Short: "show flashnode stat all(key with expired time)",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(_ *cobra.Command, args []string) (err error) {
			// check flashnode whether exist
			if name == "" {
				name = proto.DefaultTopoName
			}
			_, err = client.NodeAPI().GetFlashNodeByTopo(args[0], name)
			if err != nil {
				return
			}
			stat, err := httpclient.New().Addr(addr2Prof(args[0])).FlashNode().StatAll()
			if err != nil {
				return
			}
			stdoutln(formatIndent(stat))
			return
		},
	}
	cmd.Flags().StringVarP(&name, "topoName", "n", proto.DefaultTopoName, "flash topology name")
	return cmd
}

func newCmdFlashNodeHTTPSlotStat(client *master.MasterClient) *cobra.Command {
	var name string
	cmd := &cobra.Command{
		Use:   "httpSlotStat" + _flashnodeAddr,
		Short: "show flashnode slot stat",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(_ *cobra.Command, args []string) (err error) {
			// check flashnode whether exist
			if name == "" {
				name = proto.DefaultTopoName
			}
			_, err = client.NodeAPI().GetFlashNodeByTopo(args[0], name)
			if err != nil {
				return
			}
			stat, err := httpclient.New().Addr(addr2Prof(args[0])).FlashNode().SlotStat()
			if err != nil {
				return
			}

			sort.SliceStable(stat.SlotStat, func(i, j int) bool {
				return stat.SlotStat[i].SlotId < stat.SlotStat[j].SlotId
			})
			stdout("%v\n", formatFlashNodeSlotStat(&stat))
			return
		},
	}
	cmd.Flags().StringVarP(&name, "topoName", "n", proto.DefaultTopoName, "flash topology name")
	return cmd
}

func newCmdFlashNodeHTTPEvict(client *master.MasterClient) *cobra.Command {
	var name string
	cmd := &cobra.Command{
		Use:   "httpEvict" + _flashnodeAddr + " [volume]",
		Short: "evict cache in flashnode",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(_ *cobra.Command, args []string) (err error) {
			addr := args[0]
			// check flashnode whether exist
			if name == "" {
				name = proto.DefaultTopoName
			}
			_, err = client.NodeAPI().GetFlashNodeByTopo(addr, name)
			if err != nil {
				return
			}
			if len(args) == 1 {
				if err = httpclient.New().Addr(addr2Prof(addr)).FlashNode().EvictAll(); err == nil {
					stdoutlnf("%s evicts all [OK]", addr)
				}
				return
			}
			volume := args[1]
			if err = httpclient.New().Addr(addr2Prof(addr)).FlashNode().EvictVol(volume); err == nil {
				stdoutlnf("%s evicts volume(%s) [OK]", addr, volume)
			}
			return
		},
	}
	cmd.Flags().StringVarP(&name, "topoName", "n", proto.DefaultTopoName, "flash topology name")
	return cmd
}

func newCmdFlashNodeHTTPInactiveDisk(client *master.MasterClient) *cobra.Command {
	var name string
	cmd := &cobra.Command{
		Use:   "httpInactiveDisk" + _flashnodeAddr + " [dataPath]",
		Short: "inactive the disk in flashnode",
		Args:  cobra.MinimumNArgs(2),
		RunE: func(_ *cobra.Command, args []string) (err error) {
			addr := args[0]
			// check flashnode whether exist
			if name == "" {
				name = "default"
			}
			_, err = client.NodeAPI().GetFlashNodeByTopo(addr, name)
			if err != nil {
				return
			}
			dataPath := args[1]
			if err = httpclient.New().Addr(addr2Prof(addr)).FlashNode().InactiveDisk(dataPath); err == nil {
				stdoutlnf("%s inactives dataPath(%s) [OK]", addr, dataPath)
			}
			return
		},
	}
	cmd.Flags().StringVarP(&name, "topoName", "n", "default", "flash topology name")
	return cmd
}

func showFlashNodesView(flashNodeViewInfos []*proto.FlashNodeViewInfo, showStat bool, groupStats map[uint64]string, tbl table) table {
	sort.Slice(flashNodeViewInfos, func(i, j int) bool {
		if flashNodeViewInfos[i].Region == flashNodeViewInfos[j].Region {
			return flashNodeViewInfos[i].ID < flashNodeViewInfos[j].ID
		}
		return flashNodeViewInfos[i].Region < flashNodeViewInfos[j].Region
	})
	var groupActiveInfo string
	for _, fn := range flashNodeViewInfos {
		groupActiveInfo = ""
		nodeInfo := arow(fn.ZoneName, fn.Region, fn.ID, fn.Addr, formatYesNo(fn.IsActive), formatYesNo(fn.IsEnable),
			fn.FlashGroupID, fn.FlashNodeTopoName, formatTimeToString(fn.ReportTime))
		if groupStats != nil {
			if v, ok := groupStats[fn.FlashGroupID]; ok {
				groupActiveInfo = v
			}
			nodeInfo = arow(fn.ZoneName, fn.Region, fn.ID, fn.Addr, formatYesNo(fn.IsActive), formatYesNo(fn.IsEnable),
				fn.FlashGroupID, groupActiveInfo, fn.FlashNodeTopoName, formatTimeToString(fn.ReportTime))
		}
		if !showStat {
			tbl = tbl.append(nodeInfo)
			continue
		}
		if len(fn.DiskStat) == 0 {
			nodeInfo = append(nodeInfo, "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A")
			tbl = tbl.append(nodeInfo)
			continue
		}
		for index, stat := range fn.DiskStat {
			dataPath, hitRate, evicts, limit, maxAlloc, hasAlloc, num, status := "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A"
			if fn.IsActive && fn.IsEnable {
				dataPath = stat.DataPath
				hitRate = fmt.Sprintf("%.2f%%", stat.HitRate*100)
				evicts = strconv.Itoa(stat.Evicts)
				limit = strconv.FormatUint(uint64(stat.ReadRps), 10)
				maxAlloc = strconv.FormatInt(stat.MaxAlloc, 10)
				hasAlloc = strconv.FormatInt(stat.HasAlloc, 10)
				num = strconv.Itoa(stat.KeyNum)
				status = strconv.Itoa(stat.Status)
			}
			if index != 0 {
				if groupStats != nil {
					// pre-stat columns: Zone, Region, ID, Address, Active, Enable, FlashGroupID, GroupStatus, TopoName, ReportTime
					nodeInfo = arow("", "", "", "", "", "", "", "", "", "")
				} else {
					// pre-stat columns: Zone, Region, ID, Address, Active, Enable, FlashGroupID, TopoName, ReportTime
					nodeInfo = arow("", "", "", "", "", "", "", "", "")
				}
			}
			nodeInfo = append(nodeInfo, dataPath, hitRate, evicts, limit, maxAlloc, hasAlloc, num, status)
			tbl = tbl.append(nodeInfo)
		}
	}
	return tbl
}

// TODO: mandatory design prof http port is service port+1
func addr2Prof(addr string) string {
	arr := strings.SplitN(addr, ":", 2)
	p, _ := strconv.ParseUint(arr[1], 10, 64)
	return fmt.Sprintf("%s:%d", arr[0], p+1)
}
