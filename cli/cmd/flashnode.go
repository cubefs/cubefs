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
	)
	return cmd
}

func newCmdFlashNodeSet(client *master.MasterClient) *cobra.Command {
	return &cobra.Command{
		Use:   CliOpSet + _flashnodeAddr + " [state]",
		Short: "set flash node enable or not",
		Args:  cobra.MinimumNArgs(2),
		Run: func(_ *cobra.Command, args []string) {
			var err error
			defer func() { errout(err) }()
			if err = client.NodeAPI().SetFlashNode(args[0], args[1]); err != nil {
				return
			}
			stdoutlnf("set flashnode:%s state:%s success", args[0], args[1])
		},
	}
}

func newCmdFlashNodeRemove(client *master.MasterClient) *cobra.Command {
	return &cobra.Command{
		Use:   CliOpRemove + _flashnodeAddr,
		Short: "remove flash node by addr",
		Args:  cobra.MinimumNArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			var err error
			defer func() { errout(err) }()
			result, err := client.NodeAPI().RemoveFlashNode(args[0])
			if err != nil {
				return
			}
			stdoutlnf("decommission flashnode:%s %s", args[0], result)
		},
	}
}

func newCmdFlashNodeGet(client *master.MasterClient) *cobra.Command {
	return &cobra.Command{
		Use:   CliOpInfo + _flashnodeAddr,
		Short: "get flash node by addr",
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() { errout(err) }()
			fn, err := client.NodeAPI().GetFlashNode(args[0])
			if err != nil {
				return
			}
			stdoutln(formatFlashNodeView(&fn))
		},
	}
}

func newCmdFlashNodeList(client *master.MasterClient) *cobra.Command {
	var showAllFlashNodes bool
	cmd := &cobra.Command{
		Use:   CliOpList,
		Short: "list all flash nodes",
		Args:  cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() { errout(err) }()
			zoneFlashNodes, err := client.NodeAPI().ListFlashNodes(showAllFlashNodes)
			if err != nil {
				return
			}
			stdoutln("[FlashNodes]")
			tbl := table{formatFlashNodeViewTableTitle}
			for _, flashNodeViewInfos := range zoneFlashNodes {
				tbl = showFlashNodesView(flashNodeViewInfos, true, tbl)
			}
			stdoutln(alignTable(tbl...))
		},
	}
	cmd.Flags().BoolVar(&showAllFlashNodes, "all", true, "show all flashnodes contain inactive and not enabled")
	return cmd
}

func showFlashNodesView(flashNodeViewInfos []*proto.FlashNodeViewInfo, showStat bool, tbl table) table {
	client := httpclient.New()
	sort.Slice(flashNodeViewInfos, func(i, j int) bool {
		return flashNodeViewInfos[i].ID < flashNodeViewInfos[j].ID
	})
	for _, fn := range flashNodeViewInfos {
		if !showStat {
			tbl = tbl.append(arow(fn.ZoneName, fn.ID, fn.Addr, formatYesNo(fn.IsActive), formatYesNo(fn.IsEnable),
				fn.FlashGroupID, formatTimeToString(fn.ReportTime)))
			continue
		}

		hitRate, evicts, limit := "N/A", "N/A", "N/A"
		if fn.IsActive && fn.IsEnable {
			if stat, e := client.WithAddr(addr2Prof(fn.Addr)).FlashNode().Stat(); e == nil {
				hitRate = fmt.Sprintf("%.2f%%", stat.CacheStatus.HitRate*100)
				evicts = strconv.Itoa(stat.CacheStatus.Evicts)
				limit = strconv.FormatUint(stat.NodeLimit, 10)
			} else {
				stdoutln(e)
			}
		}
		tbl = tbl.append(arow(fn.ZoneName, fn.ID, fn.Addr, formatYesNo(fn.IsActive), formatYesNo(fn.IsEnable),
			fn.FlashGroupID, formatTimeToString(fn.ReportTime), hitRate, evicts, limit))
	}
	return tbl
}

// TODO: mandatory design prof http port is service port+1
func addr2Prof(addr string) string {
	arr := strings.SplitN(addr, ":", 2)
	p, _ := strconv.ParseUint(arr[1], 10, 64)
	return fmt.Sprintf("%s:%d", arr[0], p+1)
}
