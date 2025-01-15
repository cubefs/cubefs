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

		newCmdFlashNodeHTTPStat(client),
		newCmdFlashNodeHTTPStatAll(client),
		newCmdFlashNodeHTTPEvict(client),
	)
	return cmd
}

func newCmdFlashNodeSet(client *master.MasterClient) *cobra.Command {
	return &cobra.Command{
		Use:   CliOpSet + _flashnodeAddr + " [IsEnable]",
		Short: "set flash node enable or not",
		Args:  cobra.MinimumNArgs(2),
		RunE: func(_ *cobra.Command, args []string) (err error) {
			addr := args[0]
			enable, err := strconv.ParseBool(args[1])
			if err != nil {
				return
			}
			if err = client.NodeAPI().SetFlashNode(addr, enable); err != nil {
				return
			}
			stdoutlnf("set flashnode:%s enable:%v success", addr, enable)
			return
		},
	}
}

func newCmdFlashNodeRemove(client *master.MasterClient) *cobra.Command {
	return &cobra.Command{
		Use:   CliOpRemove + _flashnodeAddr,
		Short: "remove flash node by addr",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(_ *cobra.Command, args []string) (err error) {
			result, err := client.NodeAPI().RemoveFlashNode(args[0])
			if err != nil {
				return
			}
			stdoutlnf("decommission flashnode:%s %s", args[0], result)
			return
		},
	}
}

func newCmdFlashNodeGet(client *master.MasterClient) *cobra.Command {
	return &cobra.Command{
		Use:   CliOpInfo + _flashnodeAddr,
		Short: "get flash node by addr",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			fn, err := client.NodeAPI().GetFlashNode(args[0])
			if err != nil {
				return
			}
			stdoutln(formatFlashNodeView(&fn))
			return
		},
	}
}

func newCmdFlashNodeList(client *master.MasterClient) *cobra.Command {
	var showAllFlashNodes bool
	cmd := &cobra.Command{
		Use:   CliOpList,
		Short: "list all flash nodes",
		Args:  cobra.MinimumNArgs(0),
		RunE: func(cmd *cobra.Command, args []string) (err error) {
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
			return
		},
	}
	cmd.Flags().BoolVar(&showAllFlashNodes, "all", true, "show all flashnodes contain inactive and not enabled")
	return cmd
}

func newCmdFlashNodeHTTPStat(client *master.MasterClient) *cobra.Command {
	return &cobra.Command{
		Use:   "httpStat" + _flashnodeAddr,
		Short: "show flashnode stat",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(_ *cobra.Command, args []string) (err error) {
			// check flashnode whether exist
			_, err = client.NodeAPI().GetFlashNode(args[0])
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
}

func newCmdFlashNodeHTTPStatAll(client *master.MasterClient) *cobra.Command {
	return &cobra.Command{
		Use:   "httpStatAll" + _flashnodeAddr,
		Short: "show flashnode stat all(key with expired time)",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(_ *cobra.Command, args []string) (err error) {
			// check flashnode whether exist
			_, err = client.NodeAPI().GetFlashNode(args[0])
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
}

func newCmdFlashNodeHTTPEvict(client *master.MasterClient) *cobra.Command {
	return &cobra.Command{
		Use:   "httpEvict" + _flashnodeAddr + " [volume]",
		Short: "evict cache in flashnode",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(_ *cobra.Command, args []string) (err error) {
			addr := args[0]
			// check flashnode whether exist
			_, err = client.NodeAPI().GetFlashNode(addr)
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
}

func showFlashNodesView(flashNodeViewInfos []*proto.FlashNodeViewInfo, showStat bool, tbl table) table {
	sort.Slice(flashNodeViewInfos, func(i, j int) bool {
		return flashNodeViewInfos[i].ID < flashNodeViewInfos[j].ID
	})
	for _, fn := range flashNodeViewInfos {
		if !showStat {
			tbl = tbl.append(arow(fn.ZoneName, fn.ID, fn.Addr, formatYesNo(fn.IsActive), formatYesNo(fn.IsEnable),
				fn.FlashGroupID, formatTimeToString(fn.ReportTime)))
			continue
		}

		for index, stat := range fn.HeartBeatStat {
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
			if index == 0 {
				tbl = tbl.append(arow(fn.ZoneName, fn.ID, fn.Addr, formatYesNo(fn.IsActive), formatYesNo(fn.IsEnable),
					fn.FlashGroupID, formatTimeToString(fn.ReportTime), dataPath, hitRate, evicts, limit, maxAlloc, hasAlloc, num, status))
			} else {
				tbl = tbl.append(arow("", "", "", "", "",
					"", "", dataPath, hitRate, evicts, limit, maxAlloc, hasAlloc, num, status))
			}
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
