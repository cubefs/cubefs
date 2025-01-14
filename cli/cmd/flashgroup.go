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
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/spf13/cobra"
)

const _flashgroupID = " [FlashGroupID]"

type slotInfo struct {
	fgID    uint64
	slot    uint32
	percent float64
}

func newFlashGroupCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "flashgroup [COMMAND]",
		Short: "cluster flashgroup management",
	}
	cmd.AddCommand(
		newCmdFlashGroupTurn(client),
		newCmdFlashGroupCreate(client),
		newCmdFlashGroupSet(client),
		newCmdFlashGroupRemove(client),
		newCmdFlashGroupNodeAdd(client),
		newCmdFlashGroupNodeRemove(client),
		newCmdFlashGroupGet(client),
		newCmdFlashGroupList(client),
		newCmdFlashGroupClient(client),
		newCmdFlashGroupSearch(client),
		newCmdFlashGroupGraph(client),
	)
	return cmd
}

func newCmdFlashGroupTurn(client *master.MasterClient) *cobra.Command {
	return &cobra.Command{
		Use:   "turn [IsEnable]",
		Short: "turn flash group cache",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			enabled, err := strconv.ParseBool(args[0])
			if err != nil {
				return
			}
			result, err := client.AdminAPI().TurnFlashGroup(enabled)
			if err != nil {
				return
			}
			stdoutln(result)
			return
		},
	}
}

func newCmdFlashGroupCreate(client *master.MasterClient) *cobra.Command {
	var optSlots string
	var optWeight int
	cmd := &cobra.Command{
		Use:   CliOpCreate,
		Short: "create a new flash group",
		Args:  cobra.MinimumNArgs(0),
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			if optSlots != "" {
				numbers := strings.Split(optSlots, ",")
				for _, numStr := range numbers {
					_, err = strconv.Atoi(numStr)
					if err != nil {
						return
					}
				}
			}
			if optWeight <= 0 || optWeight > proto.FlashGroupMaxWeight {
				err = fmt.Errorf("param weight(%v) must greater than 0 and not greater than %v", optWeight, proto.FlashGroupMaxWeight)
				return
			}

			fgView, err := client.AdminAPI().CreateFlashGroup(optSlots, optWeight)
			if err != nil {
				return
			}
			stdoutln(formatFlashGroupView(&fgView))
			return
		},
	}
	cmd.Flags().StringVar(&optSlots, "slots", "", "set group in which slots, --slots=slot1,slot2,...")
	cmd.Flags().IntVar(&optWeight, "weight", proto.FlashGroupDefaultWeight, "set group weight(default 1, must 1<=weight<=30), if it was specified slots count equal to 32*weight")
	return cmd
}

func newCmdFlashGroupSet(client *master.MasterClient) *cobra.Command {
	return &cobra.Command{
		Use:   CliOpSet + _flashgroupID + " [IsActive]",
		Short: "set flash group active or not",
		Args:  cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			flashGroupID, err := parseFlashGroupID(args[0])
			if err != nil {
				return
			}
			isActive, err := strconv.ParseBool(args[1])
			if err != nil {
				return
			}
			fgView, err := client.AdminAPI().SetFlashGroup(flashGroupID, isActive)
			if err != nil {
				return
			}
			stdoutln(formatFlashGroupView(&fgView))
			return
		},
	}
}

func newCmdFlashGroupRemove(client *master.MasterClient) *cobra.Command {
	return &cobra.Command{
		Use:   CliOpRemove + _flashgroupID,
		Short: "remove flash group by id",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			flashGroupID, err := parseFlashGroupID(args[0])
			if err != nil {
				return
			}
			result, err := client.AdminAPI().RemoveFlashGroup(flashGroupID)
			if err != nil {
				return
			}
			stdoutln(result)
			return
		},
	}
}

func newCmdFlashGroupNodeAdd(client *master.MasterClient) *cobra.Command {
	var (
		optAddr     string
		optZoneName string
		optCount    int
	)
	cmd := &cobra.Command{
		Use:   "nodeAdd" + _flashgroupID,
		Short: "add flash node to given flash group",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			flashGroupID, err := parseFlashGroupID(args[0])
			if err != nil {
				return
			}
			fgView, err := client.AdminAPI().FlashGroupAddFlashNode(flashGroupID, optCount, optZoneName, optAddr)
			if err != nil {
				return
			}
			stdoutln(formatFlashGroupView(&fgView))
			return
		},
	}
	cmd.Flags().StringVar(&optAddr, CliFlagAddress, "", "add flash node of given addr")
	cmd.Flags().StringVar(&optZoneName, CliFlagZoneName, "", "add flash node from given zone")
	cmd.Flags().IntVar(&optCount, CliFlagCount, 0, "add given count flash node from zone")
	return cmd
}

func newCmdFlashGroupNodeRemove(client *master.MasterClient) *cobra.Command {
	var (
		optAddr     string
		optZoneName string
		optCount    int
	)
	cmd := &cobra.Command{
		Use:   "nodeRemove" + _flashgroupID,
		Short: "remove flash node to given flash group",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			flashGroupID, err := parseFlashGroupID(args[0])
			if err != nil {
				return
			}
			fgView, err := client.AdminAPI().FlashGroupRemoveFlashNode(flashGroupID, optCount, optZoneName, optAddr)
			if err != nil {
				return
			}
			stdoutln(formatFlashGroupView(&fgView))
			return
		},
	}
	cmd.Flags().StringVar(&optAddr, CliFlagAddress, "", "remove flash node of given addr")
	cmd.Flags().StringVar(&optZoneName, CliFlagZoneName, "", "remove flash node from given zone")
	cmd.Flags().IntVar(&optCount, CliFlagCount, 0, "remove given count flash node from zone")
	return cmd
}

func newCmdFlashGroupGet(client *master.MasterClient) *cobra.Command {
	return &cobra.Command{
		Use:   CliOpInfo + _flashgroupID + " [showHitRate ture/false] ",
		Short: "get flash group by id, default don't show hit rate",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			flashGroupID, err := parseFlashGroupID(args[0])
			if err != nil {
				return
			}
			fgView, err := client.AdminAPI().GetFlashGroup(flashGroupID)
			if err != nil {
				return
			}
			stdoutln(formatFlashGroupView(&fgView))

			showHitRate := false
			if len(args) > 1 {
				showHitRate, _ = strconv.ParseBool(args[1])
			}

			stdoutln("[Flash Nodes]")
			var tbl table
			if showHitRate {
				tbl = table{formatFlashNodeViewTableTitle}
			} else {
				tbl = table{formatFlashNodeSimpleViewTableTitle}
			}
			for _, flashNodeViewInfos := range fgView.ZoneFlashNodes {
				tbl = showFlashNodesView(flashNodeViewInfos, showHitRate, tbl)
			}
			stdoutln(alignTable(tbl...))
			return
		},
	}
}

func newCmdFlashGroupList(client *master.MasterClient) *cobra.Command {
	return &cobra.Command{
		Use:   CliOpList + " [IsActive]",
		Short: "list active or inactive flash groups",
		Args:  cobra.MinimumNArgs(0),
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			var fgView proto.FlashGroupsAdminView
			var isActive bool
			if len(args) > 0 {
				if isActive, err = strconv.ParseBool(args[0]); err != nil {
					return
				}
				fgView, err = client.AdminAPI().ListFlashGroup(isActive)
			} else {
				fgView, err = client.AdminAPI().ListFlashGroups()
			}
			if err != nil {
				return
			}
			sort.Slice(fgView.FlashGroups, func(i, j int) bool {
				return fgView.FlashGroups[i].ID < fgView.FlashGroups[j].ID
			})

			stdoutln("[Flash Groups]")
			slots := make([]*slotInfo, 0)
			tbl := table{formatFlashGroupViewTile}
			for _, group := range fgView.FlashGroups {
				sort.Slice(group.Slots, func(i, j int) bool {
					return group.Slots[i] < group.Slots[j]
				})
				for _, slot := range group.Slots {
					slots = append(slots, &slotInfo{
						fgID: group.ID,
						slot: slot,
					})
				}
				tbl = tbl.append(arow(group.ID, group.Weight, len(group.Slots), group.Status, group.FlashNodeCount))
			}
			stdoutln(alignTable(tbl...))

			sort.Slice(slots, func(i, j int) bool {
				return slots[i].slot < slots[j].slot
			})
			stdoutln("Slots:")
			for i, info := range slots {
				if i < len(slots)-1 {
					info.percent = float64(slots[i+1].slot-info.slot) * 100 / math.MaxUint32
				} else {
					info.percent = float64(math.MaxUint32-info.slot) * 100 / math.MaxUint32
				}
				stdoutlnf("num:%d slot:%d fg:%d percent:%0.5f%%", i+1, info.slot, info.fgID, info.percent)
			}
			return
		},
	}
}

func newCmdFlashGroupClient(client *master.MasterClient) *cobra.Command {
	return &cobra.Command{
		Use:   "client",
		Short: "show client response",
		Args:  cobra.MinimumNArgs(0),
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			fgv, err := client.AdminAPI().ClientFlashGroups()
			if err != nil {
				return
			}
			stdoutln("Client Response:")
			stdoutln(formatIndent(fgv))
			return
		},
	}
}

func newCmdFlashGroupSearch(client *master.MasterClient) *cobra.Command {
	return &cobra.Command{
		Use:   "search [volume] [inode] [offset]",
		Short: "search flash group by volume inode offset",
		Args:  cobra.MinimumNArgs(3),
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			volume := args[0]
			if volume == "" {
				err = fmt.Errorf("volume is empty")
				return
			}
			inode, err := strconv.ParseUint(args[1], 10, 64)
			if err != nil {
				return
			}
			offset, err := strconv.ParseUint(args[2], 10, 64)
			if err != nil {
				return
			}
			slotKey := proto.ComputeCacheBlockSlot(volume, inode, offset)

			fgView, err := client.AdminAPI().ListFlashGroups()
			if err != nil {
				return
			}
			set := make(map[uint32]struct{})
			slots := make([]slotInfo, 0)
			for _, fg := range fgView.FlashGroups {
				if fg.Status != proto.FlashGroupStatus_Active {
					continue
				}
				for _, slot := range fg.Slots {
					if _, in := set[slot]; in {
						continue
					}
					slots = append(slots, slotInfo{
						fgID: fg.ID,
						slot: slot,
					})
				}
			}
			sort.Slice(slots, func(i, j int) bool {
				return slots[i].slot < slots[j].slot
			})

			var whichGroup uint64
			for _, slot := range slots {
				if slotKey >= slot.slot {
					whichGroup = slot.fgID
				}
			}
			for _, fg := range fgView.FlashGroups {
				if fg.ID == whichGroup {
					stdoutlnf("Found in FlashGroup:%d", whichGroup)
					tbl := table{formatFlashNodeSimpleViewTableTitle}
					for _, fnNodes := range fg.ZoneFlashNodes {
						tbl = showFlashNodesView(fnNodes, false, tbl)
					}
					stdoutln(alignTable(tbl...))
					return
				}
			}
			stdoutlnf("Not found (%s %d %d) -> %d", volume, inode, offset, slotKey)
			return
		},
	}
}

func newCmdFlashGroupGraph(client *master.MasterClient) *cobra.Command {
	return &cobra.Command{
		Use:   "graph",
		Short: "show flash group and node",
		Args:  cobra.MinimumNArgs(0),
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			fgView, err := client.AdminAPI().ListFlashGroups()
			if err != nil {
				return
			}
			set := make(map[uint32]struct{})
			groups := make(map[uint64]proto.FlashGroupAdminView)
			groupn := make(map[uint64]int)
			slots := make([]slotInfo, 0)
			for _, fg := range fgView.FlashGroups {
				groups[fg.ID] = fg
				groupn[fg.ID] = 0
				for _, slot := range fg.Slots {
					if _, in := set[slot]; in {
						continue
					}
					groupn[fg.ID]++
					slots = append(slots, slotInfo{
						fgID: fg.ID,
						slot: slot,
					})
				}
			}
			sort.Slice(slots, func(i, j int) bool {
				return slots[i].slot < slots[j].slot
			})

			stdoutln("[Flash Groups]")
			tbl := table{arow("Slot", "ID", "Status", "Count", "Ref", "Proportion")}
			for idx, slot := range slots {
				g := groups[slot.fgID]
				var p string
				if idx == len(slots)-1 {
					p = proportion(slot.slot, math.MaxUint32)
				} else {
					p = proportion(slot.slot, slots[idx+1].slot)
				}
				tbl = tbl.append(arow(slot.slot, g.ID, g.Status.String(), g.FlashNodeCount, groupn[g.ID], p))
			}
			stdoutln(alignTable(tbl...))

			fnView, err := client.NodeAPI().ListFlashNodes(true)
			if err != nil {
				return
			}
			busyNodes := make([]*proto.FlashNodeViewInfo, 0)
			idleNodes := make([]*proto.FlashNodeViewInfo, 0)
			for _, nodes := range fnView {
				for _, node := range nodes {
					if node.FlashGroupID == 0 {
						idleNodes = append(idleNodes, node)
					} else {
						busyNodes = append(busyNodes, node)
					}
				}
			}
			stdoutln("[FlashNodes Busy]")
			tbl = showFlashNodesView(busyNodes, true, table{formatFlashNodeViewTableTitle})
			stdoutln(alignTable(tbl...))
			stdoutln("[FlashNodes Idle]")
			tbl = showFlashNodesView(idleNodes, true, table{formatFlashNodeViewTableTitle})
			stdoutln(alignTable(tbl...))
			return
		},
	}
}

func parseFlashGroupID(id string) (uint64, error) {
	return strconv.ParseUint(id, 10, 64)
}

const fullDot = ".................................................."

func proportion(s, e uint32) string {
	p := "."
	if n := int(float64(e-s) * float64(len(fullDot)) / float64(math.MaxUint32)); n > 0 {
		p = fullDot[:n]
	}
	return p
}
