package cmd

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/http_client"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/spf13/cobra"
	"math"
	"sort"
	"strconv"
	"strings"
)

const (
	cmdFlashGroupUse   = "flashGroup [COMMAND]"
	cmdFlashGroupShort = "cluster flashGroup info"
)

func newFlashGroupCommand(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   cmdFlashGroupUse,
		Short: cmdFlashGroupShort,
	}
	cmd.AddCommand(
		newListFlashGroupsCmd(client),
		newFlashGroupCreateCmd(client),
		newFlashGroupGetCmd(client),
		newFlashGroupSetCmd(client),
		newFlashGroupRemoveCmd(client),
		newFlashGroupAddFlashNodeCmd(client),
		newFlashGroupRemoveFlashNodeCmd(client),
	)
	return cmd
}

type slotInfo struct {
	fgID    uint64
	slot    uint32
	percent float64
}

func newListFlashGroupsCmd(client *master.MasterClient) *cobra.Command {
	var (
		isActive      bool
		showSortSlots bool
	)
	var cmd = &cobra.Command{
		Use:   CliOpList + " [IsActive] ",
		Short: "list active(true) or inactive(false) flash groups",
		Args:  cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			listAllStatus := true
			if len(args) != 0 {
				listAllStatus = false
				if isActive, err = strconv.ParseBool(args[0]); err != nil {
					return
				}
			}
			fgView, err := client.AdminAPI().ListFlashGroups(isActive, listAllStatus)
			if err != nil {
				return
			}
			var row string
			stdout("[Flash Groups]\n")
			stdout("%v\n", formatFlashGroupViewHeader())
			sort.Slice(fgView.FlashGroups, func(i, j int) bool {
				return fgView.FlashGroups[i].ID < fgView.FlashGroups[j].ID
			})
			sortSlots := make([]*slotInfo, 0)
			for _, group := range fgView.FlashGroups {
				sort.Slice(group.Slots, func(i, j int) bool {
					return group.Slots[i] < group.Slots[j]
				})
				for _, slot := range group.Slots {
					sortSlots = append(sortSlots, &slotInfo{
						fgID: group.ID,
						slot: slot,
					})
				}
				row = fmt.Sprintf(formatFlashGroupViewPattern, group.ID, group.Status, group.FlashNodeCount, len(group.Slots))
				stdout("%v\n", row)
			}

			if showSortSlots != true {
				return
			}

			sort.Slice(sortSlots, func(i, j int) bool {
				return sortSlots[i].slot < sortSlots[j].slot
			})

			stdout("sortSlots:\n")
			for i, info := range sortSlots {
				if i < len(sortSlots)-1 {
					info.percent = float64(sortSlots[i+1].slot-info.slot) * 100 / math.MaxUint32
				} else {
					info.percent = float64(math.MaxUint32-info.slot) * 100 / math.MaxUint32
				}
				stdout("num:%v slot:%v fg:%v percent:%0.5f%% \n", i+1, info.slot, info.fgID, info.percent)
			}
		},
	}
	cmd.Flags().BoolVar(&showSortSlots, "showSortSlots", false, fmt.Sprintf("show all fg sort slots"))
	return cmd
}

func newFlashGroupCreateCmd(client *master.MasterClient) *cobra.Command {
	var (
		optSlots string
	)
	var cmd = &cobra.Command{
		Use:   CliOpCreate,
		Short: "create a new flash group",
		Args:  cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			fgView, err := client.AdminAPI().CreateFlashGroup(optSlots)
			if err != nil {
				return
			}
			stdout("[Flash Group info]\n")
			sort.Slice(fgView.Slots, func(i, j int) bool {
				return fgView.Slots[i] < fgView.Slots[j]
			})
			stdout(formatFlashGroupDetail(fgView))
		},
	}
	cmd.Flags().StringVar(&optSlots, CliFlagGroupSlots, "", fmt.Sprintf("set group slots, --slots=slot1,slot2,..."))
	return cmd
}

func newFlashGroupGetCmd(client *master.MasterClient) *cobra.Command {
	var (
		flashGroupID   uint64
		showFlashNodes bool
	)
	var cmd = &cobra.Command{
		Use:   CliOpInfo + " [FlashGroupID]  [ShowFlashNodes] ",
		Short: "get flash group by id",
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			flashGroupID, err = strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return
			}
			fgView, err := client.AdminAPI().GetFlashGroup(flashGroupID)
			if err != nil {
				return
			}
			stdout("[Flash Group info]\n")
			sort.Slice(fgView.Slots, func(i, j int) bool {
				return fgView.Slots[i] < fgView.Slots[j]
			})
			stdout(formatFlashGroupDetail(fgView))

			if len(args) <= 1 {
				return
			}
			if showFlashNodes, err = strconv.ParseBool(args[1]); err != nil {
				return
			}
			if showFlashNodes != true {
				return
			}
			stdout("[Flash nodes]\n")
			stdout("%v\n", formatFlashNodeViewTableHeader())
			var row string
			for _, flashNodeViewInfos := range fgView.ZoneFlashNodes {
				for _, fn := range flashNodeViewInfos {
					var (
						hitRate = "100%"
						evicts  = 0
						limit   = uint64(0)
					)
					stat, err1 := getFlashNodeStat(fn.Addr, client.FlashNodeProfPort)
					if err1 == nil {
						hitRate = fmt.Sprintf("%.2f%%", stat.CacheStatus.HitRate*100)
						evicts = stat.CacheStatus.Evicts
						limit = stat.NodeLimit
					}
					row = fmt.Sprintf(flashNodeViewTableRowPattern, fn.ZoneName, fn.ID, fn.Addr, fn.Version,
						formatYesNo(fn.IsActive), fn.FlashGroupID, hitRate, evicts, limit, formatTime(fn.ReportTime.Unix()), fn.IsEnable)
					stdout("%v\n", row)
				}
			}
		},
	}
	return cmd
}

func getFlashNodeStat(host string, port uint16) (*proto.FlashNodeStat, error) {
	fnClient := http_client.NewFlashClient(fmt.Sprintf("%v:%v", strings.Split(host, ":")[0], port), false)
	return fnClient.GetStat()
}
func newFlashGroupSetCmd(client *master.MasterClient) *cobra.Command {
	var (
		flashGroupID uint64
		isActive     bool
	)
	var cmd = &cobra.Command{
		Use:   CliOpSet + " [FlashGroupID] [IsActive] ",
		Short: "set flash group to active or inactive by id",
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			flashGroupID, err = strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return
			}
			if isActive, err = strconv.ParseBool(args[1]); err != nil {
				return
			}
			fgView, err := client.AdminAPI().SetFlashGroup(flashGroupID, isActive)
			if err != nil {
				return
			}
			stdout("[Flash Group info]\n")
			sort.Slice(fgView.Slots, func(i, j int) bool {
				return fgView.Slots[i] < fgView.Slots[j]
			})
			stdout(formatFlashGroupDetail(fgView))
		},
	}
	return cmd
}

func newFlashGroupRemoveCmd(client *master.MasterClient) *cobra.Command {
	var (
		flashGroupID uint64
	)
	var cmd = &cobra.Command{
		Use:   CliOpDelete + " [FlashGroupID] ",
		Short: "remove flash group by id",
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			flashGroupID, err = strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return
			}
			result, err := client.AdminAPI().RemoveFlashGroup(flashGroupID)
			if err != nil {
				return
			}
			stdout("%v\n", result)
		},
	}
	return cmd
}

func newFlashGroupAddFlashNodeCmd(client *master.MasterClient) *cobra.Command {
	var (
		flashGroupID uint64
		optAddr      string
		optZoneName  string
		optCount     int
	)
	var cmd = &cobra.Command{
		Use:   "addNode [FlashGroupID] ",
		Short: "add flash node to given flash group",
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err    error
				fgView proto.FlashGroupAdminView
			)
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			flashGroupID, err = strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return
			}
			if optAddr != "" {
				fgView, err = client.AdminAPI().FlashGroupAddFlashNode(flashGroupID, 0, "", optAddr)
			} else if optZoneName != "" && optCount > 0 {
				fgView, err = client.AdminAPI().FlashGroupAddFlashNode(flashGroupID, optCount, optZoneName, "")
			} else {
				err = fmt.Errorf("addr or zonename and count should not be empty")
				return
			}
			if err != nil {
				return
			}
			stdout("[Flash Group info]\n")
			sort.Slice(fgView.Slots, func(i, j int) bool {
				return fgView.Slots[i] < fgView.Slots[j]
			})
			stdout(formatFlashGroupDetail(fgView))
		},
	}
	cmd.Flags().StringVar(&optAddr, CliFlagAddress, "", fmt.Sprintf("Add flash node of given addr"))
	cmd.Flags().StringVar(&optZoneName, CliFlagZoneName, "", fmt.Sprintf("Add flash node from given zone"))
	cmd.Flags().IntVar(&optCount, CliFlagCount, 0, fmt.Sprintf("Add given count flash node from zone"))
	return cmd
}

func newFlashGroupRemoveFlashNodeCmd(client *master.MasterClient) *cobra.Command {
	var (
		flashGroupID uint64
		optAddr      string
		optZoneName  string
		optCount     int
	)
	var cmd = &cobra.Command{
		Use:   "deleteNode [FlashGroupID] ",
		Short: "delete flash node to given flash group",
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err    error
				fgView proto.FlashGroupAdminView
			)
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			flashGroupID, err = strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return
			}
			if optAddr != "" {
				fgView, err = client.AdminAPI().FlashGroupRemoveFlashNode(flashGroupID, 0, "", optAddr)
			} else if optZoneName != "" && optCount > 0 {
				fgView, err = client.AdminAPI().FlashGroupRemoveFlashNode(flashGroupID, optCount, optZoneName, "")
			} else {
				err = fmt.Errorf("addr or zonename and count should not be empty")
				return
			}
			if err != nil {
				return
			}
			stdout("[Flash Group info]\n")
			sort.Slice(fgView.Slots, func(i, j int) bool {
				return fgView.Slots[i] < fgView.Slots[j]
			})
			stdout(formatFlashGroupDetail(fgView))
		},
	}
	cmd.Flags().StringVar(&optAddr, CliFlagAddress, "", fmt.Sprintf("remove flash node of given addr"))
	cmd.Flags().StringVar(&optZoneName, CliFlagZoneName, "", fmt.Sprintf("remove flash node from given zone"))
	cmd.Flags().IntVar(&optCount, CliFlagCount, 0, fmt.Sprintf("remove given count flash node from zone"))
	return cmd
}
