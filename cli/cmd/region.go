package cmd

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/spf13/cobra"
)

const (
	cmdRegionUse         = "region [command]"
	cmdRegionShort       = "Manage regions"
	cmdRegionInfoUse     = "info [REGION NAME]"
	cmdRegionInfoShort   = "Show region information"
	cmdCreateRegionUse   = "create [REGION NAME] "
	cmdCreateRegionShort = "Create a new region with the region type"
	cmdRegionSetShort    = "Set configuration of the region"
	cmdRegionListShort   = "List cluster regions"
)

func newRegionCmd(mc *master.MasterClient) *cobra.Command {
	client = mc
	var cmd = &cobra.Command{
		Use:   cmdRegionUse,
		Short: cmdRegionShort,
		Args:  cobra.MinimumNArgs(1),
	}
	cmd.AddCommand(
		newRegionListCmd(client),
		newRegionInfoCmd(client),
		newCreateRegionCmd(client),
		newRegionSetCmd(client),
	)
	return cmd
}

func newRegionListCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpList,
		Short: cmdRegionListShort,
		Args:  cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			regionViews, err := client.AdminAPI().RegionList()
			if err != nil {
				return
			}
			stdout("%v\n", regionInfoTableHeader)
			for _, regionView := range regionViews {
				stdout("%v\n", formatRegionInfoTableRow(regionView))
			}
			return
		},
	}
	return cmd
}

func newRegionInfoCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   cmdRegionInfoUse,
		Short: cmdRegionInfoShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			var regionName = args[0]
			var rv *proto.RegionView
			if rv, err = client.AdminAPI().GetRegionView(regionName); err != nil {
				return
			}
			stdout("RegionInfo:\n%s\n", formatRegionView(rv))
			return
		},
	}
	return cmd
}

func newCreateRegionCmd(client *master.MasterClient) *cobra.Command {
	var (
		optRegionType string
	)
	var cmd = &cobra.Command{
		Use:   cmdCreateRegionUse,
		Short: cmdCreateRegionShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			var regionName = args[0]
			regionTypeUint, err := strconv.ParseUint(optRegionType, 10, 64)
			if err != nil {
				return
			}
			regionType := proto.RegionType(regionTypeUint)
			if regionType != proto.SlaveRegion && regionType != proto.MasterRegion {
				err = fmt.Errorf("region type should be %d(%s) or %d(%s)",
					proto.MasterRegion, proto.MasterRegion, proto.SlaveRegion, proto.SlaveRegion)
				return
			}
			if err = client.AdminAPI().CreateRegion(regionName, uint8(regionType)); err != nil {
				return
			}
			stdout("add region[%s] type[%d(%s)] successfully\n", regionName, regionType, regionType)
			return
		},
	}
	cmd.Flags().StringVar(&optRegionType, CliFlagRegionType, "",
		"Set region type(1[master-region] or 2[slave-region] )")
	return cmd
}

func newRegionSetCmd(client *master.MasterClient) *cobra.Command {
	var (
		optYes        bool
		optRegionType string
		rv            *proto.RegionView
		confirmString = new(strings.Builder)
	)
	var cmd = &cobra.Command{
		Use:   CliOpSet + " [REGION NAME]",
		Short: cmdRegionSetShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var regionName = args[0]
			var isChange = false
			defer func() {
				if err != nil {
					errout("Error: %v\n", err)
				}
			}()
			if rv, err = client.AdminAPI().GetRegionView(regionName); err != nil {
				return
			}
			confirmString.WriteString("Region configuration changes:\n")
			confirmString.WriteString(fmt.Sprintf("  Name                : %v\n", rv.Name))

			if optRegionType != "" {
				isChange = true
				var regionTypeUint uint64
				if regionTypeUint, err = strconv.ParseUint(optRegionType, 10, 64); err != nil {
					return
				}
				regionType := proto.RegionType(regionTypeUint)
				if regionType != proto.SlaveRegion && regionType != proto.MasterRegion {
					err = fmt.Errorf("region type should be %d(%s) or %d(%s)",
						proto.MasterRegion, proto.MasterRegion, proto.SlaveRegion, proto.SlaveRegion)
					return
				}
				confirmString.WriteString(fmt.Sprintf("  region type         : %d(%s) -> %d(%s)\n", rv.RegionType, rv.RegionType, regionType, regionType))
				rv.RegionType = regionType
			} else {
				confirmString.WriteString(fmt.Sprintf("  region type         : %d(%s)\n", rv.RegionType, rv.RegionType))
			}

			if !isChange {
				stdout("No changes has been set.\n")
				return
			}
			// ask user for confirm
			if !optYes {
				stdout(confirmString.String())
				stdout("\nConfirm (yes/no)[yes]: ")
				var userConfirm string
				_, _ = fmt.Scanln(&userConfirm)
				if userConfirm != "yes" && len(userConfirm) != 0 {
					err = fmt.Errorf("Abort by user.\n")
					return
				}
			}
			err = client.AdminAPI().UpdateRegion(rv.Name, uint8(rv.RegionType))
			if err != nil {
				return
			}
			stdout("Region configuration has been set successfully.\n")
			return
		},
	}
	cmd.Flags().BoolVarP(&optYes, "yes", "y", false, "Answer yes for all questions")
	cmd.Flags().StringVar(&optRegionType, CliFlagRegionType, "",
		"Set region type(1[master-region] or 2[slave-region] )")
	return cmd
}
