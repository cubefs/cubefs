package cmd

import (
	"fmt"
	"strings"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/master"
	"github.com/spf13/cobra"
)

const (
	cmdZoneUse            = "zone [command]"
	cmdZoneShort          = "Manage zones"
	cmdZoneInfoUse        = "info [Zone Name]"
	cmdZoneInfoShort      = "Show zone information"
	cmdZoneSetRegionShort = "Set region name of the zone"
	cmdZoneListShort      = "List cluster zones"
)

func newZoneCmd(mc *master.MasterClient) *cobra.Command {
	client = mc
	var cmd = &cobra.Command{
		Use:   cmdZoneUse,
		Short: cmdZoneShort,
		Args:  cobra.MinimumNArgs(1),
	}
	cmd.AddCommand(
		newZoneListCmd(client),
		newZoneInfoCmd(client),
		newZoneSetRegionCmd(client),
	)
	return cmd
}

func newZoneInfoCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   cmdZoneInfoUse,
		Short: cmdZoneInfoShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err error
				zv  *proto.ZoneView
			)
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			var zoneName = args[0]
			zoneViews, err := client.AdminAPI().ZoneList()
			if err != nil {
				return
			}
			for _, zoneView := range zoneViews {
				if zoneView.Name == zoneName {
					zv = zoneView
					break
				}
			}
			if zv != nil {
				stdout("ZoneInfo:\n%s\n", formatZoneView(zv))
			} else {
				stdout("ZoneInfo:%s not exist.\n", zoneName)
			}
			return
		},
	}
	return cmd
}

func newZoneListCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpList,
		Short: cmdZoneListShort,
		Args:  cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			zoneViews, err := client.AdminAPI().ZoneList()
			if err != nil {
				return
			}
			stdout("%v\n", zoneInfoTableHeader)
			for _, zoneView := range zoneViews {
				stdout("%v\n", formatZoneInfoTableRow(zoneView))
			}
			return
		},
	}
	return cmd
}

func newZoneSetRegionCmd(client *master.MasterClient) *cobra.Command {
	var (
		confirmString = new(strings.Builder)
		optYes        bool
	)
	var cmd = &cobra.Command{
		Use:   "setRegion [Zone Name] [Region Name]",
		Short: cmdZoneSetRegionShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var zoneName = args[0]
			var regionName = args[1]
			var isChange = false
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			confirmString.WriteString("Zone region name change:\n")
			confirmString.WriteString(fmt.Sprintf("  Name                : %v\n", zoneName))
			if regionName != "" {
				isChange = true
				confirmString.WriteString(fmt.Sprintf("  region name         : %s\n", regionName))
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
			err = client.AdminAPI().SetZoneRegion(zoneName, regionName)
			if err != nil {
				return
			}
			stdout("Zone region name has been set successfully.\n")
			return
		},
	}
	cmd.Flags().BoolVarP(&optYes, "yes", "y", false, "Answer yes for all questions")
	return cmd
}
