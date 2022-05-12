package cmd

import (
	"encoding/json"
	"github.com/chubaofs/chubaofs/sdk/master"
	"github.com/spf13/cobra"
)

const (
	cmdIdcUse   = "idc [COMMAND]"
	cmdIdcShort = "cluster idc info"
)

func newIdcCommand(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   cmdIdcUse,
		Short: cmdIdcShort,
	}
	cmd.AddCommand(
		newIdcListCmd(client),
		newIdcCreateCmd(client),
		newIdcDeleteCmd(client),
		newIdcGetCmd(client),
		newIdcSetCmd(client),
	)
	return cmd
}

const (
	cmdGetIdcShort    = "Get Idc by name"
	cmdDeleteIdcShort = "Delete Idc by name"
	cmdCreateIdcShort = "Create Idc by name"
)

func newIdcSetCmd(client *master.MasterClient) *cobra.Command {
	var (
		idcName    string
		zoneName   string
		mediumType string
	)
	var cmd = &cobra.Command{
		Use:   CliOpSet,
		Short: cmdGetIdcShort,
		Args:  cobra.MinimumNArgs(3),
		Run: func(cmd *cobra.Command, args []string) {
			idcName = args[0]
			zoneName = args[1]
			mediumType = args[2]
			var err error
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			result, err := client.AdminAPI().SetIDC(idcName, mediumType, zoneName)
			if err != nil {
				return
			}

			stdout("%s\n", result)
		},
	}
	return cmd
}

func newIdcGetCmd(client *master.MasterClient) *cobra.Command {
	var (
		name string
	)
	var cmd = &cobra.Command{
		Use:   CliOpGet,
		Short: cmdGetIdcShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			name = args[0]
			var err error
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			idc, err := client.AdminAPI().GetIdc(name)
			if err != nil {
				return
			}
			stdout("%v\n", idcQueryDataTableHeader)
			zones, err := json.Marshal(idc.Zones)
			if err != nil {
				stdout("%v\n", formatIdcInfoTableRow(idc.Name, "zones: error"))
			} else {
				stdout("%v\n", formatIdcInfoTableRow(idc.Name, string(zones)))
			}
		},
	}
	return cmd
}

func newIdcDeleteCmd(client *master.MasterClient) *cobra.Command {
	var (
		name string
	)
	var cmd = &cobra.Command{
		Use:   CliOpDelete,
		Short: cmdDeleteIdcShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			name = args[0]
			var err error
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			result, err := client.AdminAPI().DeleteIdc(name)
			if err != nil {
				return
			}
			stdout("%v\n", result)
		},
	}
	return cmd
}

func newIdcCreateCmd(client *master.MasterClient) *cobra.Command {
	var (
		name string
	)
	var cmd = &cobra.Command{
		Use:   CliOpCreate,
		Short: cmdCreateIdcShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			name = args[0]
			var err error
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			result, err := client.AdminAPI().CreateIdc(name)

			if err != nil {
				return
			}
			stdout("%v\n", result)
		},
	}
	return cmd
}

func newIdcListCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpList,
		Short: cmdCreateIdcShort,
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			views, err := client.AdminAPI().IdcList()
			if err != nil {
				return
			}
			stdout("%v\n", idcQueryDataTableHeader)
			for _, view := range views {
				zones, err := json.Marshal(view.Zones)
				if err != nil {
					stdout("%v\n", formatIdcInfoTableRow(view.Name, "zones: error"))
				} else {
					stdout("%v\n", formatIdcInfoTableRow(view.Name, string(zones)))
				}

			}
		},
	}
	return cmd
}
