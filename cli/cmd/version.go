package cmd

import (
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/spf13/cobra"
)

const (
	cmdVersionUse         = "version [COMMAND]"
	cmdVersionShort       = "Manage cluster volumes versions"
	cmdVersionCreateShort = "create volume version"
	cmdVersionDelShort    = "del volume version"
	cmdVersionListShort   = "list volume version"
)

func newVersionCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:     cmdVersionUse,
		Short:   cmdVersionShort,
		Args:    cobra.MinimumNArgs(0),
		Aliases: []string{"version"},
	}
	cmd.AddCommand(
		newVersionCreateCmd(client),
		newVersionDelCmd(client),
		newVersionListCmd(client),
	)
	return cmd
}

func newVersionCreateCmd(client *master.MasterClient) *cobra.Command {
	var optKeyword string
	var cmd = &cobra.Command{
		Use:     CliFlagVersionCreate,
		Short:   cmdVersionCreateShort,
		Aliases: []string{"create"},
		Run: func(cmd *cobra.Command, args []string) {
			var verList *proto.VolVersionInfoList
			var volumeName = args[0]
			var err error
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			if _, err = client.AdminAPI().CreateVersion(volumeName); err != nil {
				return
			}
			stdout("create command be received by master and it's a asynchronous command,now try get the latest list\n")
			if verList, err = client.AdminAPI().GetVerList(volumeName); err != nil {
				return
			}
			stdout("%v\n\n", volumeVersionTableHeader)
			for _, ver := range verList.VerList {
				stdout("%v\n", formatVerInfoTableRow(ver))
			}

		},
	}
	cmd.Flags().StringVar(&optKeyword, "keyword", "", "Specify keyword of volume name to filter")
	return cmd
}

func newVersionListCmd(client *master.MasterClient) *cobra.Command {
	var optKeyword string
	var cmd = &cobra.Command{
		Use:     CliFlagVersionList,
		Short:   cmdVersionListShort,
		Aliases: []string{"create"},
		Run: func(cmd *cobra.Command, args []string) {
			var volumeName = args[0]
			var verList *proto.VolVersionInfoList
			var err error
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			if verList, err = client.AdminAPI().GetVerList(volumeName); err != nil {
				return
			}
			stdout("%v\n", volumeVersionTableHeader)
			for _, ver := range verList.VerList {
				stdout("%v\n", formatVerInfoTableRow(ver))
			}
		},
	}
	cmd.Flags().StringVar(&optKeyword, "keyword", "", "Specify keyword of volume name to filter")
	return cmd
}

func newVersionDelCmd(client *master.MasterClient) *cobra.Command {
	var optKeyword string
	var cmd = &cobra.Command{
		Use:     CliFlagVersionDel,
		Short:   cmdVersionDelShort,
		Aliases: []string{"create"},
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			if err = client.AdminAPI().DeleteVersion(optKeyword); err != nil {
				return
			}
		},
	}
	cmd.Flags().StringVar(&optKeyword, "keyword", "", "Specify keyword of volume name to filter")
	return cmd
}
