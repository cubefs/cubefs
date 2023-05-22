package cmd

import (
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/spf13/cobra"
	"sort"
)

const (
	cmdDiskUse   = "disk [COMMAND]"
	cmdDiskShort = "Manage cluster disks"
)

func newDiskCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:     cmdDiskUse,
		Short:   cmdDiskShort,
		Args:    cobra.MinimumNArgs(0),
		Aliases: []string{"disk"},
	}
	cmd.AddCommand(
		newListBadDiskCmd(client),
	)
	return cmd
}

const (
	cmdCheckBadDisksShort = "Check and list unhealthy disks"
)

func newListBadDiskCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpCheck,
		Short: cmdCheckBadDisksShort,
		Run: func(cmd *cobra.Command, args []string) {
			var (
				infos *proto.BadDiskInfos
				err   error
			)
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			if infos, err = client.AdminAPI().QueryBadDisks(); err != nil {
				return
			}
			stdout("[Unavaliable disks]:\n")
			stdout("%v\n", formatBadDiskTableHeader())

			sort.SliceStable(infos.BadDisks, func(i, j int) bool {
				return infos.BadDisks[i].Address < infos.BadDisks[i].Address
			})
			for _, disk := range infos.BadDisks {
				stdout("%v\n", formatBadDiskInfoRow(disk))
			}
		},
	}
	return cmd
}
