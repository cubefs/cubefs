package cmd

import (
	"sort"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/spf13/cobra"
)

const (
	cmdDiskUse   = "disk [COMMAND]"
	cmdDiskShort = "Manage cluster disks"
)

func newDiskCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
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
	cmd := &cobra.Command{
		Use:   CliOpCheck,
		Short: cmdCheckBadDisksShort,
		Run: func(cmd *cobra.Command, args []string) {
			var (
				infos *proto.BadDiskInfos
				err   error
			)
			defer func() {
				errout(err)
			}()
			if infos, err = client.AdminAPI().QueryBadDisks(); err != nil {
				return
			}
			stdout("[Unavaliable disks]:\n")
			stdout("%v\n", formatBadDiskTableHeader())

			sort.SliceStable(infos.BadDisks, func(i, j int) bool {
				return infos.BadDisks[i].Address < infos.BadDisks[j].Address
			})
			for _, disk := range infos.BadDisks {
				stdout("%v\n", formatBadDiskInfoRow(disk))
			}
		},
	}
	return cmd
}
