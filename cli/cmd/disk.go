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
		newDecommissionDiskCmd(client),
		newRecommissionDiskCmd(client),
		newQueryDecommissionDiskCmd(client),
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
			stdout("(partitionID=0 means detected by datanode disk checking, not associated with any partition)\n\n[Unavaliable disks]:\n")
			sort.SliceStable(infos.BadDisks, func(i, j int) bool {
				return infos.BadDisks[i].Address < infos.BadDisks[j].Address
			})
			stdoutln(formatBadDisks(infos.BadDisks))
		},
	}
	return cmd
}

const (
	cmdDecommissionDisksShort = "Decommission disk on datanode"
)

func newDecommissionDiskCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   CliOpDecommission + " [DATA NODE ADDR] [DISK]",
		Short: cmdDecommissionDisksShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				errout(err)
			}()
			if err = client.AdminAPI().DecommissionDisk(args[0], args[1]); err != nil {
				return
			}
			stdout("Mark disk %v:%v to be decommissioned", args[0], args[1])
		},
	}
	return cmd
}

const (
	cmdRecommissionDisksShort = "Recommission disk on datanode"
)

func newRecommissionDiskCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   CliOpRecommission + " [DATA NODE ADDR] [DISK]",
		Short: cmdRecommissionDisksShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				errout(err)
			}()
			if err = client.AdminAPI().RecommissionDisk(args[0], args[1]); err != nil {
				return
			}
			stdout("Mark disk %v:%v to be recommissioned", args[0], args[1])
		},
	}
	return cmd
}

const (
	cmdQueryDecommissionDiskProgressShort = "Query decommmission progress on datanode"
)

func newQueryDecommissionDiskCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   CliOpQueryProgress + " [DATA NODE ADDR] [DISK]",
		Short: cmdQueryDecommissionDiskProgressShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				errout(err)
			}()
			progress, err := client.AdminAPI().QueryDecommissionDiskProgress(args[0], args[1])
			if err != nil {
				return
			}
			stdout("%v", formatDecommissionProgress(progress))
		},
	}
	return cmd
}
