package cmd

import (
	"fmt"
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
		newListDisksCmd(client),
		newDiskDetailCmd(client),
		newListBadDiskCmd(client),
		newDecommissionDiskCmd(client),
		newRecommissionDiskCmd(client),
		newQueryDecommissionDiskCmd(client),
	)
	return cmd
}

const (
	cmdDiskDetailShort = "show disk detail"
)

func newDiskDetailCmd(client *master.MasterClient) *cobra.Command {
	var optDpDetail bool
	cmd := &cobra.Command{
		Use:   CliOpInfo + " [DATANODE_IP:PORT] [DISK_PATH]",
		Short: cmdDiskDetailShort,
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				detail *proto.DiskInfo
				err    error
			)
			defer func() {
				errout(err)
			}()
			if detail, err = client.AdminAPI().DiskDetail(args[0], args[1]); err != nil {
				return
			}
			stdout("Summary:\n%s\n", formatDiskDetailSummary(detail))

			// print data partition detail
			if optDpDetail {
				var view *proto.DiskDataPartitionsView
				if view, err = client.ClientAPI().GetDiskDataPartitions(args[0], args[1]); err != nil {
					err = fmt.Errorf("Get disk data detail information failed:\n%v\n", err)
					return
				}
				stdout("Data partitions:\n")
				stdout("%v\n", diskDataPartitionTableHeader)
				sort.SliceStable(view.DataPartitions, func(i, j int) bool {
					return view.DataPartitions[i].PartitionID < view.DataPartitions[j].PartitionID
				})
				for _, dp := range view.DataPartitions {
					stdout("%v\n", formatDiskDataPartitionTableRow(dp))
				}
			}
		},
	}
	cmd.Flags().BoolVarP(&optDpDetail, "data-partition", "d", false, "Display data partitions on the disk")
	return cmd
}

const (
	cmdListDisksShort = "list disks"
)

func newListDisksCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   CliOpList + " [DATANODE_IP:PORT]",
		Short: cmdListDisksShort,
		Args:  cobra.MaximumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				infos *proto.DiskInfos
				err   error
			)
			defer func() {
				errout(err)
			}()
			addr := ""
			if len(args) > 0 {
				addr = args[0]
			}
			if infos, err = client.AdminAPI().QueryDisks(addr); err != nil {
				return
			}
			sort.SliceStable(infos.Disks, func(i, j int) bool {
				return infos.Disks[i].Address < infos.Disks[j].Address
			})
			stdout("%v\n", formatDiskList(infos.Disks))
		},
	}
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
				infos *proto.DiskInfos
				err   error
			)
			defer func() {
				errout(err)
			}()
			if infos, err = client.AdminAPI().QueryBadDisks(); err != nil {
				return
			}
			stdout("(partitionID=0 means detected by datanode disk checking, not associated with any partition)\n\n[Unavaliable disks]:\n")
			sort.SliceStable(infos.Disks, func(i, j int) bool {
				return infos.Disks[i].Address < infos.Disks[j].Address
			})
			stdoutln(formatBadDisks(infos.Disks))
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
