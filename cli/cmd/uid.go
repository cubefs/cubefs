package cmd

import (
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util"
	"github.com/spf13/cobra"
)

const (
	cmdUidUse        = "uid [COMMAND]"
	cmdUidShort      = "Manage cluster volumes uid black list"
	cmdUidAddShort   = "add volume uid size"
	cmdUidDelShort   = "del volume uid"
	cmdUidListShort  = "list volume uid info list"
	cmdUidCheckShort = "check volume uid"

	//uid op
	CliUidAdd       = "add"
	cliUidListShort = "list"
	CliUidDel       = "del"
	CliUidCheck     = "check"

	//param
	uidAll = "all"
)

func newUidCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:     cmdUidUse,
		Short:   cmdUidShort,
		Args:    cobra.MinimumNArgs(0),
		Aliases: []string{"uid"},
	}
	cmd.AddCommand(
		newUidAddCmd(client),
		newUidDelCmd(client),
		newUidListCmd(client),
		newUidCheckCmd(client),
	)
	return cmd
}

func newUidAddCmd(client *master.MasterClient) *cobra.Command {
	var optKeyword string
	var cmd = &cobra.Command{
		Use:     CliUidAdd,
		Short:   cmdUidAddShort,
		Aliases: []string{"add"},
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) < 3 {
				stdout("example:cfs-cli uid add volName uid size\n")
				return
			}
			var err error
			defer func() {
				if err != nil {
					errout("Error: %v\n", err)
				}
			}()
			var uidInfo *proto.UidSpaceRsp
			if uidInfo, err = client.UserAPI().UidOperation(args[0], args[1], util.UidAddLimit, args[2]); err != nil || !uidInfo.OK {
				return
			}
			stdout("success!\n")
		},
	}
	cmd.Flags().StringVar(&optKeyword, "keyword", "", "Specify keyword of volume name to filter")
	return cmd
}

func newUidListCmd(client *master.MasterClient) *cobra.Command {
	var (
		optKeyword string
		uidListAll bool
	)
	var cmd = &cobra.Command{
		Use:     cliUidListShort,
		Short:   cmdUidListShort,
		Aliases: []string{"list"},
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) == 0 {
				stdout("need volume name\n")
				return
			}
			if len(args) == 2 {
				if args[1] == uidAll {
					uidListAll = true
				}
			}
			var volumeName = args[0]
			var err error
			defer func() {
				if err != nil {
					errout("Error: %v\n", err)
				}
			}()
			var uidInfo *proto.UidSpaceRsp
			if uidInfo, err = client.UserAPI().UidOperation(volumeName, "", util.UidLimitList, ""); err != nil || !uidInfo.OK {
				stdout("UidOperation return \n")
				return
			}
			stdout("%v\n", volumeUidTableHeader)
			for _, info := range uidInfo.UidSpaceArr {
				if !uidListAll && info.Enabled == false {
					continue
				}
				stdout("%v\n", formatUidInfoTableRow(info))
			}
		},
	}
	cmd.Flags().StringVar(&optKeyword, "keyword", "", "Specify keyword of volume name to filter")
	return cmd
}

func newUidDelCmd(client *master.MasterClient) *cobra.Command {
	var optKeyword string
	var cmd = &cobra.Command{
		Use:     CliUidDel,
		Short:   cmdUidDelShort,
		Aliases: []string{"del"},
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) <= 1 {
				stdout("USAGE:./cfs-cli uid uidDel volName\n")
				return
			}

			var err error
			defer func() {
				if err != nil {
					errout("Error: %v\n", err)
				}
			}()
			var uidInfo *proto.UidSpaceRsp
			if uidInfo, err = client.UserAPI().UidOperation(args[0], args[1], util.UidDel, ""); err != nil || !uidInfo.OK {
				return
			}
			stdout("success!\n")
		},
	}
	cmd.Flags().StringVar(&optKeyword, "keyword", "", "Specify keyword of volume name to filter")
	return cmd
}

func newUidCheckCmd(client *master.MasterClient) *cobra.Command {
	var optKeyword string
	var cmd = &cobra.Command{
		Use:     CliUidCheck,
		Short:   cmdUidCheckShort,
		Aliases: []string{"check"},
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) <= 1 {
				stdout("USAGE:./cfs-cli uid uidCheck volName\n")
				return
			}

			var err error
			defer func() {
				if err != nil {
					errout("Error: %v\n", err)
				}
			}()
			var uidInfo *proto.UidSpaceRsp
			if uidInfo, err = client.UserAPI().UidOperation(args[0], args[1], util.UidGetLimit, ""); err != nil || !uidInfo.OK {
				return
			}
			stdout("%v\n", volumeUidTableHeader)
			for _, info := range uidInfo.UidSpaceArr {
				stdout("%v\n", formatUidInfoTableRow(info))
			}
		},
	}
	cmd.Flags().StringVar(&optKeyword, "keyword", "", "Specify keyword of volume name to filter")
	return cmd
}
