package cmd

import (
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util"
	"github.com/spf13/cobra"
)

const (
	cmdAclUse   = "acl [COMMAND]"
	cmdAclShort = "Manage cluster volumes acl black list"
	cmdAclAddShort = "add volume acl ip"
	cmdAclDelShort = "del volume acl ip"
	cmdAclListShort = "list volume acl ip list"
	cmdAclCheckShort = "check volume acl ip"

	//acl op
	CliAclAdd = "aclAdd"
	cliAclListShort   = "aclList"
	CliAclDel    = "aclDel"
	CliAclCheck    = "aclCheck"
)

func newAclCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:     cmdAclUse,
		Short:   cmdAclShort,
		Args:    cobra.MinimumNArgs(0),
		Aliases: []string{"acl"},
	}
	cmd.AddCommand(
		newAclAddCmd(client),
		newAclDelCmd(client),
		newAclListCmd(client),
		newAclCheckCmd(client),
	)
	return cmd
}

func newAclAddCmd(client *master.MasterClient) *cobra.Command {
	var optKeyword string
	var cmd = &cobra.Command{
		Use:     CliAclAdd,
		Short:   cmdAclAddShort,
		Aliases: []string{"add"},
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) <= 1 {
				stdout("example:cfs-cli acl aclAdd volName 192.168.0.1\n")
				return
			}
			var err error
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			var aclInfo *proto.AclRsp
			if aclInfo, err = client.UserAPI().AclOperation(args[0],  args[1], util.AclAddIP); err != nil || !aclInfo.OK {
				return
			}
			stdout("success!\n")
		},
	}
	cmd.Flags().StringVar(&optKeyword, "keyword", "", "Specify keyword of volume name to filter")
	return cmd
}

func newAclListCmd(client *master.MasterClient) *cobra.Command {
	var optKeyword string
	var cmd = &cobra.Command{
		Use:     cliAclListShort,
		Short:   cmdAclListShort,
		Aliases: []string{"list"},
		Run: func(cmd *cobra.Command, args []string) {
			var volumeName = args[0]
			var err error
			if len(args) == 0 {
				stdout("need volume name\n")
				return
			}
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			var aclInfo *proto.AclRsp
			if aclInfo, err = client.UserAPI().AclOperation(volumeName, "", util.AclListIP); err != nil || !aclInfo.OK {
				stdout("AclOperation return \n")
				return
			}
			stdout("%v\n", volumeAclTableHeader)
			for _, info := range aclInfo.List {
				stdout("%v\n", formatAclInfoTableRow(info))
			}
		},
	}
	cmd.Flags().StringVar(&optKeyword, "keyword", "", "Specify keyword of volume name to filter")
	return cmd
}

func newAclDelCmd(client *master.MasterClient) *cobra.Command {
	var optKeyword string
	var cmd = &cobra.Command{
		Use:     CliAclDel,
		Short:   cmdAclDelShort,
		Aliases: []string{"del"},
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) <= 1 {
				stdout("USAGE:./cfs-cli acl aclDel volName ipAddr\n")
				return
			}

			var err error
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			var aclInfo *proto.AclRsp
			if aclInfo, err = client.UserAPI().AclOperation(args[0], args[1], util.AclDelIP); err != nil || !aclInfo.OK {
				return
			}
			stdout("success!\n")
		},
	}
	cmd.Flags().StringVar(&optKeyword, "keyword", "", "Specify keyword of volume name to filter")
	return cmd
}

func newAclCheckCmd(client *master.MasterClient) *cobra.Command {
	var optKeyword string
	var cmd = &cobra.Command{
		Use:     CliAclCheck,
		Short:   cmdAclCheckShort,
		Aliases: []string{"check"},
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) <= 1 {
				stdout("USAGE:./cfs-cli acl aclCheck volName ipAddr\n")
				return
			}

			var err error
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			var aclInfo *proto.AclRsp
			if aclInfo, err = client.UserAPI().AclOperation(args[0], args[1], util.AclCheckIP); err !=nil || !aclInfo.OK {
				return
			}
			stdout("%v\n", volumeAclTableHeader)
			for _, info := range aclInfo.List {
				stdout("%v\n", formatAclInfoTableRow(info))
			}
		},
	}
	cmd.Flags().StringVar(&optKeyword, "keyword", "", "Specify keyword of volume name to filter")
	return cmd
}