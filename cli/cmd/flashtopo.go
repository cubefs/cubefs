package cmd

import (
	"fmt"
	"strings"

	"github.com/cubefs/cubefs/sdk/master"
	"github.com/spf13/cobra"
)

func newFlashTopoCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "flashtopo [COMMAND]",
		Short: "flash topology management",
	}
	cmd.AddCommand(
		newCmdFlashTopoList(client),
		newCmdFlashTopoAdd(client),
		newCmdFlashTopoDel(client),
		newCmdFlashTopoRename(client),
	)
	return cmd
}

func newCmdFlashTopoList(client *master.MasterClient) *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "list all flash topologies",
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			ftvs, err := client.AdminAPI().ListAllFlashTopos()
			if err != nil {
				return
			}
			stdoutln("Flash Topologies:")
			stdoutln(formatIndent(ftvs))
			return
		},
	}
}

func newCmdFlashTopoAdd(client *master.MasterClient) *cobra.Command {
	var name string
	var region string
	cmd := &cobra.Command{
		Use:   "add",
		Short: "add a flash topology",
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			if name == "" {
				// keep consistent with server default if empty, but still allow explicit default
				name = "default"
			}
			if region == "" {
				region = "default"
			}
			result, err := client.AdminAPI().AddFlashTopo(name, region)
			if err != nil {
				return
			}
			stdoutln(result)
			return
		},
	}
	cmd.Flags().StringVarP(&name, "name", "n", "default", "flash topology name")
	cmd.Flags().StringVar(&region, "region", "default", "flash topology region")
	return cmd
}

func newCmdFlashTopoRename(client *master.MasterClient) *cobra.Command {
	return &cobra.Command{
		Use:   "rename [srcName] [dstName]",
		Short: "rename a flash topology",
		Args:  cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			srcName := strings.TrimSpace(args[0])
			dstName := strings.TrimSpace(args[1])
			if srcName == "" {
				err = fmt.Errorf("srcName should not be empty")
				return
			}
			if dstName == "" {
				err = fmt.Errorf("dstName should not be empty")
				return
			}
			result, err := client.AdminAPI().RenameFlashTopo(srcName, dstName)
			if err != nil {
				return
			}
			stdoutln(result)
			return
		},
	}
}

func newCmdFlashTopoDel(client *master.MasterClient) *cobra.Command {
	var name string
	var optYes bool
	var optGradualFlag bool
	var optStep uint32
	cmd := &cobra.Command{
		Use:   "del",
		Short: "delete a flash topology",
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			if name == "" {
				name = "default"
			}
			// ask user for confirm
			if !optYes {
				stdout("delete flash topology: %s\n", name)
				stdout("\nConfirm (yes/no)[no]: ")
				var userConfirm string
				_, _ = fmt.Scanln(&userConfirm)
				if userConfirm != "yes" {
					err = fmt.Errorf("Abort by user.\n")
					return
				}
			}
			if optGradualFlag {
				if optStep <= 0 {
					err = fmt.Errorf("param step(%v) must greater than 0", optStep)
					return
				}
			}
			result, err := client.AdminAPI().DelFlashTopo(name, optGradualFlag, optStep)
			if err != nil {
				return
			}
			stdoutln(result)
			return
		},
	}
	cmd.Flags().StringVarP(&name, "name", "n", "default", "flash topology name")
	cmd.Flags().BoolVarP(&optYes, "yes", "y", false, "Answer yes for all questions")
	cmd.Flags().BoolVar(&optGradualFlag, "gradualFlag", false, "set whether the topology's slots are deleted gradually or not(default false)")
	cmd.Flags().Uint32Var(&optStep, "step", 1, "set the step size(default 1) for slot gradual deletion")
	return cmd
}
