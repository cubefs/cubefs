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
		newCmdFlashTopoSetVolReadLimit(client),
		newCmdFlashTopoSetVolWriteLimit(client),
		newCmdFlashTopoCancelDel(client),
		newCmdFlashTopoSetDelayDeleteTime(client),
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

func newCmdFlashTopoCancelDel(client *master.MasterClient) *cobra.Command {
	var name string
	cmd := &cobra.Command{
		Use:   "cancelDel",
		Short: "cancel delayed deletion for a flash topology in markDeleted status",
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			if strings.TrimSpace(name) == "" {
				return fmt.Errorf("name should not be empty")
			}
			result, err := client.AdminAPI().CancelDeleteFlashTopo(name)
			if err != nil {
				return
			}
			stdoutln(result)
			return
		},
	}
	cmd.Flags().StringVarP(&name, "name", "n", "", "flash topology name")
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

func newCmdFlashTopoSetVolReadLimit(client *master.MasterClient) *cobra.Command {
	var topoName string
	var volName string
	var readFlow int64
	cmd := &cobra.Command{
		Use:   "setVolReadLimit",
		Short: "set volume read limit for a flash topology",
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			if strings.TrimSpace(topoName) == "" {
				topoName = "default"
			}
			if strings.TrimSpace(volName) == "" {
				return fmt.Errorf("vol should not be empty")
			}
			if readFlow < 0 {
				return fmt.Errorf("freadFlow must be >= 0")
			}
			result, err := client.AdminAPI().SetFlashTopoVolReadLimit(topoName, volName, readFlow)
			if err != nil {
				return err
			}
			stdoutln(result)
			return nil
		},
	}
	cmd.Flags().StringVarP(&topoName, "name", "n", "default", "flash topology name")
	cmd.Flags().StringVar(&volName, "vol", "", "volume name")
	cmd.Flags().Int64Var(&readFlow, "freadFlow", 0, "volume read flow limit in bytes")
	return cmd
}

func newCmdFlashTopoSetVolWriteLimit(client *master.MasterClient) *cobra.Command {
	var topoName string
	var volName string
	var writeFlow int64
	cmd := &cobra.Command{
		Use:   "setVolWriteLimit",
		Short: "set volume write limit for a flash topology",
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			if strings.TrimSpace(topoName) == "" {
				topoName = "default"
			}
			if strings.TrimSpace(volName) == "" {
				return fmt.Errorf("vol should not be empty")
			}
			if writeFlow < 0 {
				return fmt.Errorf("fwriteFlow must be >= 0")
			}
			result, err := client.AdminAPI().SetFlashTopoVolWriteLimit(topoName, volName, writeFlow)
			if err != nil {
				return err
			}
			stdoutln(result)
			return nil
		},
	}
	cmd.Flags().StringVarP(&topoName, "name", "n", "default", "flash topology name")
	cmd.Flags().StringVar(&volName, "vol", "", "volume name")
	cmd.Flags().Int64Var(&writeFlow, "fwriteFlow", 0, "volume write flow limit in bytes")
	return cmd
}

func newCmdFlashTopoDel(client *master.MasterClient) *cobra.Command {
	var name string
	var optYes bool
	var optGradualFlag bool
	var optStep uint32
	var optForceDel bool
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
			result, err := client.AdminAPI().DelFlashTopo(name, optGradualFlag, optStep, optForceDel)
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
	cmd.Flags().BoolVar(&optForceDel, "forceDel", false, "force delete the topology immediately (default false)")
	return cmd
}

func newCmdFlashTopoSetDelayDeleteTime(client *master.MasterClient) *cobra.Command {
	var hours int
	cmd := &cobra.Command{
		Use:   "setDelayDeleteTime",
		Short: "set flash topology delayed deletion time (hours)",
		Args:  cobra.MinimumNArgs(0),
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			if hours <= 0 {
				return fmt.Errorf("hours must be greater than 0")
			}
			if err = client.AdminAPI().SetMasterFlashTopoDeletionDelayTime(hours); err != nil {
				return
			}
			stdoutlnf("set flashTopoDeletionDelayTime to %d h successfully", hours)
			return
		},
	}
	cmd.Flags().IntVar(&hours, "hours", 48, "delayed deletion time in hours")
	return cmd
}
