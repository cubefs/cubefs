package cmd

import (
	"fmt"
	"os"
	"path"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/log"
	"github.com/spf13/cobra"
)

type CubeFSCmd struct {
	CFSCmd *cobra.Command
}

func NewRootCmd(client *master.MasterClient) *CubeFSCmd {
	var optShowVersion bool
	cmd := &CubeFSCmd{
		CFSCmd: &cobra.Command{
			Use:   path.Base(os.Args[0]),
			Short: "CubeFS Command Line Interface (CLI)",
			Args:  cobra.MinimumNArgs(0),
			Run: func(cmd *cobra.Command, args []string) {
				if optShowVersion {
					stdout("%v", proto.DumpVersion("CLI"))
					return
				}
				if len(args) == 0 {
					cmd.Help()
					return
				}
				suggestionsString := ""
				if suggestions := cmd.SuggestionsFor(args[0]); len(suggestions) > 0 {
					suggestionsString += "\nDid you mean this?\n"
					for _, s := range suggestions {
						suggestionsString += fmt.Sprintf("\t%v\n", s)
					}
				}
				errout(fmt.Errorf("cfs-cli: unknown command %q\n%s", args[0], suggestionsString))
			},
			SilenceErrors: true,
			SilenceUsage:  true,
		},
	}
	cmd.CFSCmd.Flags().BoolVarP(&optShowVersion, "version", "v", false, "Show version information")

	cmd.CFSCmd.AddCommand(
		newConfigCmd(),
		newClusterCmd(client),
		newFlashNodeCmd(client),
		newFlashGroupCmd(client),
	)
	return cmd
}

var stdout = stdoutf

func stdoutf(format string, a ...interface{}) {
	fmt.Fprintf(os.Stdout, format, a...)
}

func errout(err error) {
	if err == nil {
		return
	}
	fmt.Fprintln(os.Stderr, "Error:", err)
	log.LogError("Error:", err)
	log.LogFlush()
	os.Exit(1)
}

func stdoutln(a ...interface{}) {
	fmt.Fprintln(os.Stdout, a...)
}

func stdoutlnf(format string, a ...interface{}) {
	stdoutf(format, a...)
	fmt.Fprintln(os.Stdout)
}
