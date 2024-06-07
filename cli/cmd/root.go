// Copyright 2018 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

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

	// TODO: delete compatibility cmd at 49e62e794d7c1000c9fb09bd75565112ecd5c5e1.
	// add back into Commands later ?
	_ = newCompatibilityCmd()

	cmd.CFSCmd.AddCommand(
		newClusterCmd(client),
		newVolCmd(client),
		newUserCmd(client),
		newMetaNodeCmd(client),
		newDataNodeCmd(client),
		newDataPartitionCmd(client),
		newMetaPartitionCmd(client),
		newConfigCmd(),
		newZoneCmd(client),
		newNodeSetCmd(client),
		newAclCmd(client),
		newUidCmd(client),
		newQuotaCmd(client),
		newDiskCmd(client),
		newVersionCmd(client),
		newFlashNodeCmd(client),
		newFlashGroupCmd(client),
	)
	return cmd
}

var stdout = stdoutf

func stdoutln(a ...interface{}) {
	fmt.Fprintln(os.Stdout, a...)
}

func stdoutf(format string, a ...interface{}) {
	fmt.Fprintf(os.Stdout, format, a...)
}

func stdoutlnf(format string, a ...interface{}) {
	stdoutf(format, a...)
	fmt.Fprintln(os.Stdout)
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
