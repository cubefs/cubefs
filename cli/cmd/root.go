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

const (
	cmdRootShort = "CubeFS Command Line Interface (CLI)"
)

type CubeFSCmd struct {
	CFSCmd *cobra.Command
}

func NewRootCmd(client *master.MasterClient) *CubeFSCmd {
	var optShowVersion bool
	var cmd = &CubeFSCmd{
		CFSCmd: &cobra.Command{
			Use:   path.Base(os.Args[0]),
			Short: cmdRootShort,
			Args:  cobra.MinimumNArgs(0),
			Run: func(cmd *cobra.Command, args []string) {
				if optShowVersion {
					stdout(proto.DumpVersion("CLI"))
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
				errout("cfs-cli: unknown command %q\n%s", args[0], suggestionsString)
			},
		},
	}

	cmd.CFSCmd.Flags().BoolVarP(&optShowVersion, "version", "v", false, "Show version information")

	cmd.CFSCmd.AddCommand(
		cmd.newClusterCmd(client),
		newVolCmd(client),
		newUserCmd(client),
		newMetaNodeCmd(client),
		newDataNodeCmd(client),
		newDataPartitionCmd(client),
		newMetaPartitionCmd(client),
		newConfigCmd(),
		newZoneCmd(client),
		newAclCmd(client),
		newUidCmd(client),
		newQuotaCmd(client),
		newDiskCmd(client),
		newInodeCmd(client),
	)
	return cmd
}

func stdout(format string, a ...interface{}) {
	_, _ = fmt.Fprintf(os.Stdout, format, a...)
}

func errout(format string, a ...interface{}) {
	log.LogErrorf(format, a...)
	_, _ = fmt.Fprintf(os.Stderr, format, a...)
	OsExitWithLogFlush()
}

func OsExitWithLogFlush() {
	log.LogFlush()
	os.Exit(1)
}
