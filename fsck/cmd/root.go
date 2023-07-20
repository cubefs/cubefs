// Copyright 2020 The CubeFS Authors.
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
	"github.com/spf13/cobra"
)

func NewRootCmd() *cobra.Command {
	var optShowVersion bool
	var c = &cobra.Command{
		Use:   path.Base(os.Args[0]),
		Short: "CubeFS fsck tool",
		Args:  cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			if optShowVersion {
				_, _ = fmt.Fprintf(os.Stdout, proto.DumpVersion("FSCK"))
				return
			}
		},
	}

	proto.InitBufferPool(0)

	c.AddCommand(
		newCheckCmd(),
		newCleanCmd(),
		newInfoCmd(),
	)

	c.PersistentFlags().StringVarP(&MasterAddr, "master", "m", "", "master addresses")
	c.PersistentFlags().StringVarP(&VolName, "vol", "V", "", "volume name")
	c.PersistentFlags().StringVarP(&InodesFile, "inode-list", "i", "", "inode list file")
	c.PersistentFlags().StringVarP(&DensFile, "dentry-list", "d", "", "dentry list file")
	c.PersistentFlags().StringVarP(&MetaPort, "mport", "", "", "prof port of metanode")
	c.PersistentFlags().Uint64VarP(&InodeID, "inode", "", 0, "inode id of a file")
	c.Flags().BoolVarP(&optShowVersion, "version", "v", false, "Show version information")
	return c
}
