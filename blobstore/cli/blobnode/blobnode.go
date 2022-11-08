// Copyright 2022 The CubeFS Authors.
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

package blobnode

import (
	"github.com/cubefs/cubefs/blobstore/cli/common/flags"

	"github.com/desertbit/grumble"
)

// Register register blobnode.
func Register(app *grumble.App) {
	blobnodeCommand := &grumble.Command{
		Name: "blobnode",
		Help: "blobnode tools",
	}
	app.AddCommand(blobnodeCommand)

	addCmdDisk(blobnodeCommand)
	addCmdChunk(blobnodeCommand)
	addCmdShard(blobnodeCommand)
	addCmdIOStat(blobnodeCommand)
}

func blobnodeFlags(f *grumble.Flags) {
	// readable
	flags.VerboseRegister(f)

	// host
	f.StringL("host", "http://127.0.0.1:8889", "host of the blobnode")
}
