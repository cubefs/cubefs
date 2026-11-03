// Copyright 2026 The CubeFS Authors.
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

package clustermgr

import (
	"github.com/desertbit/grumble"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
)

func addCmdMetaStat(cmd *grumble.Command) {
	command := &grumble.Command{
		Name: "meta_stat",
		Help: "show rocksdb stats of clustermgr databases",
		Run:  cmdMetaStat,
		Flags: func(f *grumble.Flags) {
			clusterFlags(f)
			f.StringL("db_name", "", "specific db name, empty means all dbs")
		},
	}
	cmd.AddCommand(command)
}

func cmdMetaStat(c *grumble.Context) error {
	cli := newCMClient(c.Flags)
	args := &clustermgr.MetaStatArgs{DBName: c.Flags.String("db_name")}
	ret, err := cli.MetaStat(common.CmdContext(), args)
	if err != nil {
		return err
	}
	fmt.Println(common.Readable(ret))
	return nil
}
