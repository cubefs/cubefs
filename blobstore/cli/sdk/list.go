// Copyright 2024 The CubeFS Authors.
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

package sdk

import (
	"os"

	"github.com/desertbit/grumble"

	acapi "github.com/cubefs/cubefs/blobstore/api/access"
	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
)

func addCmdListBlob(cmd *grumble.Command) {
	command := &grumble.Command{
		Name: "list",
		Help: "list blob",
		Run:  listBlob,
		Flags: func(f *grumble.Flags) {
			f.String("a", "args", "", "request args string by [json]")
			f.String("f", "filepath", "", "save blob file path")
		},
	}
	cmd.AddCommand(command)
}

func listBlob(c *grumble.Context) error {
	client, err := getSdkClient()
	if err != nil {
		return err
	}

	// sdk list --args={\"ClusterID\":10000,\"Count\":20}
	// sdk list --args={\"ClusterID\":10000,\"ShardID\":4,\"Marker\":\"YmxvYjg=\"}
	args, err := common.UnmarshalAny[acapi.ListBlobArgs]([]byte(c.Flags.String("args")))
	if err != nil {
		return fmt.Errorf("invalid (%s) %+v", c.Flags.String("args"), err)
	}
	fmt.Printf("list blob args json=%s\n", common.RawString(args))

	// list blob
	ret, err := client.ListBlob(common.CmdContext(), &args)
	if err != nil {
		return err
	}
	fmt.Println("----list blob ok----")

	// show result
	locPath := c.Flags.String("filepath")
	if locPath == "" {
		fmt.Printf("list blob json\t: %s\n", common.RawString(ret))
		return nil
	}

	f, err := os.OpenFile(locPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("open file %s : %+v", locPath, err)
	}
	defer f.Close()
	return common.NewEncoder(f).Encode(ret)
}
