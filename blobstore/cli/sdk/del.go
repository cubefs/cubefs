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
	"github.com/desertbit/grumble"

	acapi "github.com/cubefs/cubefs/blobstore/api/access"
	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
)

func addCmdDelBlob(cmd *grumble.Command) {
	command := &grumble.Command{
		Name: "del",
		Help: "del blob",
		Run:  delBlob,
		Flags: func(f *grumble.Flags) {
			f.String("a", "args", "", "request args string by [json]")
		},
	}
	cmd.AddCommand(command)
}

func delBlob(c *grumble.Context) error {
	client, err := getSdkClient()
	if err != nil {
		return err
	}

	// sdk del --args={\"ClusterID\":10000,\"BlobName\":\"YmxvYjE=\",\"ShardKeys\":[\"YmxvYjE=\",\"MQ==\"]}
	args, err := common.UnmarshalAny[acapi.DelBlobArgs]([]byte(c.Flags.String("args")))
	if err != nil {
		return fmt.Errorf("invalid (%s) %+v", c.Flags.String("args"), err)
	}
	fmt.Printf("del blob name=%s, keys=%s, args json=%s\n", args.BlobName, args.ShardKeys, common.RawString(args))

	if !common.Confirm("to delete?") {
		return nil
	}

	err = client.DeleteBlob(common.CmdContext(), &args)
	if err != nil {
		return err
	}

	fmt.Println("----DELETE OK----")
	return nil
}
