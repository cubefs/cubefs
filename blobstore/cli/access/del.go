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

package access

import (
	"github.com/desertbit/grumble"

	"github.com/cubefs/cubefs/blobstore/api/access"
	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/common/cfmt"
	"github.com/cubefs/cubefs/blobstore/cli/common/flags"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
	"github.com/cubefs/cubefs/blobstore/cli/config"
)

func delFile(c *grumble.Context) error {
	client, err := newAccessClient()
	if err != nil {
		return err
	}

	location, err := readLocation(c.Flags)
	if err != nil {
		return err
	}

	fmt.Printf("del location json    : %s\n", common.RawString(location))
	if flags.Verbose(c.Flags) || config.Verbose() {
		fmt.Println("del location readable:")
		fmt.Println(cfmt.LocationJoin(&location, "\t"))
	} else {
		fmt.Printf("del location verbose : %+v\n", location)
	}

	if !common.Confirm("to delete?") {
		return nil
	}
	deleteArgs := &access.DeleteArgs{
		Locations: []access.Location{location},
	}
	_, err = client.Delete(common.CmdContext(), deleteArgs)
	if err != nil {
		return err
	}

	fmt.Println("DELETE OK!")
	return nil
}
