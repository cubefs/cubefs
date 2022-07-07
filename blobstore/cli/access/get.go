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
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"time"

	"github.com/desertbit/grumble"

	"github.com/cubefs/cubefs/blobstore/api/access"
	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/common/cfmt"
	"github.com/cubefs/cubefs/blobstore/cli/common/flags"
	"github.com/cubefs/cubefs/blobstore/cli/config"
)

func getFile(c *grumble.Context) error {
	client, err := newAccessClient()
	if err != nil {
		return err
	}

	location, err := readLocation(c.Flags)
	if err != nil {
		return err
	}

	fmt.Printf("get location json    : %s\n", common.RawString(location))
	if flags.Verbose(c.Flags) || config.Verbose() {
		fmt.Println("get location readable:")
		fmt.Println(cfmt.LocationJoin(&location, "\t"))
	} else {
		fmt.Printf("get location verbose : %+v\n", location)
	}

	size := c.Flags.Uint64("readsize")
	if size == 0 {
		size = location.Size - c.Flags.Uint64("offset")
	}

	r, err := client.Get(common.CmdContext(), &access.GetArgs{
		Location: location,
		Offset:   c.Flags.Uint64("offset"),
		ReadSize: size,
	})
	if err != nil {
		return err
	}
	defer r.Close()

	reader := common.NewPReader(int(size), r)
	defer reader.Close()

	reader.LineBar(50)

	filepath := c.Flags.String("filepath")
	if filepath == "" {
		var w io.Writer
		buffer := bytes.NewBuffer(nil)
		// > 4K
		if size > 1<<12 {
			fmt.Printf("data is too long %d > %d\n", size, 1<<12)
			w = ioutil.Discard
		} else {
			w = buffer
		}

		_, err = io.CopyN(w, reader, int64(size))
		if err != nil {
			return fmt.Errorf("downloading %s", err.Error())
		}

		data := buffer.Bytes()
		if len(data) > 0 {
			time.Sleep(10 * time.Millisecond)
			fmt.Printf("raw data %d: '%s'\n", len(data), string(data))
		}
		return nil
	}

	file, err := os.OpenFile(filepath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("open file %s : %s", filepath, err.Error())
	}
	defer file.Close()

	_, err = io.CopyN(file, reader, int64(size))
	if err != nil {
		return fmt.Errorf("downloading to %s : %s", filepath, err.Error())
	}

	return nil
}
