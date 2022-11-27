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
	"os"

	"github.com/desertbit/grumble"

	"github.com/cubefs/cubefs/blobstore/api/access"
	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/common/cfmt"
	"github.com/cubefs/cubefs/blobstore/cli/common/flags"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
	"github.com/cubefs/cubefs/blobstore/cli/config"
)

func putFile(c *grumble.Context) error {
	client, err := newAccessClient()
	if err != nil {
		return err
	}

	var (
		reader *common.PReader
		size   int64
	)
	raw := c.Flags.String("data")
	if len(raw) > 0 {
		data := []byte(raw)
		size = int64(len(data))
		reader = common.NewPReader(int(size), bytes.NewReader(data))

	} else {
		filepath := c.Flags.String("filepath")
		if filepath == "" {
			return fmt.Errorf("no filepath setting")
		}

		file, err := os.Open(c.Flags.String("filepath"))
		if err != nil {
			return fmt.Errorf("open file %s : %s", filepath, err.Error())
		}
		defer file.Close()

		size = c.Flags.Int64("size")
		if size == 0 {
			st, _ := os.Stat(filepath)
			size = st.Size()
		}
		reader = common.NewPReader(int(size), file)
	}
	defer reader.Close()

	reader.LineBar(50)

	putHashes := access.HashAlgorithm(c.Flags.Uint("hashes"))
	fmt.Printf("to upload size:%d hash:b(%b)\n", size, putHashes)

	location, hashes, err := client.Put(common.CmdContext(), &access.PutArgs{
		Size:   size,
		Hashes: putHashes,
		Body:   reader,
	})
	if err != nil {
		return err
	}

	fmt.Println(cfmt.HashSumMapJoin(hashes, "\t"))
	fmt.Printf("put location json    : %s\n", common.RawString(location))
	if flags.Verbose(c.Flags) || config.Verbose() {
		fmt.Println("put location readable:")
		fmt.Println(cfmt.LocationJoin(&location, "\t"))
	} else {
		fmt.Printf("put location verbose : %+v\n", location)
	}

	locPath := c.Flags.String("locationpath")
	if locPath != "" {
		f, err := os.OpenFile(locPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			return fmt.Errorf("open file %s : %s", locPath, err.Error())
		}
		defer f.Close()
		return common.NewEncoder(f).Encode(location)
	}

	return nil
}
