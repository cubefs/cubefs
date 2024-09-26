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
	"bytes"
	"os"

	"github.com/desertbit/grumble"

	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
	"github.com/cubefs/cubefs/blobstore/sdk/base"
)

func addCmdPutBlob(cmd *grumble.Command) {
	command := &grumble.Command{
		Name: "put",
		Help: "put blob",
		Run:  putBlob,
		Flags: func(f *grumble.Flags) {
			f.String("a", "args", "", "request args string by [json]")
			f.String("f", "filepath", "", "src_data: put file path")
			f.String("d", "data", "", "src_data: raw data body")
			f.String("p", "location_path", "", "save location file path")
		},
	}
	cmd.AddCommand(command)
}

func putBlob(c *grumble.Context) error {
	client, err := getSdkClient()
	if err != nil {
		return err
	}

	args, err := common.UnmarshalAny[base.PutBlobArgs]([]byte(c.Flags.String("args")))
	if err != nil {
		return fmt.Errorf("invalid (%s) %+v", c.Flags.String("args"), err)
	}
	fmt.Printf("put blob args json    : %s\n", common.RawString(args))

	reader, err := getReader(c, &args)
	if err != nil {
		return err
	}

	defer reader.Close()
	reader.LineBar(50)
	args.Body = reader

	location, err := client.PutBlob(common.CmdContext(), &args)
	if err != nil {
		return err
	}

	locPath := c.Flags.String("location_path")
	if locPath == "" {
		fmt.Printf("put location json    : %s\n", common.RawString(location))
		return nil
	}

	f, err := os.OpenFile(locPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("open file %s : %+v", locPath, err)
	}
	defer f.Close()
	return common.NewEncoder(f).Encode(location)
}

func getReader(c *grumble.Context, args *base.PutBlobArgs) (*common.PReader, error) {
	var reader *common.PReader
	size := uint64(0)

	raw := c.Flags.String("data")
	if len(raw) > 0 {
		data := []byte(raw)
		size = uint64(len(data))
		reader = common.NewPReader(int(size), bytes.NewReader(data))

	} else {
		filepath := c.Flags.String("filepath")
		if filepath == "" {
			return nil, fmt.Errorf("no filepath setting")
		}

		file, err := os.Open(c.Flags.String("filepath"))
		if err != nil {
			return nil, fmt.Errorf("open file %s : %+v", filepath, err)
		}
		defer file.Close()

		size = c.Flags.Uint64("size")
		if size == 0 {
			st, _ := os.Stat(filepath)
			size = uint64(st.Size())
		}
		reader = common.NewPReader(int(size), file)
	}

	if size != args.Size {
		fmt.Printf("args.size=%d, we use read size=%d \n", args.Size, size)
		args.Size = size
	}
	return reader, nil
}
