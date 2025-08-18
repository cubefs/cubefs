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
	"encoding/json"
	"os"

	"github.com/desertbit/grumble"

	acapi "github.com/cubefs/cubefs/blobstore/api/access"
	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/common/cfmt"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
)

func addCmdPutBlob(cmd *grumble.Command) {
	command := &grumble.Command{
		Name: "put",
		Help: "put blob",
		Run:  putBlob,
		Flags: func(f *grumble.Flags) {
			f.String("a", "args", "", "raw request args string by [json]")
			f.String("", "wrap_args", "", "request args with readable string keys")
			f.String("d", "data", "", "src_data: raw data body")
			f.String("f", "filepath", "", "src_data: put file path")
			f.Uint64("s", "filesize", 0, "src_size: want file size")
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

	// sdk put --args={\"CodeMode\":11,\"ShardKeys\":[\"YmxvYi0z=\",\"MQ==\"],\"NeedSeal\":true,\"Size\":10} --data="test-data3"
	// sdk put --wrap_args={\"blob_name_str\":\"blob11\",\"CodeMode\":11,\"NeedSeal\":true,\"Size\":10} --data="testData11" -p=location.json
	args, wrapArgs := acapi.PutBlobArgs{}, ReadablePutArg{}
	wrap := c.Flags.String("wrap_args")
	if wrap != "" {
		err = json.Unmarshal([]byte(wrap), &wrapArgs)
		if err != nil {
			return fmt.Errorf("invalid (%s) %+v", wrap, err)
		}
		wrapArgs.BlobName = string(wrapArgs.BlobNameStr)
		args = wrapArgs.PutBlobArgs
	} else {
		args, err = common.UnmarshalAny[acapi.PutBlobArgs]([]byte(c.Flags.String("args")))
		if err != nil {
			return fmt.Errorf("invalid (%s) %+v", c.Flags.String("args"), err)
		}
	}
	fmt.Printf("put blob name=%s, args json=%s\n", args.BlobName, common.RawString(args))

	reader, file, err := getReader(c, &args)
	if err != nil {
		return err
	}

	defer func() {
		file.Close()
		reader.Close()
	}()
	reader.LineBar(50)
	args.Body = reader

	clusterID, hashMap, err := client.PutBlob(common.CmdContext(), &args)
	if err != nil {
		return err
	}
	fmt.Printf("----put blob ok---- cluster:%d, hash:%v\n", clusterID, cfmt.HashSumMapJoin(hashMap, "\t"))

	locPath := c.Flags.String("location_path")
	if locPath == "" {
		return nil
	}

	f, err := os.OpenFile(locPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open file %s : %+v", locPath, err)
	}
	defer f.Close()
	return common.NewEncoder(f).Encode(wrapArgs)
}

func getReader(c *grumble.Context, args *acapi.PutBlobArgs) (reader *common.PReader, file *os.File, err error) {
	size := uint64(0)

	raw := c.Flags.String("data")
	if len(raw) > 0 {
		data := []byte(raw)
		size = uint64(len(data))
		reader = common.NewPReader(int(size), bytes.NewReader(data))

	} else {
		filepath := c.Flags.String("filepath")
		if filepath == "" {
			return nil, nil, fmt.Errorf("no filepath setting")
		}

		file, err = os.Open(c.Flags.String("filepath"))
		if err != nil {
			return nil, nil, fmt.Errorf("open file %s : %+v", filepath, err)
		}
		// defer file.Close()

		size = c.Flags.Uint64("filesize")
		if size == 0 {
			st, _ := os.Stat(filepath)
			size = uint64(st.Size())
		}
		reader = common.NewPReader(int(size), file)
	}

	if size != args.Size {
		fmt.Printf("args.size=%d, we use read size=%d\n", args.Size, size)
		args.Size = size
	}
	return reader, file, nil
}

type ReadablePutArg struct {
	BlobNameStr  string   `json:"blob_name_str"`
	ShardKeysStr []string `json:"shard_keys_str"`
	acapi.PutBlobArgs
}
