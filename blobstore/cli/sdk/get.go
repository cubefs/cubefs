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
	"io"
	"os"
	"time"

	"github.com/desertbit/grumble"

	acapi "github.com/cubefs/cubefs/blobstore/api/access"
	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
)

func addCmdGetBlob(cmd *grumble.Command) {
	command := &grumble.Command{
		Name: "get",
		Help: "get blob",
		Run:  getBlob,
		Flags: func(f *grumble.Flags) {
			f.String("a", "args", "", "request args string by [json]")
			f.String("f", "filepath", "", "save data file path")
		},
	}
	cmd.AddCommand(command)
}

func getBlob(c *grumble.Context) error {
	client, err := getSdkClient()
	if err != nil {
		return err
	}

	// sdk get --args={\"ClusterID\":10000,\"BlobName\":\"YmxvYjEx\",\"ReadSize\":10,\"Mode\":1}
	args, err := common.UnmarshalAny[acapi.GetBlobArgs]([]byte(c.Flags.String("args")))
	if err != nil {
		return fmt.Errorf("invalid (%s) %+v", c.Flags.String("args"), err)
	}
	fmt.Printf("get blob name=%s, keys=%s, args json=%s\n", args.BlobName, args.ShardKeys, common.RawString(args))

	rc, err := client.GetBlob(common.CmdContext(), &args)
	if err != nil {
		return err
	}
	defer rc.Close()

	return readToDst(c.Flags.String("filepath"), args.ReadSize, rc)
}

func readToDst(filePath string, size uint64, rc io.ReadCloser) error {
	reader := common.NewPReader(int(size), rc)
	defer reader.Close()

	reader.LineBar(50)

	if filePath != "" {
		file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			return fmt.Errorf("open file %s : %+v", filePath, err)
		}
		defer file.Close()

		_, err = io.CopyN(file, reader, int64(size))
		if err != nil {
			return fmt.Errorf("downloading to %s : %+v", filePath, err)
		}

		fmt.Println("----get blob done----")
		return nil
	}

	// show to screen
	var w io.Writer
	buffer := bytes.NewBuffer(nil)
	// > 4K
	if size > 1<<12 {
		fmt.Printf("data is too long %d > %d\n", size, 1<<12)
		w = io.Discard
	} else {
		w = buffer
	}

	_, err := io.CopyN(w, reader, int64(size))
	if err != nil {
		return fmt.Errorf("downloading %+v", err)
	}

	data := buffer.Bytes()
	if len(data) > 0 {
		time.Sleep(10 * time.Millisecond)
		fmt.Printf("----get blob ok---- raw data %d: '%s'\n", len(data), string(data))
	}
	return nil
}
