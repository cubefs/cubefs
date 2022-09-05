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

package clustermgr

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"

	"github.com/desertbit/grumble"
	pb "go.etcd.io/etcd/raft/v3/raftpb"

	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
)

func addCmdWalParse(cmd *grumble.Command) {
	command := &grumble.Command{
		Name:     "wal",
		Help:     "wal tools",
		LongHelp: "wal tools for clustermgr",
	}
	cmd.AddCommand(command)

	command.AddCommand(&grumble.Command{
		Name: "parse",
		Help: "parse wal file or path",
		Run:  cmdWalParse,
		Args: func(a *grumble.Args) {
			a.String("filepath", "wal log filepath")
			a.String("out", "pase result output", grumble.Default(""))
		},
	})
}

func cmdWalParse(c *grumble.Context) error {
	filepath := c.Args.String("filepath")
	if filepath == "" {
		return errors.New("wal log file path can't be null")
	}

	printf := func(format string, opts ...interface{}) {
		fmt.Printf(format, opts...)
	}

	out := c.Args.String("out")
	if out != "" {
		mode := os.O_CREATE | os.O_APPEND | os.O_RDWR
		f, err := os.OpenFile(out, mode, 0o755)
		if err != nil {
			return err
		}
		printf = log.New(f, "", log.LstdFlags).Printf
	}

	file, err := os.Open(filepath)
	if err != nil {
		return err
	}

	stat, err := file.Stat()
	if err != nil {
		return err
	}
	if !stat.IsDir() {
		return parse(filepath, printf)
	}

	fis, err := ioutil.ReadDir(filepath)
	if err != nil {
		return err
	}
	for i := range fis {
		if fis[i].IsDir() {
			continue
		}
		if err := parse(path.Join(filepath, fis[i].Name()), printf); err != nil {
			return err
		}
	}

	return nil
}

func parse(filename string, printf func(format string, opts ...interface{})) error {
	f, err := os.Open(filename)
	if err != nil {
		printf("open %s error: %v", filename, err)
		return err
	}

	u64Buf := make([]byte, 8)
	var offset int64
	for {
		n, err := io.ReadFull(f, u64Buf)
		if err != nil && err != io.EOF {
			printf("read data len offset=%d error: %v\n", offset, err)
			return err
		}
		offset = offset + int64(n)
		dataLen := binary.BigEndian.Uint64(u64Buf)
		crc := crc32.NewIEEE()
		tr := io.TeeReader(f, crc)
		data := make([]byte, dataLen+4)
		n, err = io.ReadFull(tr, data[0:int(dataLen)])
		if err != nil {
			printf("read data offset=%d error: %v\n", offset, err)
			break
		}
		offset = offset + int64(n)
		n, err = io.ReadFull(f, data[dataLen:])
		if err != nil {
			printf("read data crc offset=%d error: %v\n", offset, err)
			return err
		}
		offset = offset + int64(n)

		// decode crc
		recType := data[0]
		checksum := binary.BigEndian.Uint32(data[dataLen:])
		data = data[1:dataLen]
		// checksum
		if checksum != crc.Sum32() {
			printf("error checksum=%d\n", checksum)
			return errors.New("error checksum")
		}

		if recType != 1 {
			fmt.Println("type: ", recType)
			return errors.New("type illegal")
		}

		var entry pb.Entry
		if err = entry.Unmarshal(data); err != nil {
			printf("unmarshal error: %v\n", err)
			return fmt.Errorf("data unmarshal error: %s\n", err.Error())
		}
		if entry.Type == pb.EntryConfChange {
			var cc pb.ConfChange
			if err := cc.Unmarshal(entry.Data); err != nil {
				return err
			}
			printf("term=%d index=%d ConfChange=%s\n", entry.Term, entry.Index, cc.String())
			return nil
		}
		if len(entry.Data) == 0 {
			printf("term=%d index=%d\n", entry.Term, entry.Index)
		} else {
			data := entry.Data[8:]
			proposeInfo := base.DecodeProposeInfo(data)
			printf("term=%d index=%d module=%s opertype=%d data=%s \n",
				entry.Term, entry.Index, proposeInfo.Module, proposeInfo.OperType, string(proposeInfo.Data))
		}
	}
	return nil
}
