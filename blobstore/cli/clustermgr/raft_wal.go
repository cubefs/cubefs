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

	"github.com/desertbit/grumble"
	pb "go.etcd.io/etcd/raft/v3/raftpb"

	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/clustermgr/volumemgr"
)

var logger *log.Logger

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
			a.String("filename", "wal log filename", grumble.Default(""))
			a.String("path", "wal log path", grumble.Default(""))
			a.String("out", "pase result output", grumble.Default(""))
		},
	})
}

func cmdWalParse(c *grumble.Context) error {
	filename := c.Args.String("filename")
	path := c.Args.String("path")
	out := c.Args.String("out")

	if filename == "" && path == "" {
		return errors.New("wal log file path can't be null")
	}
	if out != "" {
		f, err := os.OpenFile(out, os.O_CREATE|os.O_APPEND, 0o755)
		if err != nil {
			return err
		}
		logger = log.New(f, "", log.LstdFlags)
	}

	if filename != "" {
		parse(filename)
		return nil
	}
	fis, err := ioutil.ReadDir(path)
	if err != nil {
		return err
	}
	for i := range fis {
		if fis[i].IsDir() {
			continue
		}
		parse(path + fis[i].Name())
	}
	return nil
}

func parse(filename string) {
	f, err := os.Open(filename)
	if err != nil {
		printf("open %s error: %v", filename, err)
		return
	}

	u64Buf := make([]byte, 8)
	var offset int64
	for {
		n, err := io.ReadFull(f, u64Buf)
		if err != nil {
			if err != io.EOF {
				printf("read data len offset=%d error: %v\n", offset, err)
			}
			break
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
			break
		}
		offset = offset + int64(n)

		// decode crc
		recType := data[0]
		checksum := binary.BigEndian.Uint32(data[dataLen:])
		data = data[1:dataLen]
		// checksum
		if checksum != crc.Sum32() {
			printf("error checksum=%d\n", checksum)
			break
		}

		if recType != 1 {
			fmt.Println("type: ", recType)
			break
		}

		var entry pb.Entry
		if err = entry.Unmarshal(data); err != nil {
			printf("unmarshal error: %v\n", err)
			break
		}
		if entry.Type == pb.EntryConfChange {
			var cc pb.ConfChange
			cc.Unmarshal(entry.Data)
			printf("term=%d index=%d ConfChange=%s\n", entry.Term, entry.Index, cc.String())
		} else {
			if len(entry.Data) == 0 {
				printf("term=%d index=%d\n", entry.Term, entry.Index)
			} else {
				data := entry.Data[8:]
				proposeInfo := base.DecodeProposeInfo(data)
				if proposeInfo.Module == "VolumeMgr" && proposeInfo.OperType == volumemgr.OperTypeChunkReport {
					printf("term=%d index=%d module=%s opertype=%d data=%v \n",
						entry.Term, entry.Index, proposeInfo.Module, proposeInfo.OperType, proposeInfo.Data)
				} else {
					printf("term=%d index=%d module=%s opertype=%d data=%s \n",
						entry.Term, entry.Index, proposeInfo.Module, proposeInfo.OperType, string(proposeInfo.Data))
				}
			}
		}
	}
}

func printf(format string, opts ...interface{}) {
	if logger != nil {
		logger.Printf(format, opts...)
	} else {
		fmt.Printf(format, opts...)
	}
}
