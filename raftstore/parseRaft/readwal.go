// Copyright 2018 The CFS Authors.
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

package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/chubaofs/chubaofs/metanode"
	"io/ioutil"
)

type OpKvData struct {
	Op uint32 `json:"op"`
	K  string `json:"k"`
	V  []byte `json:"v"`
}

func main() {
	fmt.Println("Read raft wal record")
	readWal("/home/guowl/1394/0000000000000001-0000000000000001.log")
	return
}

func readWal(name string) {
	var (
		fileOffset uint64
		dataSize   uint64
		dataString string
	)

	data, _ := ioutil.ReadFile(name)
	fmt.Println("data len", len(data))
	var err error
	fileOffset = 0

	for {
		if fileOffset >= uint64(len(data)) {
			return
		}

		dataSize = 0
		dataTemp := data[fileOffset:]
		//first byte is record type
		recordType := dataTemp[0]
		//sencond 8 bytes is datasize
		dataSize = binary.BigEndian.Uint64(dataTemp[1:9])
		//third byte is op type
		opType := dataTemp[9]
		//and 8+8 types is term and index
		term := binary.BigEndian.Uint64(dataTemp[10:18])
		index := binary.BigEndian.Uint64(dataTemp[18:26])

		if dataSize > 17 {
			if opType == 0 {
				cmd := new(OpKvData)
				if err = json.Unmarshal(dataTemp[26:26+dataSize-17], cmd); err != nil {
					fmt.Println("unmarshal fail", cmd, err)
					return
				}
				dataString = fmt.Sprintf("opt:%v, k:%v, v:%v", cmd.Op, cmd.K, cmd.V)
				if cmd.Op == 0 {
					ino := metanode.NewInode(0, 0)
					if err = ino.Unmarshal(context.Background(), cmd.V); err != nil {
						continue
					}
					if ino.Inode == 33570077 {
						fmt.Println(fmt.Sprintf("create inode %v", ino))
					}

				} else if cmd.Op == 17 {
					req := &metanode.SetattrRequest{}
					err = json.Unmarshal(cmd.V, req)
					if err != nil {
						continue
					}
					if req.Inode == 33570077 {
						fmt.Println(fmt.Sprintf("set attr inode %v", req))
					}
				}

			} else if opType == 1 {
				cType := dataTemp[26]
				pType := dataTemp[27]
				prt := binary.BigEndian.Uint16(dataTemp[28:30])
				pid := binary.BigEndian.Uint64(dataTemp[30:38])
				dataString = fmt.Sprintf("cngType:%v, peerType:%v, priority:%v, id:%v", cType, pType, prt, pid)
			}

		}
		crcOffset := 9 + dataSize
		crc := binary.BigEndian.Uint32(dataTemp[crcOffset : crcOffset+4])
		fmt.Sprintf("recType[%v] dataSize[%v] opType[%v] term[%v] index[%v] data[%v] crc[%x]", recordType, dataSize, opType, term, index, dataString, crc)
		recordSize := 1 + 8 + dataSize + 4
		fileOffset = fileOffset + recordSize
	}

}
