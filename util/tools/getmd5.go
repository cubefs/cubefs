// Copyright 2018 The ChuBao Authors.
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
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

const (
	BlockHeaderInoSize     = 8
	BlockHeaderCrcSize     = PerBlockCrcSize * BlockCount
	BlockHeaderCrcIndex    = BlockHeaderInoSize
	BlockHeaderDelMarkSize = 1
	BlockHeaderSize        = BlockHeaderInoSize + BlockHeaderCrcSize + BlockHeaderDelMarkSize
	BlockCount             = 1024
	MarkDelete             = 'D'
	UnMarkDelete           = 'U'
	MarkDeleteIndex        = BlockHeaderSize - 1
	BlockSize              = 65536 * 4
	ReadBlockSize          = 65536
	PerBlockCrcSize        = 4
	DeleteIndexFileName    = "delete.index"
	ExtentSize             = BlockCount * BlockSize
	ExtentFileSizeLimit    = BlockHeaderSize + ExtentSize
	PacketHeaderSize       = 45
)

var (
	name = flag.String("name", "f", "read file name")
)

func main() {
	flag.Parse()
	f, err := os.Open(*name)
	if err != nil {
		fmt.Println(err)
		return
	}
	var offset int64
	offset = BlockHeaderSize
	if err != nil {
		fmt.Println(err)
		return
	}
	exist := false
	blockNo := 0
	for {
		data := make([]byte, BlockSize)
		n, err := f.ReadAt(data, offset)
		if err == io.EOF {
			exist = true
		}
		crc := crc32.ChecksumIEEE(data[:n])
		fmt.Println(fmt.Sprintf("blockNO[%v] crc[%v] n[%v]", blockNo, crc, n))
		offset += int64(n)
		blockNo += 1
		if exist {
			break
		}
	}
	f.Close()
}
