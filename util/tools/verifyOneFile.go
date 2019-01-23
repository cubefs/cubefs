// Copyright 2018 The Container File System Authors.
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

package tools

import (
	"encoding/binary"
	"flag"
	"fmt"
	"hash/crc32"
	"os"
)

var (
	filename = flag.String("name", "", "file name")
)

func main() {
	flag.Parse()
	if *filename == "" {
		fmt.Printf("Error: Filename not specified!\n")
		return
	}
	fp, err := os.Open(*filename)
	if err != nil {
		fmt.Printf("Error: open file(%v) err(%v)\n", *filename, err)
		return
	}
	defer fp.Close()

	stat, err := fp.Stat()
	if err != nil {
		fmt.Printf("Error: stat file(%v) err(%v)\n", *filename, err)
		return
	}

	size := stat.Size()
	data := make([]byte, size)
	var n int
	if n, err = fp.Read(data); err != nil {
		fmt.Printf("Error: read file(%v) n(%v) err(%v)\n", *filename, n, err)
		return
	}

	writeCrc := binary.BigEndian.Uint32(data[size-4:])
	actualCrc := crc32.ChecksumIEEE(data[:size-4])
	if writeCrc != actualCrc {
		fmt.Printf("name(%v) crc not match actualCrc(%v) writeCrc(%v) size(%v) readn(%v)\n", *filename, actualCrc, writeCrc, size, n)
		return
	}

	fmt.Println("Done\n")
	return
}
