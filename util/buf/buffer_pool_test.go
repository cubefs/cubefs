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

package buf

import (
	"encoding/binary"
	"fmt"
	"github.com/chubaoio/cbfs/storage"
	"github.com/chubaoio/cbfs/util/pool"
	"hash/crc32"
	"math/rand"
	"testing"
)

func TestBufferPool_Get(t *testing.T) {
	cp := pool.NewBufferPool()
	for i := 0; i < 1024; i++ {
		buffer, err := cp.Get(storage.BlockSize + 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(buffer) != storage.BlockSize {
			t.FailNow()
		}
		buffer[i%storage.BlockSize] = uint8(rand.Intn(255))
		crc := crc32.ChecksumIEEE(buffer[:storage.BlockSize-4])
		binary.BigEndian.PutUint32(buffer[storage.BlockSize-4:storage.BlockSize], crc)
		cp.Put(buffer)
	}
	for i := 0; i < 1024; i++ {
		buffer, _ := cp.Get(storage.BlockSize)
		actualCrc := crc32.ChecksumIEEE(buffer[:storage.BlockSize-4])
		expectCrc := binary.BigEndian.Uint32(buffer[storage.BlockSize-4 : storage.BlockSize])
		fmt.Printf("i[%v] actualCrc[%v] expect[%v]\n", i, actualCrc, expectCrc)
		if actualCrc != expectCrc {
			fmt.Printf("i[%v] actualCrc[%v] expect[%v]\n", i, actualCrc, expectCrc)
			t.FailNow()
		}
	}
}
