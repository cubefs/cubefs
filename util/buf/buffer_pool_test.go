package buf

import (
	"encoding/binary"
	"fmt"
	"github.com/tiglabs/baudstorage/storage"
	"github.com/tiglabs/baudstorage/util/pool"
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
