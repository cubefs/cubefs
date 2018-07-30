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
