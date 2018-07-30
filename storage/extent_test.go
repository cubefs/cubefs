package storage

import (
	"bytes"
	"encoding/binary"
	"github.com/chubaoio/cbfs/util"
	"hash/crc32"
	"io"
	"math/rand"
	"os"
	"testing"
	"time"
)

func TestFsExtent_Write(t *testing.T) {
	var err error
	extent := NewExtentInCore("/tmp/extent_1", 1)
	if err = extent.InitToFS(1, true); err != nil {
		panic(err)
	}

	defer extent.Close()
	if err = extent.RestoreFromFS(); err != nil {
		panic(err)
	}
	offset := 0
	source := rand.NewSource(time.Now().Unix())
	r := rand.New(source)
	for i := 0; i < 1000; i++ {
		length := r.Intn(util.BlockSize)
		data := make([]byte, length)
		for i, _ := range data {
			data[i] = byte(r.Intn(256))
		}
		if err = extent.Write(data, int64(offset), int64(length), crc32.ChecksumIEEE(data)); err != nil {
			panic(err)
		}
		offset += length
	}
	extent.Flush()
}

func TestFsExtent_Validate(t *testing.T) {
	var err error
	extent := NewExtentInCore("/tmp/extent_1", 1)
	defer extent.Close()
	if err = extent.RestoreFromFS(); err != nil {
		panic(err)
	}
	fse := extent.(*fsExtent)
	header := fse.header
	headerReader := bytes.NewReader(header)
	var ino uint64
	binary.Read(headerReader, binary.BigEndian, &ino)
	if ino != 1 {
		t.Fatalf("ino act[%v] and exp[1]", ino)
	}
	var file *os.File
	file, err = os.OpenFile("/tmp/extent_1", os.O_RDONLY, 0666)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	fileOffset := util.BlockHeaderSize
	extentOffset := 0
	readBuff := make([]byte, util.BlockSize)
	var (
		fileCrc   uint32
		extentCrc uint32
		headerCrc uint32
	)
	for {
		if _, err = file.ReadAt(readBuff, int64(fileOffset)); err != nil && err != io.EOF {
			panic(err)
		}
		if err == io.EOF {
			return
		}
		binary.Read(headerReader, binary.BigEndian, &headerCrc)
		fileCrc = crc32.ChecksumIEEE(readBuff)
		extentCrc, err = extent.Read(readBuff, int64(extentOffset), int64(len(readBuff)))
		if fileCrc != extentCrc || extentCrc != headerCrc {
			t.Fatalf("%8x %8x %8x %10d %10d",
				fileCrc, extentCrc, headerCrc, fileOffset, extentOffset)
		}
		fileOffset += util.BlockSize
		extentOffset += util.BlockSize
		t.Logf("%8x %8x %8x %10d %10d",
			fileCrc, extentCrc, headerCrc, fileOffset, extentOffset)
	}

}
