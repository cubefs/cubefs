package main

/*
 * dump chunk info from chunk file
 */

import (
	"bytes"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"github.com/cubefs/cubefs/blobstore/blobnode/core/storage"
	"github.com/cubefs/cubefs/blobstore/common/crc32block"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

const (
	_pagesize = 4 * 1024

	// shard header size
	_shardHeaderSize = 4 + 4 + 8 + 8 + 4 + 4 // 32 (crc + magic + bid + vuid + size + reserved)
	// shard footer size
	_shardFooterSize = 4 + 4 // 8 (magic + checkSum)

	_shardCrcSize   = 4
	_shardMagicSize = 4
	_shardBidSize   = 8
	_shardVuidSize  = 8
	_shardSizeSize  = 4
	// _shardHeaderPaddingSize = _shardHeaderSize - _shardCrcSize - _shardMagicSize - _shardBidSize - _shardVuidSize - _shardSizeSize

	// shard header offset
	_shardCrcOffset      = 0
	_shardHdrMagicOffset = _shardCrcOffset + _shardCrcSize
	_shardBidOffset      = _shardHdrMagicOffset + _shardMagicSize
	_shardVuidOffset     = _shardBidOffset + _shardBidSize
	_shardSizeOffset     = _shardVuidOffset + _shardVuidSize
	_shardPaddingOffset  = _shardSizeOffset + _shardSizeSize
	// _shardDataOffset     = _shardPaddingOffset + _shardHeaderPaddingSize

	// footer offset
	_checksumSize = 4

	_shardFooterMagicOffset = 0
	_checksumOffset         = _shardFooterMagicOffset + _shardMagicSize
	_footerPaddingOffset    = _checksumOffset + _checksumSize
)

var (
	_chunkMagic       = []byte{0x20, 0x21, 0x03, 0x18}
	_shardHeaderMagic = []byte{0xab, 0xcd, 0xef, 0xcc}
	_shardFooterMagic = []byte{0xcc, 0xef, 0xcd, 0xab}
)

// header format:
// ---------------------
// |  crc    (uint32)  |
// |  magic  (uint32)  |
// |  bid    (int64)   |
// |  vuid   (uint64)  |
// |  size   (uint32)  |
// | padding (4 bytes) |
// ---------------------

type shardHeader struct {
	Crc     uint32
	Magic   [4]byte
	Bid     proto.BlobID
	Vuid    proto.Vuid
	Size    uint32
	Padding [4]byte
}

// footer format:
// ----------------------
// |  magic   (int32)   |
// | checksum (int32)   |
// | padding  (0 bytes) |
// ----------------------

var (
	// ErrShardHeaderMagic = errors.New("shard header magic")
	ErrShardHeaderSize  = errors.New("shard header size")
	ErrShardHeaderCrc   = errors.New("shard header crc not match")
	ErrShardFooterMagic = errors.New("shard footer magic")
	ErrShardFooterSize  = errors.New("shard footer size")
)

type shardFooter struct {
	Magic    [4]byte
	Checksum uint32
}

func main() {
	var (
		chunkDataFile        string
		checkChunkDataHeader bool
	)

	flag.StringVar(&chunkDataFile, "datafile", "", "chunk data file name")
	flag.BoolVar(&checkChunkDataHeader, "checkchunkheader", true, "check chunk header or not")
	flag.Parse()

	if len(chunkDataFile) == 0 {
		panic("chunk data can not be null")
	}

	file, err := os.OpenFile(chunkDataFile, os.O_RDONLY, 0)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	buff := make([]byte, _pagesize)

	n, err := file.ReadAt(buff, 0)
	if err != nil {
		log.Fatalf("err:%v", err)
	}
	if n != _pagesize {
		log.Fatalf("chunk data file size error")
	}

	if checkChunkDataHeader && !bytes.Equal(_chunkMagic, buff[:4]) {
		log.Fatalf("check chunk data header failed")
	}

	if err := dumpDataHeader(buff); err != nil {
		log.Fatalf("err:%v", err)
	}

	if buff[4] == byte(1) {
		dumpVersion1Data(file)
	}
}

func dumpDataHeader(buffHeader []byte) error {
	hdr := storage.ChunkHeader{}
	if err := hdr.Unmarshal(buffHeader); err != nil {
		return err
	}

	fmt.Printf("== chunkdata header ==\n%s\n", &hdr)
	return nil
}

func dumpVersion1Data(file *os.File) {
	var (
		pos      = int64(_pagesize)
		validCnt = 0
		err      error
		n        int
		sh       *shardHeader
		sf       *shardFooter
	)

	for {
		if err != nil {
			pos += _pagesize
		}

		headerBuff := make([]byte, _shardHeaderSize)
		n, err = file.ReadAt(headerBuff, pos)
		if err == io.EOF || n != _shardHeaderSize {
			fmt.Printf("chunk data file scan finish\n")
			break
		}

		if err != nil {
			panic(err)
		}

		if !bytes.Equal(headerBuff[_shardHdrMagicOffset:_shardBidOffset], _shardHeaderMagic) {
			pos += _pagesize
			continue
		}

		sh, err = parseHeader(headerBuff)
		if err != nil {
			fmt.Printf("parse header failed: %v\n", err)
			continue
		}

		fmt.Printf("%v\n", *sh)

		decoder, err := crc32block.NewDecoder(file, pos+_shardHeaderSize, int64(sh.Size))
		if err != nil {
			fmt.Println("new crc32 decoder failed", err)
			continue
		}
		reader, err := decoder.Reader(0, int64(sh.Size))
		if err != nil {
			fmt.Println("new reader failed", err)
			continue
		}

		userData := make([]byte, int(sh.Size))
		n, err = reader.Read(userData)
		if err != nil {
			fmt.Printf("read data failed\n")
			continue
		}

		if n != int(sh.Size) {
			fmt.Printf("read size error\n")
			pos += _pagesize
			continue
		}

		crc := crc32.NewIEEE()
		n, err = crc.Write(userData)
		if err != nil {
			fmt.Printf("calculate crc failed\n")
			continue
		}

		if n != int(sh.Size) {
			fmt.Printf("write size error\n")
			pos += _pagesize
			continue
		}

		physize := alignphysize(int64(sh.Size))

		footerBuff := make([]byte, _shardFooterSize)
		n, err = file.ReadAt(footerBuff, pos+physize-_shardFooterSize)
		if err == io.EOF || n != _shardFooterSize {
			fmt.Printf("chunk data file scan finished\n")
			break
		}

		if err != nil {
			panic(err)
		}

		sf, err = parseFooter(footerBuff)
		if err != nil {
			fmt.Printf("parse footer failed\n")
			continue
		}

		if sf.Checksum != crc.Sum32() {
			fmt.Printf("data check sum not equal, record value:%v, cal value:%v\n", sf.Checksum, crc.Sum32())
			continue
		}

		fmt.Printf("valid data, header:%v, footer:%v\n", *sh, *sf)

		pos += physize
		pos = AlignSize(pos, _pagesize)
		validCnt++
	}
	fmt.Printf("%v valid data count: %d\n", file.Name(), validCnt)
}

func parseHeader(buf []byte) (sh *shardHeader, err error) {
	if len(buf) != _shardHeaderSize {
		return nil, ErrShardHeaderSize
	}

	sh = new(shardHeader)
	copy(sh.Magic[:], buf[_shardHdrMagicOffset:_shardBidOffset])
	if !bytes.Equal(sh.Magic[:], _shardHeaderMagic[:]) {
		return nil, errcode.ErrInvalidParam
	}

	sh.Bid = proto.BlobID(binary.BigEndian.Uint64(buf[_shardBidOffset:_shardVuidOffset]))
	sh.Vuid = proto.Vuid(binary.BigEndian.Uint64(buf[_shardVuidOffset:_shardSizeOffset]))
	sh.Size = binary.BigEndian.Uint32(buf[_shardSizeOffset:_shardPaddingOffset])

	// shard header crc
	actualCrc := crc32.ChecksumIEEE(buf[_shardHdrMagicOffset:])
	sh.Crc = binary.BigEndian.Uint32(buf[_shardCrcOffset:_shardHdrMagicOffset])
	if actualCrc != sh.Crc {
		return nil, ErrShardHeaderCrc
	}

	return
}

func parseFooter(buf []byte) (sf *shardFooter, err error) {
	if len(buf) != _shardFooterSize {
		return nil, ErrShardFooterSize
	}

	sf = new(shardFooter)

	copy(sf.Magic[:], buf[_shardFooterMagicOffset:_checksumOffset])
	if !bytes.Equal(sf.Magic[:], _shardFooterMagic[:]) {
		return nil, ErrShardFooterMagic
	}

	sf.Checksum = binary.BigEndian.Uint32(buf[_checksumOffset:_footerPaddingOffset])

	return
}

func alignphysize(shardSize int64) int64 {
	bodysize := crc32block.EncodeSizeWithDefualtBlock(shardSize)
	return _shardHeaderSize + int64(bodysize) + _shardFooterSize
}

func AlignSize(p int64, bound int64) (r int64) {
	r = (p + bound - 1) & (^(bound - 1))
	return r
}
