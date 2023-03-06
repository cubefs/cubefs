package main

/*
 * dump shard data from chunk file
 * @chunkName:
 * @offset:
 */

import (
	"bytes"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"

	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	"github.com/cubefs/cubefs/blobstore/common/crc32block"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

var (
	ErrShardFooterSize  = errors.New("shard footer size")
	ErrShardFooterMagic = errors.New("shard footer magic")
)

var _shardFooterMagic = []byte{0xcc, 0xef, 0xcd, 0xab}

const (
	_shardFooterSize        = 8
	_shardFooterMagicOffset = 0
	_checksumOffset         = 4
	_footerPaddingOffset    = 8
	_shardHeaderSize        = 32
)

type shardFooter struct {
	Magic    [4]byte
	Checksum uint32
}

func main() {
	var (
		chunkName = flag.String("chunkName", "", "input chunk name")
		offset    = flag.Int64("offset", 0, "input shard offset")
	)
	flag.Parse()

	if *chunkName == "" || *offset == 0 {
		log.Fatal("arguments need to be specified")
	}

	f, err := os.Open(*chunkName)
	if err != nil {
		log.Fatal(err)
	}

	userData, err := getData(f, offset)
	if err != nil {
		log.Error(err)
		return
	}

	err = ioutil.WriteFile("dumpdata", userData, 0o644)
	if err != nil {
		log.Error(err)
	}
}

func getData(f *os.File, offset *int64) (userData []byte, err error) {
	var ns core.Shard
	pos := *offset
	buf := make([]byte, _shardHeaderSize)
	_, err = f.ReadAt(buf, pos)
	if err != nil {
		log.Error(err)
		return
	}
	err = ns.ParseHeader(buf)
	if err != nil {
		log.Error(err)
		return
	}
	fmt.Printf("shard bid:%v,vuid:%v,size:%v\n", ns.Bid, ns.Vuid, ns.Size)

	decoder, err := crc32block.NewDecoder(f, pos+_shardHeaderSize, int64(ns.Size))
	if err != nil {
		fmt.Println("new crc32 decoder failed", err)
		return
	}
	reader, err := decoder.Reader(0, int64(ns.Size))
	if err != nil {
		fmt.Println("new reader failed", err)
		return
	}

	userData = make([]byte, int(ns.Size))
	n, err := reader.Read(userData)
	if err != nil {
		log.Errorf("read data failed.\n")
		return
	}

	if n != int(ns.Size) {
		log.Errorf("read size error, n:%v,ns.Size:%v\n", n, ns.Size)
		return
	}

	crc := crc32.NewIEEE()
	_, err = crc.Write(userData)
	if err != nil {
		log.Errorf("calculate crc failed\n")
		return
	}

	physize := core.Alignphysize(int64(ns.Size))

	footerBuff := make([]byte, _shardFooterSize)
	n, err = f.ReadAt(footerBuff, pos+physize-_shardFooterSize)
	if err == io.EOF || n != _shardFooterSize {
		log.Errorf("chunk data file scan finished\n")
		return
	}

	if err != nil {
		log.Error(err)
		return
	}

	sf, err := parseFooter(footerBuff)
	if err != nil {
		log.Errorf("parse footer failed\n")
		return
	}

	if sf.Checksum != crc.Sum32() {
		log.Errorf("data check sum not equal, record value:%v, cal value:%v\n", sf.Checksum, crc.Sum32())
		return
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
