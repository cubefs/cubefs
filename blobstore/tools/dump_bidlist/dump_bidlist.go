package main

/*
 * dump shard bid list from chunk file
 * @chunkName:
 */

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

const (
	_chunkHeaderSize = 4096
	_shardHeaderSize = 32
	_pagesize        = 4096
	_minReadBuff     = 1024 * 1024
	_shardCrcSize    = 4
)

var _shardHeaderMagic = []byte{0xab, 0xcd, 0xef, 0xcc}

func main() {
	chunkName := flag.String("chunkName", "", "input chunk name")
	flag.Parse()

	if *chunkName == "" {
		log.Fatal("arguments need to be specified")
	}

	f, err := os.Open(*chunkName)
	if err != nil {
		log.Fatal(err)
	}

	var sd core.Shard

	readBuff := make([]byte, _minReadBuff)
	pos := int64(_chunkHeaderSize)
	headerBuff := make([]byte, _shardHeaderSize)

	for {
		sd, err = getHeader(f, headerBuff, pos)
		if err == io.EOF {
			break
		}
		if err != nil {
			hdrOff, err := nextValidHeader(f, pos, headerBuff, readBuff)
			if err == io.EOF {
				break
			}
			if err == nil {
				log.Infof("nextValidHeader from %d to %d\n", pos, hdrOff)
				pos = hdrOff
				continue
			}
		}
		phySize := core.Alignphysize(int64(sd.Size))
		realPhySize := core.AlignSize(phySize, _pagesize)
		fmt.Printf("bid:%v,offset:%v\n", sd.Bid, pos)
		pos += realPhySize
	}
}

func getHeader(f *os.File, headerBuff []byte, pos int64) (sd core.Shard, err error) {
	_, err = f.ReadAt(headerBuff, pos)
	if err != nil {
		return
	}
	err = sd.ParseHeader(headerBuff)
	if err != nil {
		return
	}
	return
}

func nextValidHeader(f *os.File, pos int64, headerBuff []byte, readBuff []byte) (hdrOff int64, err error) {
	for {
		n, err := f.ReadAt(readBuff, pos)
		magicOff := bytes.Index(readBuff[:n], _shardHeaderMagic[:])
		if magicOff < 0 {
			if err != nil {
				return 0, err
			}
			pos += int64(n - len(_shardHeaderMagic) + 1)
			continue
		}
		hdrOff = pos + int64(magicOff-_shardCrcSize)
		_, err = getHeader(f, headerBuff, hdrOff)
		if err == nil {
			return hdrOff, nil
		}
		if err != nil {
			pos += int64(magicOff + 1)
			continue
		}
	}
}
