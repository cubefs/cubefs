// Copyright 2022 The CubeFS Authors.
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

package blobnode

import (
	"bytes"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path"

	"github.com/desertbit/grumble"
	"github.com/dustin/go-humanize"

	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	"github.com/cubefs/cubefs/blobstore/blobnode/core/storage"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
	"github.com/cubefs/cubefs/blobstore/common/crc32block"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

const (
	_pagesize = 4 * 1024
	fileFlag  = os.O_CREATE | os.O_WRONLY | os.O_TRUNC
)

func addCmdChunkDumpData(chunkCommand *grumble.Command) {
	chunkCommand.AddCommand(&grumble.Command{
		Name: "data",
		Help: "dump shards in blobnode static chunk file",
		Flags: func(f *grumble.Flags) {
			f.Int64L("bid", 0, "dump bid")
			f.StringL("chunkfile", "", "chunk file path")
			f.StringL("metafile", "", "write meta of bids to file")
			f.StringL("datadir", "", "write data of bids to dir")
		},
		Run: chunkDumpData,
	})
}

func chunkDumpData(c *grumble.Context) error {
	chunkPath := c.Flags.String("chunkfile")
	if chunkPath == "" {
		return errors.New("--chunkfile is required")
	}

	f, err := os.OpenFile(chunkPath, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer f.Close()

	chunkHeaderBuff := make([]byte, _pagesize)
	if _, err = io.ReadFull(f, chunkHeaderBuff); err != nil {
		return err
	}
	chunkHeader := storage.ChunkHeader{}
	if err = chunkHeader.Unmarshal(chunkHeaderBuff); err != nil {
		return err
	}
	fmt.Println("----- chunk header -----")
	fmt.Println(chunkHeader)

	if chunkHeaderBuff[4] == byte(1) {
		fmt.Println("-----  dump stats  -----")
		return dumpChunkDataV1(f, c)
	}
	return fmt.Errorf("not support version %d", chunkHeaderBuff[4])
}

func dumpChunkDataV1(file *os.File, c *grumble.Context) error {
	metaPath := c.Flags.String("metafile")
	dataDir := c.Flags.String("datadir")
	blobID := proto.BlobID(c.Flags.Int64("bid"))
	var (
		metaFile *os.File
		err      error
	)
	if metaPath != "" {
		if metaFile, err = os.OpenFile(metaPath, fileFlag, 0o644); err != nil {
			return nil
		}
		defer metaFile.Close()
	}
	writeMeta := func(shard *core.Shard) error {
		if metaFile == nil {
			return nil
		}
		metaFile.Write([]byte(fmt.Sprintf("bid:%d size:%d crc:%d\n", shard.Bid, shard.Size, shard.Crc)))
		return nil
	}

	errFound := errors.New("found")
	writeData := func(bid proto.BlobID, data []byte) error {
		if bid == blobID {
			bidf, e := os.OpenFile(fmt.Sprintf("%d", bid), fileFlag, 0o644)
			if e != nil {
				return e
			}
			defer bidf.Close()
			if _, e = bidf.Write(data); e != nil {
				return e
			}

			if dataDir == "" {
				return errFound
			}
		}

		if dataDir == "" {
			return nil
		}
		bidFile, e := os.OpenFile(path.Join(dataDir, fmt.Sprintf("%d", bid)), fileFlag, 0o644)
		if e != nil {
			return e
		}
		defer bidFile.Close()
		_, e = bidFile.Write(data)
		return e
	}

	stats := struct {
		Nfile  int
		Size   uint64 // chunk size (header + footer + align + phy) + chunk header
		Hole   uint64 // hole of shards
		Phy    uint64 // shards physical size
		Header uint64 // header size
		Footer uint64 // footer size
		Align  uint64 // align of shard
	}{}
	stats.Size += _pagesize // chunk header

	defer func() {
		fmt.Println("Number   File:", stats.Nfile)
		fmt.Printf("Chunk    Size: %d (%s)\n", stats.Size, humanize.IBytes(stats.Size))
		fmt.Printf("Hole     Size: %d (%s)\n", stats.Hole, humanize.IBytes(stats.Hole))
		fmt.Printf("Physical Size: %d (%s)\n", stats.Phy, humanize.IBytes(stats.Phy))
		fmt.Printf("Header   Size: %d (%s)\n", stats.Header, humanize.IBytes(stats.Header))
		fmt.Printf("Footer   Size: %d (%s)\n", stats.Footer, humanize.IBytes(stats.Footer))
		fmt.Printf("Align    Size: %d (%s)\n", stats.Align, humanize.IBytes(stats.Align))
	}()

	buff := make([]byte, 4*(1<<20)) // 4M
	getBuff := func(size int) []byte {
		if size <= cap(buff) {
			return buff[:size]
		}
		buff = make([]byte, size)
		return buff
	}

	headerSize := int(core.GetShardHeaderSize())
	footerSize := int(core.GetShardFooterSize())
	holeBuff := make([]byte, headerSize)

	pos := int64(_pagesize)
	for {
		shard := &core.Shard{}
		file.Seek(pos, io.SeekStart)
		headerBuff := getBuff(headerSize)
		if _, err = io.ReadFull(file, headerBuff); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if err = shard.ParseHeader(headerBuff); err != nil {
			if bytes.Equal(headerBuff, holeBuff) {
				stats.Hole += _pagesize
				pos += _pagesize
				continue
			}
			return err
		}

		decoder, err := crc32block.NewDecoder(file, pos+int64(headerSize), int64(shard.Size))
		if err != nil {
			return err
		}
		reader, err := decoder.Reader(0, int64(shard.Size))
		if err != nil {
			return err
		}

		dataBuff := getBuff(int(shard.Size))
		if _, err = io.ReadFull(reader, dataBuff); err != nil {
			return err
		}
		if err = writeData(shard.Bid, dataBuff); err != nil {
			if err == errFound {
				return nil
			}
			return err
		}

		crc := crc32.NewIEEE()
		crc.Write(dataBuff)

		footerBuff := getBuff(footerSize)
		allsize := core.Alignphysize(int64(shard.Size))
		file.Seek(pos+allsize-int64(footerSize), io.SeekStart)
		if _, err = io.ReadFull(file, footerBuff); err != nil {
			return err
		}
		if err = shard.ParseFooter(footerBuff); err != nil {
			return err
		}
		if actual := crc.Sum32(); actual != shard.Crc {
			return fmt.Errorf("crc mismatch bid:%d record:%d actual:%d",
				shard.Bid, shard.Crc, actual)
		}

		writeMeta(shard)
		pos += allsize
		pos = alignSize(pos, _pagesize)

		stats.Nfile++
		stats.Size += uint64(allsize)
		stats.Phy += uint64(shard.Size)
		stats.Header += uint64(headerSize)
		stats.Footer += uint64(footerSize)
		stats.Align += uint64(allsize) - uint64(shard.Size) - uint64(headerSize+footerSize)
	}
}

func alignSize(p int64, bound int64) int64 {
	return (p + bound - 1) & (^(bound - 1))
}
