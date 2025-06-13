// Copyright 2025 The CubeFS Authors.
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

package storage

import (
	"hash/crc32"
	"io"
	"net/http"
	"time"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	"github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/util/bytespool"
)

type shardsDecoder struct {
	from      io.ReaderAt
	bids      []bnapi.BidInfo
	threshold int64
	buf       []byte
	bufSize   int64 // buf size % core.CrcBlockUnitSize must be 0 and >= 2 * core.CrcBlockUnitSize
	duration  time.Duration

	segments []segment
	idx      int
	rc       WriterToDuration
}

type WriterToDuration interface {
	io.WriterTo
	Duration() time.Duration
}

func newShardsReader(from io.ReaderAt, bids []bnapi.BidInfo, threshold int64, bufSize int64) core.WriteToCloser {
	reader := &shardsDecoder{}
	reader.from = from
	reader.bids = bids
	reader.threshold = threshold
	reader.buf = bytespool.Alloc(int(core.AlignSize(bufSize, _pageSize)))
	reader.bufSize = bufSize
	reader.segments = reader.findContiguousSegments()
	return reader
}

func (d *shardsDecoder) Close() error {
	bytespool.Free(d.buf)
	d.buf = nil
	return nil
}

func (d *shardsDecoder) WriteTo(w io.Writer) (n int64, err error) {
	if d.rc == nil {
		d.idx = 0
		err = d.readNextSeg()
		if err != nil {
			return
		}
	}
	write := int64(0)
	for {
		write, err = d.rc.WriteTo(w)
		n += write
		if err != nil && err != io.EOF {
			d.duration += d.rc.Duration()
			return
		}
		if err == io.EOF {
			d.duration += d.rc.Duration()
			err = d.readNextSeg()
			if err != nil {
				if n != 0 {
					err = nil
				}
				return
			}
		}
	}
}

func (d *shardsDecoder) Duration() time.Duration { return d.duration }

func (d *shardsDecoder) readNextSeg() (err error) {
	if d.idx >= len(d.segments) {
		return io.EOF
	}
	bids := d.bids
	seg := d.segments[d.idx]
	start := bids[seg.Start].Offset
	end := bids[seg.End].Offset + core.Alignphysize(bids[seg.End].Size) - core.FooterSize
	want := end - start
	d.idx++
	d.rc = &segmentDecoder{
		from: io.NewSectionReader(d.from, start, want),
		bids: bids[seg.Start : seg.End+1],
		buf:  d.buf,
		want: want,
	}
	return
}

type segment struct {
	Start int
	End   int
}

// threshold is threshold of trash data size to real data size of the segment
func (d *shardsDecoder) findContiguousSegments() []segment {
	if len(d.bids) <= 1 {
		return []segment{{0, 0}}
	}

	bidInfos := d.bids
	threshold := d.threshold
	maxSize := d.bufSize

	n := len(bidInfos)
	rs := make([]segment, 0)
	for i := 0; i < n; {
		start := i
		for j := i + 1; j < n; j++ {
			sj := core.Alignphysize(bidInfos[j].Size)
			offsetL := bidInfos[j].Offset + sj - bidInfos[i].Offset
			hole := bidInfos[j].Offset - bidInfos[j-1].Offset - core.AlignSize(core.Alignphysize(bidInfos[j-1].Size), _pageSize)
			if hole <= threshold && offsetL <= maxSize {
				if j == n-1 {
					rs = append(rs, segment{Start: start, End: j})
					return rs
				}
				continue
			}
			rs = append(rs, segment{Start: start, End: j - 1})
			i = j
			break
		}
		if i == n-1 {
			rs = append(rs, segment{Start: i, End: i})
			break
		}
	}
	return rs
}

type segmentDecoder struct {
	from     io.Reader
	bids     []bnapi.BidInfo
	buffered bool
	buf      []byte
	want     int64
	duration time.Duration

	idx int
	sd  *shardDecoder
}

func (d *segmentDecoder) Duration() time.Duration { return d.duration }

func (d *segmentDecoder) WriteTo(w io.Writer) (n int64, err error) {
	if !d.buffered {
		st := time.Now()
		_, err = io.ReadFull(d.from, d.buf[:d.want])
		d.duration += time.Since(st)
		if err != nil {
			return
		}
		err = d.readNextShard()
		if err != nil {
			return
		}
		d.buffered = true
	}
	write := int64(0)
	for {
		write, err = d.sd.WriteTo(w)
		n += write
		if err != nil && err != io.EOF {
			return
		}
		if err == io.EOF {
			err = d.readNextShard()
			if err != nil {
				return
			}
		}
	}
}

func (d *segmentDecoder) readNextShard() error {
	if d.idx == len(d.bids) {
		return io.EOF
	}
	bid := d.bids[d.idx]
	start := bid.Offset - d.bids[0].Offset
	end := start + core.Alignphysize(bid.Size) - core.FooterSize

	d.idx++
	d.sd = &shardDecoder{bid: bid.Bid, size: bid.Size, buf: d.buf[start:end]}
	return nil
}

type shardDecoder struct {
	bid  proto.BlobID
	buf  []byte
	size int64

	checked bool
	sw      io.WriterTo
}

func (d *shardDecoder) WriteTo(w io.Writer) (n int64, err error) {
	if d.checked {
		return d.sw.WriteTo(w)
	}
	header := http.StatusOK
	err = d.checkHeader()
	if err != nil {
		header = errors.CodeBidNotMatch
	}
	start := core.HeaderSize
	wt := &blockDecoder{
		buf:      d.buf[start:],
		remain:   int(d.size),
		blockLen: core.CrcBlockUnitSize,
		crcSize:  crc32.Size,
	}
	d.sw = bnapi.NewShardWriter(header, wt)
	d.checked = true
	return d.sw.WriteTo(w)
}

func (d *shardDecoder) checkHeader() error {
	var ns core.Shard
	err := ns.ParseHeader(d.buf[:core.HeaderSize])
	if err != nil {
		return err
	}
	if d.bid != ns.Bid {
		return ErrShardHeaderNotMatch
	}
	return nil
}

type blockDecoder struct {
	remain int
	buf    []byte

	blockLen int
	crcSize  int
	offset   int
}

func (d *blockDecoder) WriteTo(w io.Writer) (n int64, err error) {
	if d.remain <= 0 {
		err = io.EOF
		return
	}
	write := 0
	end := d.calculate()
	write, err = w.Write(d.buf[d.offset:end])
	d.offset += write
	d.remain -= write
	return int64(write), err
}

func (d *blockDecoder) calculate() int {
	m := d.offset % d.blockLen
	if m == 0 {
		// skip crc data
		d.offset += d.crcSize
		m += d.crcSize
	}
	payload := d.blockLen - m
	if payload >= d.remain {
		payload = d.remain
	}
	end := d.offset + payload
	return end
}
