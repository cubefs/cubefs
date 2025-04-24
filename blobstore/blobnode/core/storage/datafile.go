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

package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"syscall"
	"time"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	bncomm "github.com/cubefs/cubefs/blobstore/blobnode/base"
	"github.com/cubefs/cubefs/blobstore/blobnode/base/qos"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	"github.com/cubefs/cubefs/blobstore/common/crc32block"
	bloberr "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/bytespool"
	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/cubefs/cubefs/blobstore/util/taskpool"
)

// Chunkdata has a header (4k).
// Chunkdata header format:
//  --------------
// | magic number |   ---- 4 bytes
// | version      |   ---- 1 byte
// | parent chunk |   ---- 16 byte
// | create time  |   ---- 8 byte
// | padding      |   ---- aligned with shard padding size ( 4k-4-1-16-8)
//  --------------
// |    shard     |
// |    shard     |
// |    shard     |
// |    shard     |
// |    shard     |
// |    ....      |

const (
	// chunk size
	_chunkHeaderSize      = 4 * 1024
	_chunkMagicSize       = 4
	_chunkVerSize         = 1
	_chunkParentChunkSize = bnapi.ChunkIdLength
	_chunkCreateTimeSize  = 8
	//_chunkPaddingSize     = _chunkHeaderSize - _chunkMagicSize - _chunkVerSize - _chunkParentChunkSize - _chunkCreateTimeSize

	// chunk offset
	_chunkMagicOffset       = 0
	_chunkVerOffset         = _chunkMagicOffset + _chunkMagicSize
	_chunkParentChunkOffset = _chunkVerOffset + _chunkVerSize
	_chunkCreateTimeOffset  = _chunkParentChunkOffset + _chunkParentChunkSize
	//_chunkPaddingOffset     = _chunkCreateTimeOffset + _chunkCreateTimeSize

	_pageSize  = 4 * 1024 // 4k
	sectorSize = 512
)

var (
	chunkHeaderMagic = [_chunkMagicSize]byte{0x20, 0x21, 0x03, 0x18}

	ErrShardOffNotAlignment = errors.New("chunkdata: shard offset not alignment")
	ErrShardHeaderNotMatch  = errors.New("chunkdata: shard header not match")
	ErrChunkDataMagic       = errors.New("chunkdata: magic not match")
	ErrChunkHeaderBufSize   = errors.New("chunkdata: buf size not match")
)

type ChunkHeader struct {
	magic       [_chunkMagicSize]byte
	version     byte
	parentChunk bnapi.ChunkId
	createTime  int64
}

type datafile struct {
	ef    core.BlobFile
	wOff  int64
	wLock sync.RWMutex

	File   string
	chunk  bnapi.ChunkId
	header ChunkHeader
	conf   *core.Config

	ioQos  qos.Qos
	closed bool
}

func (hdr *ChunkHeader) Marshal() ([]byte, error) {
	buf := make([]byte, _chunkHeaderSize)

	// magic
	copy(buf[_chunkMagicOffset:], hdr.magic[:])
	// ver
	copy(buf[_chunkVerOffset:], []byte{hdr.version})
	// parent chunk
	copy(buf[_chunkParentChunkOffset:], hdr.parentChunk[:])
	// create time
	binary.BigEndian.PutUint64(buf[_chunkCreateTimeOffset:], uint64(hdr.createTime))

	return buf, nil
}

func (hdr *ChunkHeader) Unmarshal(data []byte) error {
	if len(data) != _chunkHeaderSize {
		panic(ErrChunkHeaderBufSize)
	}

	magic := data[_chunkMagicOffset : _chunkMagicOffset+_chunkMagicSize]
	if !bytes.Equal(magic, chunkHeaderMagic[:]) {
		return ErrChunkDataMagic
	}
	hdr.magic = chunkHeaderMagic
	hdr.version = data[_chunkVerOffset : _chunkVerOffset+_chunkVerSize][0]
	copy(hdr.parentChunk[:], data[_chunkParentChunkOffset:_chunkParentChunkOffset+_chunkParentChunkSize])
	hdr.createTime = int64(binary.BigEndian.Uint64(data[_chunkCreateTimeOffset : _chunkCreateTimeOffset+_chunkCreateTimeSize]))

	return nil
}

func (hdr *ChunkHeader) String() string {
	ctime := time.Unix(0, hdr.createTime)
	s := fmt.Sprintf("magic:\t%v\nversion:\t%v\nparent:\t%s\nctime:\t%s",
		hdr.magic, hdr.version, hdr.parentChunk, ctime)
	return s
}

func NewChunkData(ctx context.Context, vm core.VuidMeta, file string, conf *core.Config, createIfMiss bool, ioQos qos.Qos, readPool taskpool.IoPool, writePool taskpool.IoPool) (
	cd *datafile, err error,
) {
	span := trace.SpanFromContextSafe(ctx)

	if file == "" || conf == nil {
		span.Errorf("file:%s, conf:%v, create:%v", file, conf, createIfMiss)
		return nil, bloberr.ErrInvalidParam
	}

	fd, err := core.OpenFile(file, createIfMiss)
	if err != nil {
		err = fmt.Errorf("os.OpenFile(\"%s\") error(%v)", file, err)
		return nil, err
	}

	handleIOError := func(err error) {
		conf.HandleIOError(context.Background(), vm.DiskID, err)
	}

	ef := core.NewBlobFile(fd, handleIOError, uint64(vm.ChunkId.VolumeUnitId()), readPool, writePool)

	cd = &datafile{
		File:   file,
		chunk:  vm.ChunkId,
		conf:   conf,
		closed: false,
		ef:     ef,
		ioQos:  ioQos,
	}

	if err = cd.init(&vm); err != nil {
		err = fmt.Errorf("block: %s init() error(%v)", file, err)
		cd.Close()
		return nil, err
	}

	return cd, nil
}

func (cd *datafile) initHeader(meta *core.VuidMeta) {
	cd.header = ChunkHeader{
		magic:       chunkHeaderMagic,
		version:     meta.Version,
		parentChunk: meta.ParentChunk,
		createTime:  meta.Ctime,
	}
}

func (cd *datafile) init(meta *core.VuidMeta) (err error) {
	var sysstat syscall.Stat_t

	if sysstat, err = cd.ef.SysStat(); err != nil {
		return
	}

	chunkSize := sysstat.Size
	if chunkSize == 0 {
		// first time. auto format
		cd.initHeader(meta)
		if err = cd.writeMeta(); err != nil {
			return
		}
		cd.wOff = _chunkHeaderSize
	} else {
		if err = cd.parseMeta(); err != nil {
			return
		}
		cd.wOff = core.AlignSize(chunkSize, int64(_pageSize))
	}

	return
}

func (cd *datafile) Flush() (err error) {
	if cd.conf.DisableSync {
		return
	}

	return cd.ef.Sync()
}

func (cd *datafile) Close() {
	if cd.ef == nil {
		cd.closed = true
		return
	}

	if err := cd.Flush(); err != nil {
		log.Errorf("flush err(%v)", err)
	}

	if err := cd.ef.Close(); err != nil {
		log.Errorf("close err(%v)", err)
	}

	cd.ef = nil
	cd.closed = true
}

func (cd *datafile) writeMeta() (err error) {
	// allocate 4k
	if err = cd.ef.Allocate(0, _chunkHeaderSize); err != nil {
		return
	}

	// marshal header
	buf, _ := cd.header.Marshal()

	// write to file
	if _, err = cd.ef.WriteAt(buf, _chunkMagicOffset); err != nil {
		return
	}

	if err = cd.ef.Sync(); err != nil {
		return
	}

	return nil
}

func (cd *datafile) parseMeta() (err error) {
	buf := make([]byte, _chunkHeaderSize)
	if _, err = cd.ef.ReadAt(buf[:_chunkHeaderSize], 0); err != nil {
		return
	}

	hdr := &ChunkHeader{}
	if err = hdr.Unmarshal(buf); err != nil {
		return
	}

	cd.header = *hdr
	return
}

func (cd *datafile) allocSpace(fsize int64) (pos int64, err error) {
	cd.wLock.Lock()
	defer cd.wLock.Unlock()

	pos = cd.wOff

	cd.wOff += fsize
	cd.wOff = core.AlignSize(cd.wOff, _pageSize)

	return pos, nil
}

func (cd *datafile) Write(ctx context.Context, shard *core.Shard) error {
	span := trace.SpanFromContextSafe(ctx)

	if !cd.qosAllow(ctx, qos.IOTypeWrite) { // If there is too much io, it will discard some low-priority io
		return bloberr.ErrOverload
	}
	defer cd.qosRelease(qos.IOTypeWrite)

	// allocate space
	phySize := core.Alignphysize(int64(shard.Size))
	pos, err := cd.allocSpace(phySize)
	if err != nil {
		return err
	}
	shard.Offset = pos

	if shard.Size <= core.SmallIOSize {
		return cd.writeSmalllIO(ctx, shard)
	}

	headerBuf := bytespool.Alloc(core.HeaderSize)
	footerBuf := bytespool.Alloc(core.FooterSize)
	defer bytespool.Free(headerBuf) // nolint: staticcheck
	defer bytespool.Free(footerBuf) // nolint: staticcheck

	qoswAt := cd.qosWriterAt(ctx, cd.ef)

	// header
	err = shard.WriterHeader(headerBuf)
	if err != nil {
		return err
	}

	start := time.Now()

	_, err = qoswAt.WriteAt(headerBuf, pos) // qos write to raw
	span.AppendTrackLog("hdr.w", start, err)
	if err != nil {
		return err
	}

	pos += core.GetShardHeaderSize()

	w := &bncomm.Writer{WriterAt: cd.ef, Offset: pos}
	twRaw := bncomm.NewTimeWriter(w)

	qosw := cd.qosWriter(ctx, twRaw)

	crc := crc32.NewIEEE()
	body := io.LimitReader(shard.Body, int64(shard.Size))
	body = io.TeeReader(body, crc)

	buffer := bytespool.Alloc(core.CrcBlockUnitSize)
	defer bytespool.Free(buffer) // nolint: staticcheck

	tw := bncomm.NewTimeWriter(qosw)
	tr := bncomm.NewTimeReader(body)

	// write shard body
	// encoder, err := crc32block.NewEncoder2(buffer, footerBuf)
	encoder, err := crc32block.NewEncoder(buffer)
	if err != nil {
		return err
	}

	_, err = encoder.Encode(tr, int64(shard.Size), tw)
	span.AppendTrackLogWithDuration("net.r", tr.Duration(), err)
	span.AppendTrackLogWithDuration("dat.w", twRaw.Duration(), err)
	span.AppendTrackLogWithDuration("dat.wai", tw.Duration()-twRaw.Duration(), err)

	if err != nil {
		if _, ok := err.(crc32block.ReaderError); ok {
			err = bloberr.ErrReaderError
		}
		return err
	}

	shard.Crc = crc.Sum32()

	// write footer
	err = shard.WriterFooter(footerBuf)
	if err != nil {
		return err
	}

	pos = w.Offset
	start = time.Now()

	_, err = qoswAt.WriteAt(footerBuf, pos)
	span.AppendTrackLog("fo.w", start, err)
	if err != nil {
		return err
	}

	return nil
}

func (cd *datafile) writeSmalllIO(ctx context.Context, shard *core.Shard) error {
	span := trace.SpanFromContextSafe(ctx)

	buffer := bytespool.Alloc(int(shard.Size) + core.HeaderSize + core.CrcSize + core.FooterSize)
	defer bytespool.Free(buffer) // nolint: staticcheck

	headerCrcBuf := buffer[:core.HeaderSize+core.CrcSize]
	footerBuf := buffer[len(headerCrcBuf)+int(shard.Size):]

	// read payload data from request to buffer
	tr := bncomm.NewTimeReader(shard.Body)
	n, err := io.ReadFull(shard.Body, buffer[len(headerCrcBuf):len(headerCrcBuf)+int(shard.Size)])

	span.AppendTrackLogWithDuration("net.r", tr.Duration(), err)
	if err != nil {
		return err
	}

	if n != int(shard.Size) {
		return fmt.Errorf("read size not match, expect:%d, actual:%d", shard.Size, n)
	}

	// calculate crc
	crc := crc32.NewIEEE()
	crc.Write(buffer[len(headerCrcBuf) : len(headerCrcBuf)+int(shard.Size)])
	shard.Crc = crc.Sum32()

	// fill block payload crc
	binary.LittleEndian.PutUint32(headerCrcBuf[core.HeaderSize:], shard.Crc)

	// fill header
	if err = shard.WriterHeader(headerCrcBuf[:core.HeaderSize]); err != nil {
		return err
	}

	// fill footer
	if err = shard.WriterFooter(footerBuf); err != nil {
		return err
	}

	w := &bncomm.Writer{WriterAt: cd.ef, Offset: shard.Offset}
	twRaw := bncomm.NewTimeWriter(w)
	qosw := cd.qosWriter(ctx, twRaw)
	tw := bncomm.NewTimeWriter(qosw)

	// write all io at once: header + blockCrc + payload + footer, write to chunk file
	_, err = twRaw.Write(buffer)
	span.AppendTrackLogWithDuration("dat.w", twRaw.Duration(), err)
	span.AppendTrackLogWithDuration("dat.wai", tw.Duration()-twRaw.Duration(), err)

	return err
}

func (cd *datafile) Read(ctx context.Context, shard *core.Shard, from, to uint32) (r io.Reader, err error) {
	if shard == nil {
		return nil, bloberr.ErrInvalidParam
	}

	//                from                to
	//                 |                  |
	// |------------|------------|------------|------------|
	// 0          block        block        block         size
	if to > shard.Size || to-from > shard.Size {
		return nil, bloberr.ErrInvalidParam
	}

	if !cd.qosAllow(ctx, qos.IOTypeRead) { // If there is too much io, it will discard some low-priority io
		return nil, bloberr.ErrOverload
	}
	defer cd.qosRelease(qos.IOTypeRead)

	// skip header
	pos := shard.Offset + core.GetShardHeaderSize()

	// new reader
	iosr := cd.qosReaderAt(ctx, cd.ef)

	// new buffer
	buffer := bytespool.Alloc(core.CrcBlockUnitSize)
	defer bytespool.Free(buffer) // nolint: staticcheck

	// decode crc
	decoder, err := crc32block.NewDecoderWithBlock(iosr, pos, int64(shard.Size), buffer, cd.conf.BlockBufferSize)
	if err != nil {
		return nil, err
	}

	r, err = decoder.Reader(int64(from), int64(to))
	if err != nil {
		return nil, err
	}

	return r, nil
}

func (cd *datafile) Delete(ctx context.Context, shard *core.Shard) (err error) {
	var ns core.Shard
	var discardSize int64

	if shard.Offset < _chunkHeaderSize {
		return bloberr.ErrShardInvalidOffset
	}

	// read shard header
	buf := bytespool.Alloc(core.HeaderSize)
	defer bytespool.Free(buf) // nolint: staticcheck
	_, err = cd.ef.ReadAt(buf, shard.Offset)
	if err != nil {
		return err
	}

	// verify
	err = ns.ParseHeader(buf)
	if err != nil {
		return err
	}
	if shard.Bid != ns.Bid || shard.Vuid != ns.Vuid || shard.Size != ns.Size {
		return ErrShardHeaderNotMatch
	}

	if shard.Offset%_pageSize != 0 {
		return ErrShardOffNotAlignment
	}

	// punch hole
	discardSize = core.Alignphysize(int64(shard.Size))
	discardSize = core.AlignSize(discardSize, _pageSize)
	err = cd.ef.Discard(shard.Offset, discardSize)

	return err
}

func (cd *datafile) Destroy(ctx context.Context) (err error) {
	log.Warnf("destroy chunk data: %s", cd.ef.Name())
	return os.Remove(cd.File)
}

func (cd *datafile) String() string {
	return fmt.Sprintf(`
-----------------------------
wOff:           %d
File:           %s
Ver:            %d
-----------------------------
`, cd.wOff, cd.ef.Name(), cd.header.version)
}

func (cd *datafile) Stat() (stat *core.StorageStat, err error) {
	fsize, physize, err := cd.spaceInfo()
	if err != nil {
		return nil, err
	}

	stat = &core.StorageStat{
		FileSize:   fsize,
		PhySize:    physize,
		ParentID:   cd.header.parentChunk,
		CreateTime: cd.header.createTime,
	}

	return stat, nil
}

func (cd *datafile) spaceInfo() (size int64, phySpace int64, err error) {
	stat, err := cd.ef.SysStat()
	if err != nil {
		log.Errorf("get ChunkData.f sysstat_t failed: %v", err)
		return 0, 0, err
	}
	size = stat.Size
	phySpace = stat.Blocks * sectorSize
	return
}

func (cd *datafile) qosReaderAt(ctx context.Context, reader io.ReaderAt) io.ReaderAt {
	ioType := bnapi.GetIoType(ctx)
	return cd.ioQos.ReaderAt(ctx, ioType, reader)
}

func (cd *datafile) qosWriterAt(ctx context.Context, writer io.WriterAt) io.WriterAt {
	ioType := bnapi.GetIoType(ctx)
	w := cd.ioQos.WriterAt(ctx, ioType, writer)
	return w
}

func (cd *datafile) qosWriter(ctx context.Context, writer io.Writer) io.Writer {
	ioType := bnapi.GetIoType(ctx)
	return cd.ioQos.Writer(ctx, ioType, writer)
}

func (cd *datafile) qosAllow(ctx context.Context, rwType qos.IOTypeRW) bool {
	q, ok := cd.ioQos.(*qos.IoQueueQos)
	if !ok {
		panic("wrong io qos type")
	}
	return q.TryAcquireIO(ctx, uint64(cd.chunk.VolumeUnitId()), rwType)
}

func (cd *datafile) qosRelease(rwType qos.IOTypeRW) {
	q, ok := cd.ioQos.(*qos.IoQueueQos)
	if !ok {
		return
	}
	q.ReleaseIO(uint64(cd.chunk.VolumeUnitId()), rwType)
}
