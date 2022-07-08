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

package wal

import (
	"bytes"
	"io"
	"os"
	"path"

	"github.com/cubefs/cubefs/blobstore/util/bytespool"
	pb "go.etcd.io/etcd/raft/v3/raftpb"
)

type logFile struct {
	dir     string
	name    logName
	f       *os.File
	r       *recordReader
	w       *recordWriter
	indexes logIndexSlice
}

func openLogFile(dir string, name logName, isLastOne bool) (*logFile, error) {
	p := path.Join(dir, name.String())
	f, err := os.OpenFile(p, os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}

	lf := &logFile{
		dir:  dir,
		name: name,
		f:    f,
		r:    newRecordReader(f),
	}

	if !isLastOne {
		if err = lf.loadIndex(); err != nil {
			return nil, err
		}
	} else {
		toffset, err := lf.recoverIndex()
		if err != nil {
			return nil, err
		}
		if err = lf.OpenWrite(); err != nil {
			return nil, err
		}
		if err := lf.w.Truncate(toffset); err != nil {
			return nil, err
		}
	}

	return lf, nil
}

func createLogFile(dir string, name logName) (*logFile, error) {
	p := path.Join(dir, name.String())
	f, err := os.OpenFile(p, os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}

	lf := &logFile{
		dir:  dir,
		name: name,
		f:    f,
		r:    newRecordReader(f),
	}

	if err := lf.OpenWrite(); err != nil {
		return nil, err
	}

	return lf, nil
}

func (lf *logFile) loadIndex() error {
	info, err := lf.f.Stat()
	if err != nil {
		return err
	}

	// read footer
	var ft footer
	if info.Size() < int64(recordSize(&ft)) {
		return NewLogError(lf.f.Name(), 0, "too small footer")
	}
	offset := info.Size() - int64(recordSize(&ft))
	rec, err := lf.r.ReadAt(offset)
	if err != nil {
		return err
	}
	defer func() {
		bytespool.Free(rec.data)
		rec.data = nil
	}()
	if rec.recType != FooterType {
		return NewLogError(lf.f.Name(), offset, "wrong footer record type")
	}
	if rec.dataLen != ft.Size()+1 {
		return NewLogError(lf.f.Name(), offset, "wrong footer size")
	}
	ft.Decode(rec.data[1:])
	if !bytes.Equal(ft.magic[:], magic) {
		return NewLogError(lf.f.Name(), offset, "wrong footer magic")
	}

	// read index data
	offset = int64(ft.indexOffset)
	reci, err := lf.r.ReadAt(offset)
	if err != nil {
		return err
	}
	defer func() {
		bytespool.Free(reci.data)
		reci.data = nil
	}()
	if reci.recType != IndexType {
		return NewLogError(lf.f.Name(), offset, "wrong index record type")
	}
	lf.indexes = decodeIndex(reci.data[1:])

	return nil
}

func (lf *logFile) recoverIndex() (truncateOffset int64, err error) {
	lf.indexes = lf.indexes[:0]

	var (
		rec    record
		offset int64
	)
	r := newRecordReader(lf.f)
	for {
		offset, rec, err = r.Read()
		if err != nil {
			break
		}
		if rec.recType != EntryType {
			bytespool.Free(rec.data)
			rec.data = nil
			break
		}
		ent := Entry{}
		if err = ent.Decode(rec.data[1:]); err != nil {
			err = NewLogError(lf.f.Name(), offset, err.Error())
			bytespool.Free(rec.data)
			rec.data = nil
			break
		}
		lf.indexes = lf.indexes.Append(uint32(offset), &ent.Entry)
		bytespool.Free(rec.data)
		rec.data = nil
	}
	if err == io.EOF || IsLogError(err) || err == io.ErrUnexpectedEOF {
		err = nil
	}
	return offset, err
}

func (lf *logFile) Name() logName {
	return lf.name
}

func (lf *logFile) Seq() uint64 {
	return lf.name.sequence
}

func (lf *logFile) Len() int {
	return lf.indexes.Len()
}

func (lf *logFile) FirstIndex() uint64 {
	return lf.indexes.First()
}

func (lf *logFile) LastIndex() uint64 {
	return lf.indexes.Last()
}

// Get get log entry
func (lf *logFile) Get(i uint64) (pb.Entry, error) {
	ent := Entry{}
	item, err := lf.indexes.Get(i)
	if err != nil {
		return ent.Entry, err
	}

	rec, err := lf.r.ReadAt(int64(item.offset))
	if err != nil {
		return pb.Entry{}, err
	}
	defer func() {
		bytespool.Free(rec.data)
		rec.data = nil
	}()

	if err = ent.Decode(rec.data[1:]); err != nil {
		return pb.Entry{}, err
	}

	return ent.Entry, nil
}

// Term get log's term
func (lf *logFile) Term(i uint64) (uint64, error) {
	item, err := lf.indexes.Get(i)
	if err != nil {
		return 0, err
	}
	return item.term, nil
}

func (lf *logFile) Truncate(index uint64) error {
	if lf.indexes.Len() == 0 {
		return nil
	}

	item, err := lf.indexes.Get(index)
	if err != nil {
		return err
	}

	offset := int64(item.offset)
	if err = lf.w.Truncate(offset); err != nil {
		return err
	}

	lf.indexes, err = lf.indexes.Truncate(index)
	return err
}

func (lf *logFile) Save(ent *pb.Entry) error {
	offset := lf.w.Offset()
	if err := lf.w.Write(EntryType, &Entry{*ent}); err != nil {
		return err
	}

	lf.indexes = lf.indexes.Append(uint32(offset), ent)

	return nil
}

func (lf *logFile) OpenWrite() error {
	if lf.w != nil {
		return nil
	}

	lf.w = newRecordWriter(lf.f)
	return nil
}

func (lf *logFile) WriteOffset() int64 {
	return lf.w.Offset()
}

func (lf *logFile) Flush() error {
	return lf.w.Flush()
}

// Sync flush write buffer and sync to disk
func (lf *logFile) Sync() error {
	return lf.w.Sync()
}

func (lf *logFile) FinishWrite() error {
	var err error

	recOffset := lf.w.Offset()
	if err = lf.w.Write(IndexType, lf.indexes); err != nil {
		return err
	}

	ft := &footer{
		indexOffset: uint64(recOffset),
	}
	if err = lf.w.Write(FooterType, ft); err != nil {
		return err
	}

	if err := lf.w.Close(); err != nil {
		return err
	}
	lf.w = nil
	return nil
}

func (lf *logFile) Close() error {
	if lf == nil {
		return nil
	}
	if lf.w != nil {
		if err := lf.w.Close(); err != nil {
			return err
		}
		lf.w = nil
	}

	return lf.f.Close()
}
