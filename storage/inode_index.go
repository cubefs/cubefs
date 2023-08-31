package storage

import (
	"encoding/binary"
	"io"
	"os"
)

const (
	inodeIndexRecordLength = 8
)

var (
	inodeIndexEmptyRecordBuf = make([]byte, inodeIndexRecordLength)
)

type inodeIndex struct {
	f *os.File
}

func (i *inodeIndex) Get(extentID uint64) (inode uint64, err error) {
	var buf = make([]byte, inodeIndexRecordLength)
	_, err = i.f.ReadAt(buf[0:inodeIndexRecordLength], i.locate(extentID))
	switch {
	case err == io.EOF:
		err = nil
		return
	case err != nil:
		return
	default:
	}
	inode = binary.BigEndian.Uint64(buf[0:inodeIndexRecordLength])
	return
}

func (i *inodeIndex) Put(extentID, inode uint64) (err error) {
	var buf = make([]byte, inodeIndexRecordLength)
	binary.BigEndian.PutUint64(buf[0:inodeIndexRecordLength], inode)
	_, err = i.f.WriteAt(buf[0:inodeIndexRecordLength], i.locate(extentID))
	return
}

func (i *inodeIndex) Del(extentID uint64) (err error) {
	_, err = i.f.WriteAt(inodeIndexEmptyRecordBuf[0:inodeIndexRecordLength], i.locate(extentID))
	return
}

func (i *inodeIndex) Flush() (err error) {
	err = i.f.Sync()
	return
}

func (i *inodeIndex) locate(extentID uint64) (offset int64) {
	return int64(extentID * inodeIndexRecordLength)
}

func (i *inodeIndex) Close() (err error) {
	if err = i.f.Sync(); err != nil {
		return
	}
	err = i.f.Close()
	return
}

func openInodeIndex(path string) (index *inodeIndex, err error) {
	var f *os.File
	if f, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666); err != nil {
		return
	}
	index = &inodeIndex{
		f: f,
	}
	return
}
