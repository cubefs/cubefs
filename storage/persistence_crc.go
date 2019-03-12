package storage

import (
	"encoding/binary"
	"github.com/chubaofs/cfs/util"
	"syscall"
)

type BlockCrc struct {
	BlockNo int
	Crc     uint32
}
type BlockCrcArr []*BlockCrc

func (arr BlockCrcArr) Len() int           { return len(arr) }
func (arr BlockCrcArr) Less(i, j int) bool { return arr[i].BlockNo < arr[j].BlockNo }
func (arr BlockCrcArr) Swap(i, j int)      { arr[i], arr[j] = arr[j], arr[i] }

type UpdateCrcFunc func(e *Extent, blockNo int, crc uint32) (err error)
type GetExtentCrcFunc func(extentID uint64) (crc uint32, err error)

func (s *ExtentStore) PersistenceBlockCrc(e *Extent, blockNo int, blockCrc uint32) (err error) {
	startIdx := blockNo * util.PerBlockCrcSize
	endIdx := startIdx + util.PerBlockCrcSize
	binary.BigEndian.PutUint32(e.header[startIdx:endIdx], blockCrc)
	verifyStart := startIdx + int(util.BlockHeaderSize*e.extentID)
	if _, err = s.verifyCrcFp.WriteAt(e.header[startIdx:endIdx], int64(verifyStart)); err != nil {
		return
	}

	return
}

func (s *ExtentStore) DeleteBlockCrc(extentID uint64) (err error) {
	err = syscall.Fallocate(int(s.verifyCrcFp.Fd()), FallocFLPunchHole|FallocFLKeepSize,
		int64(util.BlockHeaderSize*extentID), util.BlockHeaderSize)

	return
}

func (s *ExtentStore) PersistenceBaseExtentID(extentID uint64) (err error) {
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, extentID)
	_, err = s.metadataFp.WriteAt(value, 0)
	return
}

func (s *ExtentStore) GetPersistenceBaseExtentID() (extentID uint64, err error) {
	data := make([]byte, 8)
	_, err = s.metadataFp.ReadAt(data, 0)
	if err != nil {
		return
	}
	extentID = binary.BigEndian.Uint64(data)
	return
}

func (s *ExtentStore) PersistenceHasDeleteExtent(extentID uint64) (err error) {
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, extentID)
	if _, err = s.normalExtentDeleteFp.Write(data); err != nil {
		return
	}
	return
}
