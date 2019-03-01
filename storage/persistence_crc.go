package storage

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
)

const (
	BlockCrcValueLen       = 4
	MarkExtentDeletePrefix = "MarkExtentDelete_"
	HasExtentDeletePrefix  = "HasExtentDelete_"
	BaseExtentIDPrefix     = "BaseExtentId_"
	StoreInodePrefix       = "StoreInode_"
	ExtentCrcPrefix        = "ExtentCrc_"
	BlockCrcPrefix         = "C"
)

type BlockCrc struct {
	blockNo uint16
	crc     uint32
	key     string
	value   []byte
}

func NewBlockCrc(extentID uint64, blockNo uint16, crc uint32) (bc *BlockCrc) {
	bc = new(BlockCrc)
	bc.blockNo = blockNo
	bc.crc = crc
	bc.value = make([]byte, BlockCrcValueLen)
	bc.key = fmt.Sprintf(BlockCrcPrefix+"%v_%v", extentID, blockNo)
	binary.BigEndian.PutUint32(bc.value[0:BlockCrcValueLen], bc.crc)

	return
}

type UpdateCrcFunc func(extentID uint64, blockNo uint16, crc uint32) (err error)
type ScanBlocksFunc func(extentID uint64) (bcs []*BlockCrc, err error)
type GetExtentCrcFunc func(extentID uint64) (crc uint32, err error)

func (s *ExtentStore) PersistenceBlockCrc(extentID uint64, blockNo uint16, blockCrc uint32) (err error) {
	bc := NewBlockCrc(extentID, blockNo, blockCrc)
	cmdMap := make(map[string][]byte, 0)
	cmdMap[bc.key] = bc.value
	data := make([]byte, 4)
	binary.BigEndian.PutUint32(data, 0)
	cmdMap[fmt.Sprintf(ExtentCrcPrefix+"%v", extentID)] = data
	err = s.crcStore.BatchPut(cmdMap, false)
	return
}

func (s *ExtentStore) PersistenceMarkDeleteExtent(extentID uint64) (err error) {
	key := fmt.Sprintf(MarkExtentDeletePrefix+"%v", extentID)
	data := make([]byte, 4)
	_, err = s.crcStore.Put(key, data, false)
	return
}

func (s *ExtentStore) PersistenceBaseExtentID(extentID uint64) (err error) {
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, extentID)
	_, err = s.crcStore.Put(BaseExtentIDPrefix, value, false)
	return
}

func (s *ExtentStore) GetPersistenceBaseExtentID() (extentID uint64, err error) {
	v, err := s.crcStore.Get(BaseExtentIDPrefix)
	if err != nil {
		return 0, err
	}
	if v == nil || len(v.([]byte)) != 8 {
		return 0, fmt.Errorf("empty value")
	}
	extentID = binary.BigEndian.Uint64(v.([]byte))
	return
}

func (s *ExtentStore) PersistenceHasDeleteExtent(extentID uint64) (err error) {
	key := fmt.Sprintf(HasExtentDeletePrefix+"%v", extentID)
	data := make([]byte, 4)
	_, err = s.crcStore.Put(key, data, false)
	if err != nil {
		return
	}
	s.DeleteBlocks(extentID)
	return
}

func (s *ExtentStore) IsMarkDeleteExtent(extentID uint64) bool {
	key := fmt.Sprintf(MarkExtentDeletePrefix+"%v", extentID)
	v, err := s.crcStore.Get(key)
	if err == nil && len(v.([]byte)) == 4 {
		return true
	}
	return false
}

func (s *ExtentStore) ScanBlocks(extentID uint64) (bcs []*BlockCrc, err error) {
	key := fmt.Sprintf(BlockCrcPrefix+"%v_", extentID)
	bcs = make([]*BlockCrc, 0)
	result, err := s.crcStore.SeekForPrefix([]byte(key))
	if err != nil {
		return
	}

	for key, value := range result {
		arr := strings.Split(key, "_")
		if len(arr) != 2 {
			continue
		}
		crc := binary.BigEndian.Uint32(value)
		blockNo, err := strconv.ParseUint(arr[1], 10, 64)
		if err != nil {
			err = nil
			continue
		}
		bcs = append(bcs, &BlockCrc{blockNo: uint16(blockNo), crc: crc})
	}
	return
}

func (s *ExtentStore) DeleteBlocks(extentID uint64) (bcs []*BlockCrc, err error) {
	key := fmt.Sprintf(BlockCrcPrefix+"%v_", extentID)
	bcs = make([]*BlockCrc, 0)
	result, err := s.crcStore.SeekForPrefix([]byte(key))
	if err != nil {
		return
	}

	for blockKey := range result {
		s.crcStore.Del(blockKey, false)
	}
	return
}

func (s *ExtentStore) ScanDeleteExtent(prefix string) (extents []uint64, err error) {
	extents = make([]uint64, 0)
	result, err := s.crcStore.SeekForPrefix([]byte(prefix))
	if err != nil {
		return
	}

	for key := range result {
		arr := strings.Split(key, prefix)
		if len(arr) != 2 {
			continue
		}
		extentID, err := strconv.ParseUint(arr[1], 10, 64)
		if err != nil {
			continue
		}
		extents = append(extents, extentID)
	}
	return
}

func (s *ExtentStore) PersistenceInode(inode uint64, extentID uint64) (err error) {
	key := fmt.Sprintf(StoreInodePrefix+"%v", extentID)
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, inode)
	_, err = s.crcStore.Put(key, data, false)
	return
}

func (s *ExtentStore) GetPersistenceInode(extentID uint64) (inode uint64, err error) {
	key := fmt.Sprintf(StoreInodePrefix+"%v", extentID)
	v, err := s.crcStore.Get(key)
	if err != nil {
		return 0, err
	}
	if v == nil || len(v.([]byte)) != 8 {
		return 0, fmt.Errorf("cannot get inode")
	}
	inode = binary.BigEndian.Uint64(v.([]byte))
	return
}

func (s *ExtentStore) PersistenceExtentCrc(extentID uint64, crc uint32) (err error) {
	key := fmt.Sprintf(ExtentCrcPrefix+"%v", extentID)
	data := make([]byte, 4)
	binary.BigEndian.PutUint32(data, crc)
	_, err = s.crcStore.Put(key, data, false)
	return
}

func (s *ExtentStore) GetPersistenceExtentCrc(extentID uint64) (crc uint32, err error) {
	key := fmt.Sprintf(ExtentCrcPrefix+"%v", extentID)
	v, err := s.crcStore.Get(key)
	if err != nil {
		return 0, err
	}
	if v == nil || len(v.([]byte)) != 4 {
		return 0, nil
	}
	crc = binary.BigEndian.Uint32(v.([]byte))
	return
}
