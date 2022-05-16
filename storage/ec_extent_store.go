package storage

import (
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/log"
	"hash/crc32"
	"strings"
	"syscall"
)

func (s *ExtentStore) TinyExtentHolesAndAvaliSize(extentID uint64, offset int64) (holes []*proto.TinyExtentHole, extentAvaliSize uint64, err error) {
	var (
		e          *Extent
		preAllSize uint64
		dataOffset int64
		holeOffset int64
	)
	if !IsTinyExtent(extentID) {
		return nil, 0, fmt.Errorf("unavali extent(%v)", extentID)
	}
	ei, exist := s.extentMapSlice.Load(extentID)
	if !exist {
		return nil, 0, ExtentNotFoundError
	}
	if e, err = s.ExtentWithHeader(ei); err != nil {
		return
	}
	defer func() {
		if err != nil {
			if strings.Contains(err.Error(), syscall.ENXIO.Error()) {
				log.LogInfof("TinyExtentHolesAndAvaliSize holes(%v) extentAvaliSize(%v)", holes, extentAvaliSize)
				err = nil
				return
			}
			log.LogErrorf("TinyExtentHolesAndAvaliSize err(%v)", err)
		}
	}()
	for {
		dataOffset, holeOffset, err = e.tinyExtentAvaliAndHoleOffset(offset)
		log.LogDebugf("tinyExtentAvaliAndHoleOffset dataOffset(%v) holeOffset(%v) offset(%v) err(%v)",
			dataOffset, holeOffset, offset, err)
		if err != nil {
			break
		}
		if dataOffset < holeOffset {
			extentAvaliSize += uint64(holeOffset - dataOffset)
			offset += holeOffset - dataOffset
		} else {
			preAllSize += uint64(dataOffset - holeOffset)
			hole := &proto.TinyExtentHole{
				Size:       uint64(dataOffset - holeOffset),
				Offset:     uint64(holeOffset),
				PreAllSize: preAllSize,
			}
			offset += dataOffset - holeOffset
			holes = append(holes, hole)
		}

	}

	return
}

func (s *ExtentStore) GetExtentCrc(extentId uint64) (crc uint32, err error) {
	var (
		offset            int64
		newOffset         int64
		newEnd            int64
		currNeedReplySize int64
		currReadSize      uint32
		firstRead         = true
		dataCrc           uint32
		needReadSize      uint64
		extent            *Extent
	)
	if IsTinyExtent(extentId) {
		_, needReadSize, err = s.TinyExtentHolesAndAvaliSize(extentId, 0)
	} else {
		extent, err = s.loadExtentFromDisk(extentId, false)
		needReadSize = uint64(extent.dataSize)
	}
	if err != nil {
		return
	}
	log.LogDebugf("GetExtentCrc extentId(%v) needReadSize(%v)", extentId, needReadSize)

	for {
		if needReadSize <= 0 {
			break
		}
		if IsTinyExtent(extentId) {
			newOffset, newEnd, err = s.TinyExtentAvaliOffset(extentId, offset)
			if err != nil {
				return
			}
			log.LogInfof("GetExtentCrc extentId(%v) needReadSize(%v) newOffset(%v) newEnd(%v) offset(%v)",
				extentId, needReadSize, newOffset, newEnd, offset)
			if newOffset > offset {
				replySize := newOffset - offset
				offset += replySize
				continue
			}
			currNeedReplySize = newEnd - newOffset
			currReadSize = uint32(util.Min(int(currNeedReplySize), 128*util.MB))
		} else {
			currReadSize = uint32(util.Min(int(needReadSize), 128*util.MB))
		}

		data := make([]byte, currReadSize)

		dataCrc, err = s.Read(extentId, offset, int64(currReadSize), data, false)
		if err != nil {
			return
		}
		needReadSize -= uint64(currReadSize)
		offset += int64(currReadSize)
		if firstRead {
			crc = dataCrc
			firstRead = false
		} else {
			crc = crc32.Update(crc, crc32.IEEETable, data)
		}
	}

	return
}
