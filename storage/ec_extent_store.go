package storage

import (
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/log"
	"strings"
	"syscall"
)

func (s *ExtentStore)TinyExtentHolesAndAvaliSize(extentID uint64, offset int64) (holes []*proto.TinyExtentHole, extentAvaliSize uint64, err error) {
	var (
		e *Extent
		preAllSize uint64
	)
	if !IsTinyExtent(extentID) {
		return nil, 0, fmt.Errorf("unavali extent(%v)", extentID)
	}
	ei, exist := s.extentMapSlice.Load(extentID)
	if !exist {
		return nil, 0, ExtentNotFoundError
	}
	if e, err = s.extentWithHeader(ei); err != nil {
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
		dataOffset, holeOffset, err := e.tinyExtentAvaliAndHoleOffset(offset)
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
				Size: uint64(dataOffset - holeOffset),
				Offset: uint64(holeOffset),
				PreAllSize: preAllSize,
			}
			offset += dataOffset - holeOffset
			holes = append(holes, hole)
		}
	}

	return
}