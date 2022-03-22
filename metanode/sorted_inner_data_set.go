package metanode

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/log"
	se "github.com/chubaofs/chubaofs/util/sortedextent"
	"hash/crc32"
	"math"
	"sync"
)

const (
	SortedInnerDataSetBaseLen = 4 //crc value
)

type SortedInnerDataSet struct {
	sync.RWMutex
	innerDataSet []*proto.InnerDataSt
	crc          uint32
}

func NewSortedInnerDataSet() *SortedInnerDataSet {
	return &SortedInnerDataSet{
		innerDataSet: make([]*proto.InnerDataSt, 0),
	}
}

func (si *SortedInnerDataSet) String() string {
	si.RLock()
	defer si.RUnlock()
	data, err := json.Marshal(si.innerDataSet)
	if err != nil {
		return ""
	}
	return string(data)
}

func (si *SortedInnerDataSet) Len() int {
	si.RLock()
	defer si.RUnlock()
	return len(si.innerDataSet)
}

func (si *SortedInnerDataSet) Range(fn func(st *proto.InnerDataSt) bool) {
	si.RLock()
	defer si.RUnlock()
	for _, innerData := range si.innerDataSet {
		if !fn(innerData) {
			break
		}
	}
}

func (si *SortedInnerDataSet) Get(index int) (innData *proto.InnerDataSt) {
	if index >= len(si.innerDataSet) {
		return nil
	}
	return si.innerDataSet[index]
}

func (si *SortedInnerDataSet) Clone() *SortedInnerDataSet {
	newSi := NewSortedInnerDataSet()

	si.RLock()
	defer si.RUnlock()

	newSi.innerDataSet = si.doCopyInnerDataArr()
	return newSi
}

func (si *SortedInnerDataSet) GenCrc() (err error) {
	si.RLock()
	defer si.RUnlock()
	crc := crc32.NewIEEE()
	for _, innData := range si.innerDataSet {
		_, err = crc.Write(innData.Data)
		if err != nil {
			return
		}
	}
	si.crc = crc.Sum32()
	return
}

func (si *SortedInnerDataSet) MarshalBinary() ([]byte, error) {
	var (
		totalLen, offset int
		data             []byte
	)

	si.RLock()
	defer si.RUnlock()

	totalLen = SortedInnerDataSetBaseLen
	for _, innData := range si.innerDataSet {
		totalLen += 8 /*for inner data value len*/
		totalLen += innData.ValueLen()
	}

	data = make([]byte, totalLen)
	for _, innData := range si.innerDataSet {
		if len(data[offset:]) < 8 {
			return nil, fmt.Errorf("error data cap")
		}
		valueLen := innData.ValueLen()
		binary.BigEndian.PutUint64(data[offset:offset+8], uint64(valueLen))
		offset += 8
		if len(data[offset:]) < innData.ValueLen() {
			return nil, fmt.Errorf("error data cap")
		}
		innData.EncodeBinary(data[offset:offset+innData.ValueLen()])
		offset += innData.ValueLen()
	}

	if len(data[offset:]) < SortedInnerDataSetBaseLen {
		return nil, fmt.Errorf("error data capacity")
	}
	binary.BigEndian.PutUint32(data[offset:], si.crc)
	return data, nil
}

func (si *SortedInnerDataSet) UnmarshalBinary(count int, data []byte) (err error) {
	var (
		innData *proto.InnerDataSt
		start   int
	)

	for start = 0; start < len(data) && count > 0;{
		if len(data[start:]) < proto.BaseInnerDataLen {
			return fmt.Errorf("inner data buff err, need at least 12, but buff len:%d", len(data[start:]))
		}
		start += 8
		innData = new(proto.InnerDataSt)
		if err = innData.UnmarshalBinary(data[start:]); err != nil {
			return
		}
		si.Insert(innData)
		start += innData.ValueLen()
		count--
	}

	if len(data[start:]) < SortedInnerDataSetBaseLen {
		return fmt.Errorf("inner data buff err, need at least 4, but buff len:%d", len(data[start:]))
	}

	crcCur := binary.BigEndian.Uint32(data[start:])
	_ = si.GenCrc()
	if si.crc != crcCur {
		return fmt.Errorf("crc error, unmarshal from bytes:%v, actual calculate:%v", crcCur, si.crc)
	}
	return
}

func (si *SortedInnerDataSet) BinaryDataLen() int {
	si.RLock()
	defer si.RUnlock()

	totalLen := SortedInnerDataSetBaseLen
	for _, innData := range si.innerDataSet {
		totalLen += 8 /*for inner data value len*/
		totalLen += innData.ValueLen()
	}
	return totalLen
}

func (si *SortedInnerDataSet) EncodeBinary(data []byte) (err error) {
	if len(data) < SortedInnerDataSetBaseLen {
		return fmt.Errorf("error data cap")
	}
	offset := 0
	for _, innData := range si.innerDataSet {
		if len(data[offset:]) < 8 {
			return fmt.Errorf("error data cap")
		}
		valueLen := innData.ValueLen()
		binary.BigEndian.PutUint64(data[offset:offset+8], uint64(valueLen))
		offset += 8
		if len(data[offset:]) < valueLen {
			return fmt.Errorf("error data cap")
		}
		innData.EncodeBinary(data[offset:offset+valueLen])
		offset += valueLen
	}

	if len(data[offset:]) < SortedInnerDataSetBaseLen {
		return fmt.Errorf("error data capacity")
	}
	binary.BigEndian.PutUint32(data[offset:], si.crc)
	return
}

func (si *SortedInnerDataSet) Size() uint64 {
	si.RLock()
	defer si.RUnlock()
	last := len(si.innerDataSet) - 1
	if last < 0 {
		return uint64(0)
	}
	return si.innerDataSet[last].FileOffset + uint64(si.innerDataSet[last].Size)
}

func (si *SortedInnerDataSet) LastInnerData() *proto.InnerDataSt {
	si.RLock()
	defer si.RUnlock()
	if len(si.innerDataSet) <= 0 {
		return nil
	}
	return si.innerDataSet[len(si.innerDataSet) - 1]
}

func (si *SortedInnerDataSet) doCopyInnerDataArr() []*proto.InnerDataSt {
	innerDataArr := make([]*proto.InnerDataSt, len(si.innerDataSet))
	copy(innerDataArr, si.innerDataSet)
	return innerDataArr
}

//just for test
func (si *SortedInnerDataSet) printInnerData() {
	si.RLock()
	defer si.RUnlock()
	for index, innerData := range si.innerDataSet {
		fmt.Printf("index:%v, fileOffset:%v, size:%v, data:%s\n", index, innerData.FileOffset, innerData.Size, string(innerData.Data))
	}
}

func (si *SortedInnerDataSet) Merge() {
	var (
		index, nextIndex        int
		curInnData, nextInnData *proto.InnerDataSt
	)
	si.Lock()
	defer si.Unlock()
	for index < len(si.innerDataSet) - 1 {
		nextIndex = index + 1
		curInnData = si.innerDataSet[index]
		nextInnData = si.innerDataSet[nextIndex]

		if curInnData.FileOffset + uint64(curInnData.Size) == nextInnData.FileOffset {
			var upperInnDataArr  []*proto.InnerDataSt
			if nextIndex < len(si.innerDataSet) - 1 {
				upperInnDataArr = make([]*proto.InnerDataSt, len(si.innerDataSet) - 1 - nextIndex)
				copy(upperInnDataArr, si.innerDataSet[nextIndex+1:])
			}

			curInnData.Size = curInnData.Size + nextInnData.Size
			curInnData.Data = append(curInnData.Data, nextInnData.Data...)

			si.innerDataSet = si.innerDataSet[:index]
			si.innerDataSet = append(si.innerDataSet, curInnData)
			if len(upperInnDataArr) != 0 {
				si.innerDataSet = append(si.innerDataSet, upperInnDataArr...)
			}
			continue
		}
		index++
	}
}

func (si *SortedInnerDataSet) Insert(newInnerData *proto.InnerDataSt) {
	var (
		upperInnerDataArr           []*proto.InnerDataSt
		start, end                  int
		endOffset                   uint64
		preInnerData, nextInnerData *proto.InnerDataSt
	)

	si.Lock()
	defer si.Unlock()

	endOffset = newInnerData.FileOffset + uint64(newInnerData.Size)
	start = si.getStartInsertIndex(newInnerData.FileOffset)
	end = si.getEndInsertIndex(endOffset)

	//truncate start insert index inner data
	if start != -1 {
		cur := si.innerDataSet[start]
		preInnerData = &proto.InnerDataSt{
			FileOffset: cur.FileOffset,
			Size:       cur.Size,
			Data:       cur.Data,
		}
		if newInnerData.FileOffset < preInnerData.FileOffset+uint64(preInnerData.Size) {
			preInnerData.Size = uint32(newInnerData.FileOffset - preInnerData.FileOffset)
			preInnerData.Data = cur.Data[:preInnerData.Size]
		}
	}

	if end != -1 {
		cur := si.innerDataSet[end]
		if endOffset < cur.FileOffset+uint64(cur.Size) {
			nextInnerDataSize := uint32(cur.FileOffset + uint64(cur.Size) - (newInnerData.FileOffset + uint64(newInnerData.Size)))
			nextInnerData = &proto.InnerDataSt{
				FileOffset: newInnerData.FileOffset + uint64(newInnerData.Size),
				Size:       nextInnerDataSize,
				Data:       cur.Data[cur.Size-nextInnerDataSize:],
			}
		}
	}

	if len(si.innerDataSet) != 0 && end < len(si.innerDataSet) - 1 {
		upperInnerDataArr = make([]*proto.InnerDataSt, len(si.innerDataSet) - 1 - end)
		copy(upperInnerDataArr, si.innerDataSet[end+1:])
	}

	if start <= 0 {
		si.innerDataSet = si.innerDataSet[:0]
	} else {
		si.innerDataSet = si.innerDataSet[:start]
	}

	if preInnerData != nil && preInnerData.Size != 0 {
		si.innerDataSet = append(si.innerDataSet, preInnerData)
	}

	si.innerDataSet = append(si.innerDataSet, newInnerData)

	if nextInnerData != nil && nextInnerData.Size != 0 {
		si.innerDataSet = append(si.innerDataSet, nextInnerData)
	}
	if upperInnerDataArr != nil {
		si.innerDataSet = append(si.innerDataSet, upperInnerDataArr...)
	}
	return
}

//todo:
func (si *SortedInnerDataSet) Append(innData *proto.InnerDataSt) {
	si.Lock()
	defer si.Unlock()

	endOffset := innData.FileOffset + uint64(innData.Size)

	if len(si.innerDataSet) <= 0 {
		si.innerDataSet = append(si.innerDataSet, innData)
		return
	}
	lastKey := si.innerDataSet[len(si.innerDataSet)-1]
	if lastKey.FileOffset+uint64(lastKey.Size) <= innData.FileOffset {
		si.innerDataSet = append(si.innerDataSet, innData)
		return
	}
	firstKey := si.innerDataSet[0]
	if firstKey.FileOffset >= endOffset {
		innDataArr := si.doCopyInnerDataArr()
		si.innerDataSet = si.innerDataSet[:0]
		si.innerDataSet = append(si.innerDataSet, innData)
		si.innerDataSet = append(si.innerDataSet, innDataArr...)
		return
	}

	var startIndex, endIndex int
	//todo:
	invalidCnt := 0
	for idx, existInnerData := range si.innerDataSet {
		if innData.FileOffset > existInnerData.FileOffset {
			startIndex = idx + 1
			continue
		}
		if endOffset >= existInnerData.FileOffset+uint64(existInnerData.Size) {
			invalidCnt++
			continue
		}
		break
	}

	//todo:fix start and end inner data info
	endIndex = startIndex + invalidCnt
	upperInnerDataArr := make([]*proto.InnerDataSt, len(si.innerDataSet)-endIndex)
	copy(upperInnerDataArr, si.innerDataSet[endIndex:])
	si.innerDataSet = si.innerDataSet[:startIndex]
	si.innerDataSet = append(si.innerDataSet, innData)
	si.innerDataSet = append(si.innerDataSet, upperInnerDataArr...)
}

func (si *SortedInnerDataSet) getStartInsertIndex(startOffset uint64) (startIndex int) {
	startIndex = -1
	for index, innerData := range si.innerDataSet {
		if innerData.FileOffset >= startOffset {
			break
		}
		startIndex = index
	}
	return
}

func (si *SortedInnerDataSet) getEndInsertIndex(endOffset uint64) (endIndex int) {
	endIndex = -1
	for index, innerData := range si.innerDataSet {
		if innerData.FileOffset >= endOffset {
			break
		}
		endIndex = index
	}
	return
}

func (si *SortedInnerDataSet) ConvertInnerDataArrToExtentKeys() (eks []proto.ExtentKey) {
	si.RLock()
	defer si.RUnlock()
	eks = make([]proto.ExtentKey, 0, len(si.innerDataSet))
	for _, innerData := range si.innerDataSet {
		ek := proto.ExtentKey{
			FileOffset:   innerData.FileOffset,
			PartitionId:  math.MaxUint64,
			ExtentId:     math.MaxUint64,
			ExtentOffset: innerData.FileOffset,
			Size:         innerData.Size,
			CRC:          0,
			StoreType:    proto.InnerData,
		}
		eks = append(eks, ek)
	}
	return
}

func (si *SortedInnerDataSet) ReshuffleInnerDataSet(se *se.SortedExtents) {
	si.Lock()
	defer si.Unlock()
	if len(si.innerDataSet) == 0 {
		return
	}

	lastInnerData := si.innerDataSet[len(si.innerDataSet) - 1]
	endOffset := lastInnerData.FileOffset + uint64(lastInnerData.Size)

	innerDataTypeExtents := make([]proto.ExtentKey, 0)
	se.Range(func(ek proto.ExtentKey) bool {
		if ek.StoreType == proto.InnerData || ek.PartitionId == math.MaxUint64 {
			innerDataTypeExtents = append(innerDataTypeExtents, ek)
			return true
		}
		if ek.FileOffset >= endOffset {
			return false
		}
		return true
	})

	after := make([]*proto.InnerDataSt, 0)
	for _, ek := range innerDataTypeExtents {
		after = append(after, si.copyAlreadyExistInnerDataSet(ek.FileOffset, ek.Size)...)
	}
	si.innerDataSet = after
}

func (si *SortedInnerDataSet) copyAlreadyExistInnerDataSet(fileOffset uint64, size uint32) (existInnDataArr []*proto.InnerDataSt) {
	existInnDataArr = make([]*proto.InnerDataSt, 0)
	var offsetStart, offsetEnd uint64
	for _, innData := range si.innerDataSet {
		if fileOffset >= innData.FileOffset + uint64(innData.Size) {
			continue
		}

		if fileOffset + uint64(size) <= innData.FileOffset {
			break
		}

		offsetStart = innData.FileOffset
		if fileOffset > innData.FileOffset {
			offsetStart = fileOffset
		}

		offsetEnd = fileOffset + uint64(size)
		if offsetEnd > innData.FileOffset + uint64(innData.Size) {
			offsetEnd = innData.FileOffset + uint64(innData.Size)
		}

		existInnDataArr = append(existInnDataArr, &proto.InnerDataSt{
			FileOffset: offsetStart,
			Size:       uint32(offsetEnd - offsetStart),
			Data:       innData.Data[offsetStart - innData.FileOffset : offsetEnd - innData.FileOffset],
		})
	}

	//check
	totalSize := uint32(0)
	for _, existInnData := range existInnDataArr {
		totalSize += existInnData.Size
	}
	if totalSize != size {
		log.LogErrorf("inner data size mismatch, expect:%v, actual:%v", size, totalSize)
	}
	return
}

func (si *SortedInnerDataSet) ReadInnerData(fileOffset uint64, size uint32) (data []byte, err error) {
	si.RLock()
	defer si.RUnlock()
	endOffset := fileOffset + uint64(size)
	for _, innerData := range si.innerDataSet {

		if fileOffset >= innerData.FileOffset + uint64(innerData.Size) {
			continue
		}

		if endOffset > innerData.FileOffset + uint64(innerData.Size) {
			err = fmt.Errorf("read end file offset out of bound")
			return
		}

		if fileOffset < innerData.FileOffset {
			err = fmt.Errorf("read start file offset out of bound")
			return
		}

		data = make([]byte, size)
		copy(data, innerData.Data[fileOffset-innerData.FileOffset : endOffset-innerData.FileOffset])
		return
	}
	err = fmt.Errorf("inner data not exist, read out of bound")
	return
}