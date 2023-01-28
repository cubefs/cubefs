package bitmap

import (
	"encoding/binary"
	"fmt"
	"math"
)

const bitPerU64                 = 64

type U64BitMap []uint64

func findU64FreeBit(bitValue uint64, start int) int{
	for i := start; i < bitPerU64; i++ {
		if bitValue & (uint64(1) << i) == 0 {
			return i
		}
	}
	return -1
}

func (bitMap U64BitMap) GetU64BitInfo(index int) string{
	buff := make([]byte, 8)
	binary.BigEndian.PutUint64(buff, bitMap[index])
	return fmt.Sprintf("value:%d bits:, %08b %08b %08b %08b %08b %08b %08b %08b", bitMap[index], buff[0], buff[1], buff[2], buff[3], buff[4], buff[5], buff[6], buff[7])
}

func (bitMap U64BitMap) GetFirstFreeBit(start int, loopAllocate bool) (id int, err error) {
	lastIndex  := (start / bitPerU64) % len(bitMap)
	freeIndex := lastIndex
	freeBitIndex := findU64FreeBit(bitMap[freeIndex], start % bitPerU64 + 1)
	if freeBitIndex != -1 {
		goto CalId
	}

	for curIndex := lastIndex + 1; curIndex < len(bitMap); curIndex++ {
		if bitMap[curIndex] == math.MaxUint64 {
			continue
		}
		freeIndex = curIndex
		freeBitIndex = findU64FreeBit(bitMap[freeIndex], 0)
		if freeBitIndex != -1 {
			goto CalId
		}
	}

	if lastIndex < len(bitMap) {
		lastIndex += 1
	}

	for curIndex := 0; curIndex < lastIndex && loopAllocate; curIndex++ {
		if bitMap[curIndex] == math.MaxUint64 {
			continue
		}
		freeIndex = curIndex
		freeBitIndex = findU64FreeBit(bitMap[freeIndex], 0)
		if freeBitIndex != -1 {
			goto CalId
		}
	}

	return -1, fmt.Errorf("no free bit")

CalId:
	id = freeIndex * bitPerU64 + freeBitIndex
	return
}

func (bitMap U64BitMap) IsBitFree(id int) bool {
	index   := id / bitPerU64
	bitIndex := id % bitPerU64
	if bitMap[index] & (uint64(1) << bitIndex) != 0 {
		return false
	}
	return true
}

func (bitMap U64BitMap) SetBit(id int) {
	index   := id / bitPerU64
	bitIndex := id % bitPerU64
	bitMap[index] |= uint64(1) << bitIndex
}

func (bitMap U64BitMap) ClearBit(id int) {
	index   := id / bitPerU64
	bitIndex := id % bitPerU64
	bitMap[index] &= (uint64(1) << bitIndex) ^ math.MaxUint64
}
