package bitset

import "fmt"

// ByteSliceBitSet is a implementation of BitSet interface based on byte slice.
type ByteSliceBitSet struct {
	bytes     []byte
	wordInUse int
	cap       int
}

func NewByteSliceBitSet() *ByteSliceBitSet {
	return &ByteSliceBitSet{}
}

func (bs *ByteSliceBitSet) String() string {
	return fmt.Sprintf("byteSliceBitSet{%8b}", bs.bytes)
}

// Clear used for set the bit specified by the index to false.
func (bs *ByteSliceBitSet) Clear(index int) {
	if index < 0 {
		return
	}
	// Check capacity
	if !bs.checkCapacity(index) {
		return
	}
	bs.checkAndIncreaseCapacity(index)
	// Locate byte and bit
	byteIndex, bitIndex := bs.locateBit(index)
	// Validate word is use
	if bs.bytes[byteIndex]&byte(1<<byte(bitIndex)) != 0 {
		// Decrease word in use counter
		bs.wordInUse -= 1
	}
	// Set value
	bs.bytes[byteIndex] = bs.bytes[byteIndex] & ^(1 << byte(bitIndex))
}

// Set used for set the bit at the specified index to true.
func (bs *ByteSliceBitSet) Set(index int) {
	if index < 0 {
		return
	}
	// Check capacity
	bs.checkAndIncreaseCapacity(index)
	// Locate byte and bit
	byteIndex, bitIndex := bs.locateBit(index)
	// Set value
	bs.bytes[byteIndex] = bs.bytes[byteIndex] | (1 << byte(bitIndex))
	// Increase word in use counter
	bs.wordInUse += 1
}

// Get returns the value of the bit with the specified index.
// The value is true if the bit with the index is currently set in this BitSet;
// otherwise, the result is false.
func (bs *ByteSliceBitSet) Get(index int) bool {
	if index < 0 {
		return false
	}
	// Check capacity
	if !bs.checkCapacity(index) {
		return false
	}
	// Local byte and bit
	byteIndex, bitIndex := bs.locateBit(index)
	// Get value
	mask := byte(1 << byte(bitIndex))
	return (bs.bytes[byteIndex] & mask) != 0
}

// IsEmpty returns true if this BitSet contains no bits that are set to true.
func (bs *ByteSliceBitSet) IsEmpty() bool {
	return bs.wordInUse == 0
}

// Reset clean all bit.
func (bs *ByteSliceBitSet) Reset() {
	bs.wordInUse = 0
	bs.bytes = []byte{}
}

func (bs *ByteSliceBitSet) checkAndIncreaseCapacity(index int) {

	if index < 0 {
		return
	}

	if !bs.checkCapacity(index) {
		var newCapacity int
		if (index+1)%8 == 0 {
			newCapacity = (index + 1) / 8
		} else {
			newCapacity = (index+1)/8 + 1
		}
		newBytes := make([]byte, newCapacity)
		copy(newBytes, bs.bytes)
		bs.bytes = newBytes
		bs.cap = newCapacity*8-1
	}
}

func (bs *ByteSliceBitSet) checkCapacity(index int) bool {

	return !(cap(bs.bytes)*8-1 < index)
}

func (bs *ByteSliceBitSet) locateBit(index int) (byteIndex, bitIndex int) {

	if (index+1)%8 == 0 {
		byteIndex = (index+1)/8 - 1
		bitIndex = 7
	} else {
		byteIndex = (index + 1) / 8
		bitIndex = (index+1)%8 - 1
	}

	return
}

func (bs *ByteSliceBitSet) Xor(bitSet *ByteSliceBitSet) (r *ByteSliceBitSet){
	maxCap := bs.cap
	if bitSet.cap > maxCap {
		maxCap = bitSet.cap
	}
	bs.checkAndIncreaseCapacity(maxCap)
	bitSet.checkAndIncreaseCapacity(maxCap)
	rBitSet := make([]byte, 0, len(bs.bytes))
	for index, bit := range bs.bytes {
		rBitSet = append(rBitSet, bit^bitSet.bytes[index])
	}
	return &ByteSliceBitSet{bytes: rBitSet, cap: maxCap}
}

func (bs *ByteSliceBitSet) And(bitSet *ByteSliceBitSet) (r *ByteSliceBitSet){
	maxCap := bs.cap
	if bitSet.cap > maxCap {
		maxCap = bitSet.cap
	}
	bs.checkAndIncreaseCapacity(maxCap)
	bitSet.checkAndIncreaseCapacity(maxCap)
	rBitSet := make([]byte, 0, len(bs.bytes))
	for index, bit := range bs.bytes {
		rBitSet = append(rBitSet, bit&bitSet.bytes[index])
	}
	return &ByteSliceBitSet{bytes: rBitSet, cap: maxCap}
}

func (bs *ByteSliceBitSet) IsNil() bool {
	for _, bit := range bs.bytes {
		if bit != 0 {
			return false
		}
	}
	return true
}

func (bs *ByteSliceBitSet) Cap() int {
	return bs.cap
}

func (bs *ByteSliceBitSet) MaxNum() int {
	return len(bs.bytes) * 8
}
