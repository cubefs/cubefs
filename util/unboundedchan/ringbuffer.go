package unboundedchan

import "errors"

type V interface{}

type RingBuffer struct {
	buffer   []V
	initSize uint32
	size     uint32
	rIndex   uint32
	wIndex   uint32
}

var ErrBufferEmpty = errors.New("ringbuffer is empty")

func NewRingBuffer(initSize uint32) (buf *RingBuffer) {
	if initSize == 1 {
		initSize = 2
	}

	buf = &RingBuffer{
		buffer:   make([]V, initSize),
		initSize: initSize,
		size:     initSize,
	}
	return buf
}

func (buf *RingBuffer) IsEmpty() bool {
	return buf.rIndex == buf.wIndex
}

func (buf *RingBuffer) Read() (V, error) {
	if buf.IsEmpty() {
		return nil, ErrBufferEmpty
	}

	val := buf.buffer[buf.rIndex]
	buf.rIndex++
	if buf.rIndex == buf.size {
		buf.rIndex = 0
	}

	return val, nil
}

func (buf *RingBuffer) Pop() V {
	val, err := buf.Read()
	if err != nil {
		panic(err.Error())
	}
	return val
}

func (buf *RingBuffer) Peek() V {
	if buf.IsEmpty() {
		panic(ErrBufferEmpty.Error())
	}

	val := buf.buffer[buf.rIndex]
	return val
}

func (buf *RingBuffer) scaleUp() {
	var size uint32
	if buf.size < 1024 {
		size = buf.size * 2
	} else {
		size = buf.size + buf.size/4
	}

	buffer := make([]V, size)
	copy(buffer[0:], buf.buffer[buf.rIndex:])
	copy(buffer[buf.size-buf.rIndex:], buf.buffer[0:buf.rIndex])

	buf.rIndex = 0
	buf.wIndex = buf.size
	buf.size = size
	buf.buffer = buffer
}

func (buf *RingBuffer) Write(val V) {
	buf.buffer[buf.wIndex] = val
	buf.wIndex++

	if buf.wIndex == buf.size {
		buf.wIndex = 0
	}

	if buf.wIndex == buf.rIndex {
		buf.scaleUp()
	}
}

func (buf *RingBuffer) Capacity() uint32 {
	return buf.size
}

func (buf *RingBuffer) Len() uint32 {
	if buf.IsEmpty() {
		return 0
	}

	if buf.wIndex > buf.rIndex {
		return buf.wIndex - buf.rIndex
	} else {
		return buf.wIndex + buf.size - buf.rIndex
	}

}

func (buf *RingBuffer) Reset() {
	buf.size = buf.initSize
	buf.rIndex = 0
	buf.wIndex = 0
	buf.buffer = make([]V, buf.initSize)
}
