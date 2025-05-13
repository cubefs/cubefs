package buf

import (
	"bytes"
	"encoding/binary"
	"io"
)

type ByteBufExt struct {
	bytes.Buffer
	bs []byte
}

func NewByteBufEx(size int) *ByteBufExt {
	return &ByteBufExt{
		Buffer: *bytes.NewBuffer(make([]byte, 0, size)),
		bs:     make([]byte, 8),
	}
}

func (b *ByteBufExt) PutUint32(val uint32) error {
	binary.BigEndian.PutUint32(b.bs, val)
	_, err := b.Write(b.bs[:4])
	return err
}

func (b *ByteBufExt) PutUint64(val uint64) error {
	binary.BigEndian.PutUint64(b.bs, val)
	_, err := b.Write(b.bs[:8])
	return err
}

type ReadByteBuff struct {
	data []byte
	off  int
}

func NewReadByteBuf() *ReadByteBuff {
	return &ReadByteBuff{
		off: 0,
	}
}

func (b *ReadByteBuff) Len() int {
	return len(b.data) - b.off
}

func (b *ReadByteBuff) SetData(raw []byte) {
	b.data = raw
	b.off = 0
}

func (b *ReadByteBuff) Reset() {
	b.data = nil
	b.off = 0
}

func (b *ReadByteBuff) ReadUint64() (val uint64, err error) {
	if err = b.checkValid(8); err != nil {
		return 0, err
	}

	val = binary.BigEndian.Uint64(b.data[b.off:])
	b.off += 8
	return
}

func (b *ReadByteBuff) ReadInt64() (val int64, err error) {
	if err = b.checkValid(8); err != nil {
		return 0, err
	}

	val1 := binary.BigEndian.Uint64(b.data[b.off:])

	b.off += 8
	return int64(val1), nil
}

func (b *ReadByteBuff) ReadUint32() (val uint32, err error) {
	if err = b.checkValid(4); err != nil {
		return 0, err
	}

	val = binary.BigEndian.Uint32(b.data[b.off:])
	b.off += 4
	return
}

func (b *ReadByteBuff) Next(size int) (data []byte, err error) {
	if err = b.checkValid(size); err != nil {
		return nil, err
	}

	data = b.data[b.off : b.off+size]
	b.off += size
	return data, nil
}

func (b *ReadByteBuff) Read(data []byte) (size int, err error) {
	if err = b.checkValid(len(data)); err != nil {
		return 0, err
	}

	n := copy(data, b.data[b.off:])
	b.off += n
	return len(data), nil
}

func (b *ReadByteBuff) Bytes() []byte {
	return b.data[b.off:]
}

func (b *ReadByteBuff) checkValid(size int) error {
	if b.data == nil {
		return io.EOF
	}

	if b.off+size > len(b.data) {
		return io.EOF
	}

	return nil
}
