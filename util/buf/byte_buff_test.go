package buf

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestByteBufEx(t *testing.T) {
	buff := NewByteBufEx(7)
	if buff.Cap() != 7 {
		t.Fail()
	}

	err := buff.PutUint64(uint64(32))
	if err != nil {
		t.Fail()
	}

	err = buff.PutUint32(uint32(1))
	if err != nil {
		t.Fail()
	}

	a1 := uint64(0)
	err = binary.Read(buff, binary.BigEndian, &a1)
	if err != nil || a1 != 32 {
		t.Fail()
	}

	a2 := uint32(0)
	err = binary.Read(buff, binary.BigEndian, &a2)
	if err != nil || a2 != 1 {
		t.Fail()
	}

	err = binary.Read(buff, binary.BigEndian, &a2)
	require.Error(t, err)
}

func TestReadByteBuf(t *testing.T) {
	buff := NewReadByteBuf()
	_, err := buff.ReadInt64()
	require.Error(t, err)

	size := 20
	data := make([]byte, size)
	binary.BigEndian.PutUint32(data, uint32(10))
	binary.BigEndian.PutUint64(data[4:], uint64(12))
	binary.BigEndian.PutUint64(data[12:], uint64(14))

	buff.SetData(data)
	if buff.Len() != size {
		t.Fail()
	}

	bdata := buff.Bytes()
	if !bytes.Equal(data, bdata) {
		t.Fail()
	}

	// over flow
	_, err = buff.Next(size + 1)
	require.Error(t, err)

	d2, err := buff.Next(size)
	require.NoError(t, err)
	if !bytes.Equal(data, d2) || buff.Len() != 0 || buff.off != size {
		t.Fail()
	}

	// test reset
	buff.Reset()
	if buff.off != 0 {
		t.Fail()
	}

	buff.SetData(data)
	a1, err := buff.ReadUint32()
	if err != nil || a1 != 10 || buff.off != 4 {
		t.Fail()
	}

	a2, err := buff.ReadInt64()
	if err != nil || a2 != 12 || buff.off != 12 {
		t.Fail()
	}

	// test reset
	data2 := make([]byte, 8)
	n, err := buff.Read(data2)
	if n != len(data2) || err != nil || buff.Len() != 0 {
		t.Fail()
	}

	a3 := binary.BigEndian.Uint64(data2)
	if a3 != 14 {
		t.Fail()
	}

	_, err = buff.ReadUint32()
	require.Error(t, err)
	_, err = buff.ReadUint64()
	require.Error(t, err)
	_, err = buff.Read(data2)
	require.Error(t, err)
}
