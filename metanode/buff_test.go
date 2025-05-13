package metanode

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDentryBuf(t *testing.T) {
	b1 := GetDentryBuf()
	require.True(t, b1.Cap() == dentryBufSize && b1.Len() == 0)

	d1 := []byte("test hello")
	n, err := b1.Write(d1)
	if err != nil || n != len(d1) {
		t.Fail()
	}
	PutDentryBuf(b1)

	b2 := GetDentryBuf()
	require.True(t, b2.Cap() == dentryBufSize)
	require.True(t, b2.Len() == 0)

	// data overflow buf size
	d2 := make([]byte, dentryBufSize*2)
	n, err = b2.Write(d2)
	if err != nil || n != len(d2) || b2.Len() != n {
		t.Fail()
	}
	PutDentryBuf(b2)

	b3 := GetDentryBuf()
	if b3.Len() != 0 {
		t.Fail()
	}
}

func TestInodeBuf(t *testing.T) {
	b1 := GetInodeBuf()
	require.True(t, b1.Cap() == inodeBufSize)

	d1 := []byte("test hello")
	n, err := b1.Write(d1)
	if err != nil || n != len(d1) {
		t.Fail()
	}
	PutInodeBuf(b1)

	b2 := GetInodeBuf()
	require.True(t, b2.Cap() == inodeBufSize)
	require.True(t, b2.Len() == 0)

	// data overflow buf size
	d2 := make([]byte, inodeBufSize*2)
	n, err = b2.Write(d2)
	if err != nil || n != len(d2) || b2.Len() != n {
		t.Fail()
	}
	PutInodeBuf(b2)

	b3 := GetInodeBuf()
	if b3.Len() != 0 {
		t.Fail()
	}
}

func TestReadBuf(t *testing.T) {
	data := []byte("test hello")
	b1 := GetReadBuf(data)
	require.True(t, b1.Len() == len(data))
	_, err := b1.ReadUint64()
	if err != nil {
		t.Fail()
	}
	PutReadBuf(b1)

	b2 := GetReadBuf(data)
	require.True(t, b2.Len() == len(data))
	PutReadBuf(b2)
}
