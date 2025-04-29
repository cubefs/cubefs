package base

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

// mockWriterAt implements io.WriterAt and CtxWriterAt for testing.
type mockWriterAt struct {
	WriteAtCtxFunc func(ctx context.Context, p []byte, off int64) (n int, err error)
}

func (m *mockWriterAt) WriteAtCtx(ctx context.Context, p []byte, off int64) (n int, err error) {
	if m.WriteAtCtxFunc != nil {
		return m.WriteAtCtxFunc(ctx, p, off)
	}
	return 0, nil
}

func newMockWriterAt(w io.Writer) *mockWriterAt {
	return &mockWriterAt{
		WriteAtCtxFunc: func(ctx context.Context, p []byte, off int64) (n int, err error) {
			select {
			case <-ctx.Done():
				n, err = 0, ctx.Err()
			default:
				n, err = w.Write(p) // len(p), nil
			}
			return
		},
	}
}

// mockReaderAt implements io.ReaderAt and CtxReader for testing.
type mockReaderAt struct {
	ReadAtCtxFunc func(ctx context.Context, p []byte, off int64) (n int, err error)
}

func (m *mockReaderAt) ReadAtCtx(ctx context.Context, p []byte, off int64) (n int, err error) {
	if m.ReadAtCtxFunc != nil {
		return m.ReadAtCtxFunc(ctx, p, off)
	}
	return 0, nil
}

func newMockReaderAt(src io.ReaderAt) *mockReaderAt {
	return &mockReaderAt{
		ReadAtCtxFunc: func(ctx context.Context, p []byte, off int64) (n int, err error) {
			select {
			case <-ctx.Done():
				n, err = 0, ctx.Err()
			default:
				n, err = src.ReadAt(p, off)
			}
			return
		},
	}
}

func TestWriterWithCtx_Write(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	str := "hello"
	buf := bytes.NewBuffer(nil)
	mockWt := newMockWriterAt(buf)

	writer := &WriterWithCtx{
		Offset: 0,
		Wt:     mockWt,
		Ctx:    ctx,
	}

	// Test normal write
	n, err := writer.Write([]byte(str))
	require.NoError(t, err)
	require.Equal(t, 5, n)
	require.Equal(t, int64(5), writer.Offset)

	// Test context cancellation
	cancel()
	n, err = writer.Write([]byte("world"))
	require.Error(t, err)
	require.Equal(t, context.Canceled, err)
	require.Equal(t, 0, n)                    // n should be 0 as no bytes were written after the context was canceled
	require.Equal(t, int64(5), writer.Offset) // Offset should remain unchanged
}

func TestReaderWithCtx_ReadAt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	data := []byte("hello")
	buf := bytes.NewReader(data)
	mockRd := newMockReaderAt(buf)

	reader := &ReaderWithCtx{
		Rd:  mockRd,
		Ctx: ctx,
	}

	// Test normal read at
	p := make([]byte, 5)
	n, err := reader.ReadAt(p, 0)
	require.NoError(t, err)
	require.Equal(t, 5, n)
	require.Equal(t, string(data), string(p))

	// Test context cancellation
	cancel()
	p = make([]byte, 5)
	n, err = reader.ReadAt(p, 0)
	require.Error(t, err)
	require.Equal(t, context.Canceled, err)
	require.Equal(t, 0, n) // n should be 0 as no bytes were read after the context was canceled
}

func TestReaderWithCtx_Read(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	data := []byte("hello")
	buf := bytes.NewReader(data)
	mockRd := newMockReaderAt(buf)

	reader := &ReaderWithCtx{
		Rd:  mockRd,
		Ctx: ctx,
	}

	// Test normal read at
	p1 := make([]byte, 3)
	n, err := reader.Read(p1)
	require.NoError(t, err)
	require.Equal(t, 3, n)
	p2 := make([]byte, 2)
	n, err = reader.Read(p2)
	require.NoError(t, err)
	require.Equal(t, 2, n)
	p := append(p1, p2...)
	require.Equal(t, string(data), string(p))

	// Test context cancellation
	cancel()
	p = make([]byte, 5)
	n, err = reader.Read(p)
	require.Error(t, err)
	require.Equal(t, context.Canceled, err)
	require.Equal(t, 0, n) // n should be 0 as no bytes were read after the context was canceled
}
