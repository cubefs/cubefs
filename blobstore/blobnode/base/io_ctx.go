package base

import (
	"context"
)

type CtxWriterAt interface {
	WriteAtCtx(ctx context.Context, p []byte, off int64) (n int, err error)
}

// WriterWithCtx : wrap blobfile.go WriteAtCtx. After io dequeue, it is not carried out when ctx cancel
type WriterWithCtx struct {
	Offset int64
	Wt     CtxWriterAt
	Ctx    context.Context
}

func (w *WriterWithCtx) Write(val []byte) (n int, err error) {
	n, err = w.Wt.WriteAtCtx(w.Ctx, val, w.Offset)
	w.Offset += int64(n)
	return
}

type CtxReaderAt interface {
	ReadAtCtx(ctx context.Context, p []byte, off int64) (n int, err error)
}

// ReaderWithCtx : wrap blobfile.go ReadAtCtx. After io dequeue, it is not carried out when ctx cancel
type ReaderWithCtx struct {
	offset int64
	Rd     CtxReaderAt
	Ctx    context.Context
}

func (r *ReaderWithCtx) ReadAt(val []byte, off int64) (n int, err error) {
	n, err = r.Rd.ReadAtCtx(r.Ctx, val, off)
	return
}

func (r *ReaderWithCtx) Read(val []byte) (n int, err error) {
	n, err = r.Rd.ReadAtCtx(r.Ctx, val, r.offset)
	r.offset += int64(n)
	return
}
