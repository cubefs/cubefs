package crc32block

import (
	"bufio"
	"io"
	"io/ioutil"
)

/*
journal record:

	|-----block-----|------block-----|---block---|

block:

	|--crc--|------payload -----|
*/

type Decoder struct {
	from    io.ReaderAt // read from
	off     int64       // offset. readonly
	limit   int64       // size limit, readonly
	bufSize int64       // for speed
	block   blockUnit   // block buffer
}

type decoderReader struct {
	reader io.Reader //
	block  []byte    //
	i, j   int       // block[i:j]
	err    error     //
}

type rangeReader struct {
	r      io.Reader
	limit  int64
	skip   int64
	skiped bool
}

type blockReader struct {
	reader io.Reader //
	block  blockUnit //
	i, j   int       // block[i:j] is unread portion of the current block's payload.
	remain int64     //
	err    error     //
}

func (br *blockReader) Read(p []byte) (n int, err error) {
	if br.err != nil {
		return 0, br.err
	}

	for br.i == br.j {
		if br.remain == 0 {
			return n, io.EOF
		}
		br.err = br.nextBlock()
		if br.err != nil {
			return 0, br.err
		}
	}

	n = copy(p, br.block[br.i:br.j])

	br.i += n
	br.remain -= int64(n)
	return n, nil
}

func (br *blockReader) nextBlock() (err error) {
	blockLen := int64(len(br.block))
	blockPayloadLen := int64(blockLen - crc32Len)

	want := blockLen
	if br.remain < blockPayloadLen {
		want = br.remain + crc32Len
	}

	_, err = io.ReadFull(br.reader, br.block[:want])
	if err != nil {
		br.err = err
		return br.err
	}

	if err = blockUnit(br.block[:want]).check(); err != nil {
		return err
	}

	br.i = crc32Len
	br.j = int(want)

	return nil
}

func (r *rangeReader) Read(p []byte) (n int, err error) {
	if !r.skiped {
		_, err := io.CopyN(ioutil.Discard, r.r, r.skip)
		if err != nil {
			return 0, err
		}
		r.skiped = true
		r.r = io.LimitReader(r.r, r.limit)
	}
	return r.r.Read(p)
}

func (dec *Decoder) Reader(from, to int64) (r io.Reader, err error) {
	blockLen := int64(dec.block.length())
	blockPayloadLen := int64(dec.block.payload())

	blockOff := (from / blockPayloadLen) * blockLen
	encodedSize := EncodeSize(dec.limit, blockLen) - blockOff

	// raw reader
	r = io.NewSectionReader(dec.from, dec.off+blockOff, encodedSize)

	// buffer
	r = bufio.NewReaderSize(r, int(dec.bufSize))

	// decode reader
	r = NewBlockReader(r, DecodeSize(encodedSize, blockLen), dec.block)

	// range reader
	r = &rangeReader{
		r:     r,
		limit: to - from,
		skip:  from % blockPayloadLen,
	}

	return r, nil
}

func (r *decoderReader) Read(b []byte) (n int, err error) {
	if r.err != nil {
		return 0, r.err
	}

	for len(b) > 0 {
		if r.i == r.j {
			if r.err = r.nextBlock(); r.err != nil {
				if n > 0 {
					return n, nil
				}
				return n, r.err
			}
		}

		readn := copy(b, r.block[r.i:r.j])
		r.i += readn

		b = b[readn:]
		n += readn
	}
	return
}

func (r *decoderReader) nextBlock() (err error) {
	n, err := readFullOrToEnd(r.reader, r.block)
	if err != nil {
		return
	}

	if n <= crc32Len {
		return ErrMismatchedCrc
	}

	if err = blockUnit(r.block[:n]).check(); err != nil {
		return ErrMismatchedCrc
	}

	r.i, r.j = crc32Len, n

	return nil
}

func NewBlockReader(r io.Reader, limit int64, block []byte) *blockReader {
	if block == nil || !isValidBlockLen(int64(len(block))) {
		panic(ErrInvalidBlock)
	}
	return &blockReader{reader: r, remain: limit, block: block}
}

func NewDecoderReader(in io.Reader) (dec *decoderReader) {
	chunk := make([]byte, defaultCrc32BlockSize)
	dec = &decoderReader{block: chunk, err: nil, reader: in}
	return
}

func NewDecoderWithBlock(r io.ReaderAt, off int64, size int64, block []byte, bufferSize int64) (dec *Decoder, err error) {
	if block == nil || !isValidBlockLen(int64(len(block))) {
		return nil, ErrInvalidBlock
	}
	return &Decoder{from: r, off: off, block: block, limit: size, bufSize: bufferSize}, nil
}

func NewDecoder(r io.ReaderAt, off int64, size int64) (dec *Decoder, err error) {
	block := make([]byte, defaultCrc32BlockSize)
	return NewDecoderWithBlock(r, off, size, block, defaultCrc32BlockSize)
}
