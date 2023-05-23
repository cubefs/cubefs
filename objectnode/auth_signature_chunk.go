// Copyright 2023 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package objectnode

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"errors"
	"io"
	"strings"
)

// https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/API/sigv4-streaming.html#sigv4-chunked-body-definition

func NewSignChunkedReader(r io.Reader, key []byte, scope, datetime, seed string) *SignChunkedReader {
	br, ok := r.(*bufio.Reader)
	if !ok {
		br = bufio.NewReader(r)
	}
	return &SignChunkedReader{
		reader:   br,
		buf:      bytes.NewBuffer(nil),
		key:      key,
		scope:    scope,
		datetime: datetime,
		prevSig:  seed,
	}
}

type SignChunkedReader struct {
	reader *bufio.Reader
	buf    *bytes.Buffer

	key      []byte // signingKey
	scope    string // <yyyymmdd>/<region>/<service>/aws4_request
	datetime string // 20130524T000000Z
	prevSig  string // previous signature
}

func (cr *SignChunkedReader) Read(p []byte) (n int, err error) {
	for err == nil {
		if len(p) == 0 {
			break
		}
		if err = cr.fillCheck(); err != nil {
			break
		}
		if len(p) > cr.buf.Len() {
			p = p[:cr.buf.Len()]
		}
		var rn int
		if rn, err = cr.buf.Read(p); err != nil {
			break
		}
		n += rn
		p = p[rn:]
	}
	return
}

func (cr *SignChunkedReader) fillCheck() error {
	if cr.buf.Len() > 0 {
		return nil
	}

	cr.buf.Reset()
	header, truncated, err := cr.reader.ReadLine()
	if truncated {
		return errors.New("header line of chunk is too long")
	}
	if err != nil {
		if err == io.EOF && len(header) > 0 {
			err = io.ErrUnexpectedEOF
		}
		return err
	}

	signature, size, err := parseSignChunkedHeader(string(header))
	if err != nil {
		return err
	}
	if size == 0 {
		if signature != cr.getSignature(cr.buf.Bytes()) {
			return errors.New("signature of chunk does not match")
		}
		return io.EOF
	}

	cn, err := io.CopyN(cr.buf, cr.reader, size)
	if err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return err
	}
	if cn != size {
		return io.ErrShortBuffer
	}
	if signature != cr.getSignature(cr.buf.Bytes()) {
		return errors.New("signature of chunk does not match")
	}
	last := make([]byte, 2)
	if _, err = io.ReadFull(cr.reader, last[:2]); len(last) != 2 || string(last) != "\r\n" || err != nil {
		if err == nil {
			err = errors.New("malformed chunked encoding")
		}
		return err
	}
	cr.prevSig = signature

	return nil
}

func (cr *SignChunkedReader) getSignature(data []byte) string {
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256-PAYLOAD",
		cr.datetime,
		cr.scope,
		cr.prevSig,
		EmptyStringSHA256,
		hex.EncodeToString(MakeSha256(data)),
	}, "\n")

	return hex.EncodeToString(MakeHmacSha256(cr.key, []byte(stringToSign)))
}

func (cr *SignChunkedReader) Close() error {
	return nil
}

func parseSignChunkedHeader(header string) (sign string, size int64, err error) {
	headers := strings.SplitN(header, ";chunk-signature=", 2)
	if len(headers) != 2 {
		err = errors.New("malformed chunked encoding")
		return
	}

	csize, err := parseHexUint([]byte(headers[0]))
	if err != nil {
		return
	}

	sign = headers[1]
	size = int64(csize)

	return
}

// parseHexUint copy from net/http/internal/chunked.go
func parseHexUint(v []byte) (n uint64, err error) {
	for i, b := range v {
		switch {
		case '0' <= b && b <= '9':
			b = b - '0'
		case 'a' <= b && b <= 'f':
			b = b - 'a' + 10
		case 'A' <= b && b <= 'F':
			b = b - 'A' + 10
		default:
			return 0, errors.New("invalid byte in chunk length")
		}
		if i == 16 {
			return 0, errors.New("http chunk length too large")
		}
		n <<= 4
		n |= uint64(b)
	}
	return
}
