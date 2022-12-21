// Copyright 2022 The CubeFS Authors.
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

package auditlog

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"

	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/rpc/auth"
)

const maxSeekableBodyLength = 1 << 10

var DefaultRequestHeaderKeys = []string{
	"User-Agent",
	"Range",
	"Refer",
	"Referer",
	"Origin",
	"Content-Length",
	"Accept-Encoding",
	"If-None-Match",
	"If-Modified-Since",

	"Cdn",
	"Cdn-Src-Ip",
	"Cdn-Scheme",

	"X-Real-Ip",
	"X-Forwarded-For",
	"X-Scheme",
	"X-Remote-Ip",
	"X-From-Cdn",
	"X-Id",
	"X-From-Proxy-Getter",
	"X-From-Fsrcproxy",
	"X-Upload-Encoding",
	"X-Src",

	auth.TokenHeaderKey,
}

type DecodedReq struct {
	Path   string
	Header M
	Params []byte
}

type ReadCloser struct {
	io.Reader
	io.Closer
}

type M map[string]interface{}

func (m M) Encode() []byte {
	if len(m) > 0 {
		ret, _ := json.Marshal(m)
		return ret
	}
	return nil
}

type defaultDecoder struct{}

func (d *defaultDecoder) DecodeReq(req *http.Request) *DecodedReq {
	header := req.Header
	clientIP, _, err := net.SplitHostPort(req.RemoteAddr)
	if err != nil {
		clientIP = req.RemoteAddr
	}
	decodedReq := &DecodedReq{
		Header: M{"IP": clientIP, "Host": req.Host},
		Path:   req.URL.Path,
	}

	// decode header information
	if req.URL.RawQuery != "" {
		decodedReq.Header["RawQuery"] = req.URL.RawQuery
	}
	cm := req.Header.Get("Content-MD5")
	if cm != "" {
		decodedReq.Header["Content-MD5"] = cm
	}

	for _, key := range DefaultRequestHeaderKeys {
		if v, ok := header[key]; ok {
			decodedReq.Header[key] = v[0]
		}
	}
	// crc check header
	if encoded, ok := req.Header[rpc.HeaderCrcEncoded]; ok {
		decodedReq.Header[rpc.HeaderCrcEncoded] = encoded[0]
	}

	// decode request params, including:
	// 1. form-urlencoded
	// 2. json
	contentType, ok := req.Header["Content-Type"]
	if ok {
		decodedReq.Header["Content-Type"] = contentType[0]
	}
	if ok {
		switch contentType[0] {
		case rpc.MIMEPOSTForm:
			buff, err := d.readFull(req)
			if err == nil {
				req.Body = ReadCloser{bytes.NewReader(buff), req.Body}
				req.ParseForm()
				params := make(M)
				for k, v := range req.Form {
					if len(v) == 1 {
						params[k] = v[0]
					} else {
						params[k] = v
					}
				}
				decodedReq.Params = params.Encode()
			}
		case rpc.MIMEJSON:
			buff, err := d.readFull(req)
			if err == nil {
				req.Body = ReadCloser{bytes.NewReader(buff), req.Body}
				// check if request body is valid or not, and do not print invalid request body in audit log
				if json.Valid(buff) {
					decodedReq.Params = compactNewline(buff)
				}
			}
		}
	}
	return decodedReq
}

func (d *defaultDecoder) readFull(req *http.Request) ([]byte, error) {
	if req.ContentLength > maxSeekableBodyLength {
		return nil, errors.New("too large body for form or json")
	}

	buff := make([]byte, req.ContentLength)
	_, err := io.ReadFull(req.Body, buff)
	if err != nil {
		return nil, err
	}
	return buff, nil
}

// compactNewline json compact if buffer has '\n'(0x0a).
func compactNewline(src []byte) []byte {
	if bytes.IndexByte(src, 0x0a) < 0 {
		return src
	}
	newBuff := bytes.NewBuffer(make([]byte, 0, len(src)))
	if err := json.Compact(newBuff, src); err == nil {
		return newBuff.Bytes()
	}
	return src
}
