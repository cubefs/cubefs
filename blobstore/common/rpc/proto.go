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

package rpc

import (
	"bytes"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"runtime"
	"strings"

	"github.com/cubefs/cubefs/blobstore/util/version"
)

// headers
const (
	HeaderContentType   = "Content-Type"
	HeaderContentLength = "Content-Length"
	HeaderContentRange  = "Content-Range"
	HeaderContentMD5    = "Content-MD5"
	HeaderUA            = "User-Agent"

	// trace
	HeaderTraceLog  = "Trace-Log"
	HeaderTraceTags = "Trace-Tags"

	// crc checker
	HeaderCrcEncoded    = "X-Crc-Encoded"
	HeaderAckCrcEncoded = "X-Ack-Crc-Encoded"
)

// mime
const (
	MIMEStream            = "application/octet-stream"
	MIMEJSON              = "application/json"
	MIMEXML               = "application/xml"
	MIMEPlain             = "text/plain"
	MIMEPOSTForm          = "application/x-www-form-urlencoded"
	MIMEMultipartPOSTForm = "multipart/form-data"
	MIMEYAML              = "application/x-yaml"
)

// encoding
const (
	GzipEncodingType = "gzip"
)

// UserAgent user agent
var UserAgent = "Golang blobstore/rpc package"

type (
	// ValueGetter fill argument's field from url values or http params.
	ValueGetter func(string) string
	// Parser is the interface implemented by argument types
	// that can parse themselves from url.Values.
	Parser interface {
		Parse(ValueGetter) error
	}

	// priority of marshaler and unmarshaler (default is json).
	//  - - - - - - - - - - - - - - - - - - - - - -
	//  |         | marshaler   | unmarshaler     |
	//  | higher  |
	//  |   ^     | MarshalerTo | UnmarshalerFrom |
	//  |   |     | Marshaler   | Unmarshaler     |
	//  |   |     | JSON Marshal| JSON Unmarshal  |
	//  |  lower  |
	//  - - - - - - - - - - - - - - - - - - - - - -

	// Actions on RPC.
	//  - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
	//  |     APP       |  Client     |    TCP    |  Server     |
	//  - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
	//  | Request Type  | marshaler   | - - - - > | unmarshaler |
	//  |                                              |        |
	//  |                                              \/       |
	//  | Response Type | unmarshaler | < - - - - | marshaler   |
	//  - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

	// Marshaler is the interface implemented by types that
	// can marshal themselves into bytes, second parameter
	// is content type.
	Marshaler interface {
		Marshal() ([]byte, string, error)
	}
	// MarshalerTo is the interface implemented by types that
	// can marshal themselves into writer, the first parameter
	// is content type. (Not Recommended).
	// The underlying writer is a *bytes.Buffer.
	// Context.RespondWithReader is better than MarshalerTo on Server Side.
	MarshalerTo interface {
		MarshalTo(responseBody io.Writer) (string, error)
	}
	// Unmarshaler is the interface implemented by types
	// that can unmarshal themselves from bytes.
	Unmarshaler interface {
		Unmarshal([]byte) error
	}
	// UnmarshalerFrom is the interface implemented by types
	// that can unmarshal themselves from body reader.
	// The body underlying implementation is a *io.LimitedReader.
	UnmarshalerFrom interface {
		UnmarshalFrom(requestBody io.Reader) error
	}

	// HTTPError interface of error with http status code
	HTTPError interface {
		// StatusCode http status code
		StatusCode() int
		// ErrorCode special defined code
		ErrorCode() string
		// Error detail message
		Error() string
	}
)

// ProgressHandler http progress handler
type ProgressHandler interface {
	Handler(http.ResponseWriter, *http.Request, func(http.ResponseWriter, *http.Request))
}

// NoneBody no body of request of response.
var NoneBody Marshaler = noneBody{}

type noneBody struct{}

func (noneBody) Marshal() ([]byte, string, error) {
	return []byte{}, "", nil
}

type marshalledBody struct {
	ContentLength int
	ContentType   string
	Body          io.Reader
}

func marshalObj(obj interface{}) (*marshalledBody, error) {
	var (
		buffer []byte
		ct     string = MIMEJSON
		err    error
	)
	if obj == nil {
		buffer = jsonNull[:]
	} else if o, ok := obj.(MarshalerTo); ok {
		w := bytes.NewBuffer(nil)
		ct, err = o.MarshalTo(w)
		if err != nil {
			return nil, err
		}
		return &marshalledBody{
			ContentLength: w.Len(),
			ContentType:   ct,
			Body:          w,
		}, nil

	} else if o, ok := obj.(Marshaler); ok {
		buffer, ct, err = o.Marshal()
	} else {
		buffer, err = json.Marshal(obj)
	}
	if err != nil {
		return nil, err
	}
	return &marshalledBody{
		ContentLength: len(buffer),
		ContentType:   ct,
		Body:          bytes.NewReader(buffer),
	}, nil
}

func programVersion() string {
	sp := strings.Fields(strings.TrimSpace(version.Version()))
	if len(sp) == 0 || sp[0] == "develop" {
		data, err := ioutil.ReadFile(os.Args[0])
		if err != nil {
			return "_"
		}
		return fmt.Sprintf("%x", md5.Sum(data))[:10]
	}
	if len(sp) > 10 {
		return sp[0][:10]
	}
	return sp[0]
}

func init() {
	hostname, _ := os.Hostname()
	ua := fmt.Sprintf("%s/%s (%s/%s; %s) %s/%s",
		path.Base(os.Args[0]),
		programVersion(),
		runtime.GOOS,
		runtime.GOARCH,
		runtime.Version(),
		hostname,
		fmt.Sprint(os.Getpid()),
	)
	if UserAgent != ua {
		UserAgent = ua
	}
}
