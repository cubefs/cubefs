// Copyright 2018 The ChubaoFS Authors.
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
	"bytes"
	"io"
	"io/ioutil"
	"net/http/httputil"
	"testing"
)

type readCloser struct {
	io.Reader
}

func (r *readCloser) Read(p []byte) (n int, err error) {
	return r.Reader.Read(p)
}

func (readCloser) Close() error {
	return nil
}

func wrapReader(reader io.Reader) io.ReadCloser {
	return &readCloser{
		Reader: reader,
	}
}

func TestChunkedReader_ReadChunked(t *testing.T) {
	var err error
	plain := `Hello world.`
	buffer := new(bytes.Buffer)
	chunkedWriter := httputil.NewChunkedWriter(buffer)
	if _, err = chunkedWriter.Write([]byte(plain)); err != nil {
		t.Fatal(err)
	}
	_ = chunkedWriter.Close()
	chunked := string(buffer.Bytes())
	t.Logf("plain:\n%v", plain)
	t.Logf("chunked:\n%v", chunked)

	reader := NewChunkedReader(wrapReader(bytes.NewReader([]byte(chunked))))
	result, err := ioutil.ReadAll(reader)
	if err != nil {
		t.Fatalf("read from chunked readCloser fail: err(%v)", err)
	}
	if string(result) != plain {
		t.Fatalf("result mismatch:\nexpect(%v)\nactual(%v)", plain, string(result))
	}
	t.Logf("result: %v", string(result))
}
