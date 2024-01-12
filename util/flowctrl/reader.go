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

package flowctrl

import "io"

type rateReader struct {
	underlying io.Reader
	c          *Controller
}

func (self *rateReader) Read(p []byte) (n int, err error) {
	size := len(p)
	size = self.c.acquire(size)

	n, err = self.underlying.Read(p[:size])
	self.c.fill(size - n)
	return
}

func NewRateReader(r io.Reader, ratePerSecond int) io.ReadCloser {
	c := NewController(ratePerSecond)
	r = c.Reader(r)
	return struct {
		io.Reader
		io.Closer
	}{
		Reader: r,
		Closer: c,
	}
}

func NewRateReaderWithCtrl(r io.Reader, c *Controller) io.Reader {
	r = c.Reader(r)
	return r
}
