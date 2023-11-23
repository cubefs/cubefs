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

type rateWriter struct {
	underlying io.Writer
	c          *Controller
}

func (self *rateWriter) Write(p []byte) (written int, err error) {
write:
	size := len(p)
	size = self.c.acquire(size)

	n, err := self.underlying.Write(p[:size])
	self.c.fill(size - n)
	written += n
	if err != nil {
		return
	}
	if size != len(p) {
		p = p[size:]
		goto write
	}
	return
}

func NewRateWriter(w io.Writer, ratePerSecond int) io.WriteCloser {
	c := NewController(ratePerSecond)
	w = c.Writer(w)
	return struct {
		io.Writer
		io.Closer
	}{
		Writer: w,
		Closer: c,
	}
}

func NewRateWriterWithCtrl(w io.Writer, c *Controller) io.Writer {
	w = c.Writer(w)
	return w
}
