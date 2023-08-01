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

import (
	"bytes"
	"io"
	"log"
	"math"
	"testing"
	"testing/iotest"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRateReader(t *testing.T) {
	{
		size := size

		rc := NewRateReader(bytes.NewReader(b), 1*1024*1024)
		defer rc.Close()

		b2 := make([]byte, size)
		now := time.Now()
		n, err := io.ReadFull(rc, b2)
		elapsed := time.Since(now).Seconds()
		log.Println(elapsed)
		assert.True(t, math.Abs(4-elapsed) < 0.5)
		assert.NoError(t, err)
		assert.Equal(t, size, n)
		assert.Equal(t, b, b2)
	}
	{
		size := int64(size)

		rc := NewRateReader(bytes.NewReader(b), 1*1024*1024)
		defer rc.Close()

		b2 := new(bytes.Buffer)
		now := time.Now()
		n, err := io.Copy(b2, rc)
		elapsed := time.Since(now).Seconds()
		log.Println(elapsed)
		assert.True(t, math.Abs(4-elapsed) < 0.5)
		assert.NoError(t, err)
		assert.Equal(t, size, n)
		assert.Equal(t, b, b2.Bytes())
	}
	{
		size := size

		rc := NewRateReader(iotest.TimeoutReader(bytes.NewReader(b)), 1*1024*1024)
		defer rc.Close()

		b2 := make([]byte, size)
		n, err := io.ReadFull(rc, b2)
		assert.Equal(t, iotest.ErrTimeout, err)
		assert.Equal(t, 1*1024*1024*int(tokenWindow)/int(time.Second), n)
		assert.Equal(t, b[:n], b2[:n])
	}
	{
		size := 10
		rc := NewRateReader(iotest.OneByteReader(bytes.NewReader(b)), 1*1024*1024)
		defer rc.Close()

		b2 := make([]byte, size)
		n, err := io.ReadFull(rc, b2)
		assert.NoError(t, err)
		assert.Equal(t, size, n)
		assert.Equal(t, b[:n], b2[:n])
	}
	{
		size := 10
		rc := NewRateReader(iotest.OneByteReader(bytes.NewReader(b[:size])), 4)
		defer rc.Close()

		b2 := make([]byte, size+1)
		n, err := io.ReadFull(rc, b2)
		assert.Equal(t, io.ErrUnexpectedEOF, err)
		assert.Equal(t, size, n)
		assert.Equal(t, b[:n], b2[:n])
	}
}
