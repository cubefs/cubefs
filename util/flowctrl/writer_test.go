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

func TestRateWriter(t *testing.T) {
	{
		size := size

		w := bytes.NewBuffer(nil)
		wc := NewRateWriter(w, 1*1024*1024)
		defer wc.Close()

		now := time.Now()
		n, err := wc.Write(b)
		elapsed := time.Since(now).Seconds()
		log.Println(elapsed)
		assert.True(t, math.Abs(4-elapsed) < 0.5)
		assert.NoError(t, err)
		assert.Equal(t, size, n)
		assert.Equal(t, b, w.Bytes())
	}
	{
		size := int64(size)

		w := bytes.NewBuffer(nil)
		wc := NewRateWriter(w, 1*1024*1024)
		defer wc.Close()

		now := time.Now()
		n, err := io.Copy(wc, bytes.NewBuffer(b))
		elapsed := time.Since(now).Seconds()
		log.Println(elapsed)
		assert.True(t, math.Abs(4-elapsed) < 0.5)
		assert.NoError(t, err)
		assert.Equal(t, size, n)
		assert.Equal(t, b, w.Bytes())
	}
	{
		w := bytes.NewBuffer(nil)
		wc := NewRateWriter(w, 5*1024)
		defer wc.Close()

		n, err := io.Copy(wc, iotest.TimeoutReader(bytes.NewReader(b)))
		assert.Equal(t, iotest.ErrTimeout, err)
		assert.Equal(t, b[:n], w.Bytes()[:n])
	}
	{
		w := bytes.NewBuffer(nil)
		wc := NewRateWriter(w, 5*1024)
		defer wc.Close()

		n, err := wc.Write(b[:1])
		assert.NoError(t, err)
		assert.Equal(t, 1, n)
		assert.Equal(t, b[:1], w.Bytes())
	}
}
