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

package common_test

import (
	"math/rand"
	"testing"
	"time"

	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
)

func TestCmdCommonProgressBoldBar(t *testing.T) {
	for curr := 0; curr <= 100; curr++ {
		fmt.Printf("\r[%s] %d%%", common.BoldBar(curr), curr)
		time.Sleep(3 * time.Millisecond)
	}
	fmt.Println()
}

func TestCmdCommonProgressLineBar(t *testing.T) {
	for curr := 0; curr <= 100; curr++ {
		fmt.Printf("\r[%s] %d%%", common.LineBar(curr, 10), curr)
		time.Sleep(time.Millisecond)
	}
	fmt.Println()

	for curr := 0; curr <= 100; curr++ {
		fmt.Printf("\r[%s] %d%%", common.LineBar(curr, 50), curr)
		time.Sleep(time.Millisecond)
	}
	fmt.Println()

	for curr := 0; curr <= 100; curr++ {
		fmt.Printf("\r[%s] %d%%", common.LineBar(curr, 100), curr)
		time.Sleep(time.Millisecond)
	}
	fmt.Println()
}

func TestCmdCommonProgressLoader(t *testing.T) {
	alls := []int{
		1,
		7,
		17,
		20,
		100,
		19999,
	}

	for _, all := range alls {
		ch := common.Loader(all)
		n := (all + 9) / 10
		loaded := 0
		for ii := 0; ii < 10; ii++ {
			if loaded >= all {
				break
			}
			if loaded+n > all {
				ch <- (all - loaded)
			} else {
				ch <- n
			}
			loaded += n
			time.Sleep(20 * time.Millisecond)
		}
		close(ch)
	}
}

type mockReader struct{}

func (r *mockReader) Read(p []byte) (n int, err error) {
	time.Sleep(time.Millisecond)
	return len(p), nil
}

func TestCmdCommonProgressReader(t *testing.T) {
	sizes := []int{
		1,
		1 << 10,
		10250,
		1 << 20,
		1 << 30,
	}

	rand.Seed(time.Now().Unix())
	for _, size := range sizes {
		reader := common.NewPReader(size, &mockReader{})
		reader.LineBar((int(rand.Int31n(9)+1) * 10))
		l := (size / 100) + 1
		// 102 times
		for ii := 0; ii <= 101; ii++ {
			_, _ = reader.Read(make([]byte, l))
		}
		reader.Close()
	}

	for _, size := range sizes {
		reader := common.NewPReader(size, &mockReader{})
		reader.BoldBar()
		l := (size / 100) + 1
		for ii := 0; ii <= 101; ii++ {
			_, _ = reader.Read(make([]byte, l))
		}
		reader.Close()
	}
}

type mockWriter struct {
	total   int
	written int
}

func (w *mockWriter) Write(p []byte) (n int, err error) {
	if w.written >= w.total {
		return 0, nil
	}
	w.written += len(p)
	time.Sleep(time.Millisecond)
	return len(p), nil
}

func TestCmdCommonProgressWriter(t *testing.T) {
	sizes := []int{
		1,
		1 << 10,
		10250,
		1 << 20,
		1 << 30,
	}

	rand.Seed(time.Now().Unix())
	for _, size := range sizes {
		writer := common.NewPWriter(size, &mockWriter{total: size})
		writer.LineBar((int(rand.Int31n(9)+1) * 10))
		l := (size / 100) + 1
		// 102 times
		for ii := 0; ii <= 101; ii++ {
			_, _ = writer.Write(make([]byte, l))
		}

		writer.Close()
	}

	for _, size := range sizes {
		writer := common.NewPWriter(size, &mockWriter{total: size})
		writer.BoldBar()
		l := (size / 100) + 1
		for ii := 0; ii <= 101; ii++ {
			_, _ = writer.Write(make([]byte, l))
		}
		writer.Close()
	}
}
