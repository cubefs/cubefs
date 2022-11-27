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

package common

import (
	"io"
	"strings"
	"sync"

	"github.com/fatih/color"

	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
)

var boldRunes = []rune(" ▏▎▍▌▋▋▊▉█")

// BoldBar returns bold progress bar like:
// [███▍      ]
//
// curr is [0 - 100]
func BoldBar(curr int) string {
	if curr == 100 {
		return "██████████"
	}
	return fmt.Sprintf("%s%*c", strings.Repeat("█", curr/10), curr/10-10, boldRunes[curr%10])
}

// LineBar returns line progress bar like:
// [===============================================>  ]
//
// curr is [0 - 100]
// width is the length of bar
func LineBar(curr, width int) string {
	n := 100 / width
	if n <= 0 {
		n = 1
	}

	if curr == 100 {
		return strings.Repeat("=", 100/n)
	}
	return fmt.Sprintf("%s>%s", strings.Repeat("=", curr/n), strings.Repeat(" ", (100-curr-1)/n))
}

// Loader loader bar
func Loader(all int) chan<- int {
	ch := make(chan int)

	go func() {
		loaded := 0
		for {
			n, ok := <-ch
			if !ok {
				fmt.Println()
				return
			}
			loaded += n
			curr := loaded * 100 / all
			fmt.Printf("\rLoading: [%s] %s/%s", BoldBar(curr),
				color.GreenString("%-5d", loaded),
				color.RedString("%5d", all))
			if loaded >= all {
				fmt.Println()
				return
			}
		}
	}()

	return ch
}

// PReader progress bar for io.Reader
// example:
// reader := NewPReader(1024, r)
// defer reader.Close()
// reader.LineBar()
//
// // balabala ...
type PReader struct {
	total int
	r     io.Reader

	readCh  chan int
	closeCh chan struct{}
	once    sync.Once
}

// NewPReader returns a progress reader
func NewPReader(total int, r io.Reader) *PReader {
	return &PReader{
		total: total,
		r:     io.LimitReader(r, int64(total)),

		readCh:  make(chan int, 1),
		closeCh: make(chan struct{}),
	}
}

func (r *PReader) Read(p []byte) (n int, err error) {
	n, err = r.r.Read(p)
	if n > 0 {
		r.readCh <- n
	}
	return
}

func (r *PReader) bar(width int, printFunc func(curr int)) {
	go func() {
		read := 0
		for {
			select {
			case n := <-r.readCh:
				read += n
				curr := read * 100 / r.total
				printFunc(curr)
				if read >= r.total {
					fmt.Println()
					return
				}
			case <-r.closeCh:
				fmt.Println()
				return
			}
		}
	}()
}

// LineBar line bar
func (r *PReader) LineBar(width int) {
	r.bar(width, func(curr int) {
		fmt.Printf("\rReading: [%s] %s%%", LineBar(curr, width), color.GreenString("%3d", curr))
	})
}

// BoldBar bold bar
func (r *PReader) BoldBar() {
	r.bar(0, func(curr int) {
		fmt.Printf("\rReading: [%s] %s%%", BoldBar(curr), color.GreenString("%3d", curr))
	})
}

// Close the bar
func (r *PReader) Close() {
	r.once.Do(func() {
		close(r.closeCh)
	})
}

// PWriter progress bar for io.Writer
type PWriter struct {
	total int
	w     io.Writer

	writeCh chan int
	closeCh chan struct{}
	once    sync.Once
}

// NewPWriter returns a progress writer
func NewPWriter(total int, w io.Writer) *PWriter {
	return &PWriter{
		total: total,
		w:     w,

		writeCh: make(chan int, 1),
		closeCh: make(chan struct{}),
	}
}

func (w *PWriter) Write(p []byte) (n int, err error) {
	n, err = w.w.Write(p)
	if n > 0 {
		w.writeCh <- n
	}
	return
}

func (w *PWriter) bar(width int, printFunc func(curr int)) {
	go func() {
		written := 0
		for {
			select {
			case n := <-w.writeCh:
				written += n
				curr := written * 100 / w.total
				printFunc(curr)
				if written >= w.total {
					fmt.Println()
					return
				}
			case <-w.closeCh:
				fmt.Println()
				return
			}
		}
	}()
}

// LineBar line bar
func (w *PWriter) LineBar(width int) {
	w.bar(width, func(curr int) {
		fmt.Printf("\rWriting: [%s] %s%%", LineBar(curr, width), color.GreenString("%3d", curr))
	})
}

// BoldBar bold bar
func (w *PWriter) BoldBar() {
	w.bar(0, func(curr int) {
		fmt.Printf("\rWriting: [%s] %s%%", BoldBar(curr), color.GreenString("%3d", curr))
	})
}

// Close the bar
func (w *PWriter) Close() {
	w.once.Do(func() {
		close(w.closeCh)
	})
}
