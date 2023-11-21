// Copyright 2018 The CubeFS Authors.
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

package storage

type Marker interface {
	Len() int
	Get(i int) (ino, extent uint64, offset, size int64)
	Add(ino, extent uint64, offset, size int64)
	Walk(f func(i int, ino, extent uint64, offset, size int64) bool)
}

type batchMarker [][4]uint64

func (b *batchMarker) Len() int {
	return len(*b)
}

func (b *batchMarker) Get(i int) (ino, extent uint64, offset, size int64) {
	v := (*b)[i]
	return v[0], v[1], int64(v[2]), int64(v[3])
}

func (b *batchMarker) Add(ino, extent uint64, offset, size int64) {
	*b = append(*b, [4]uint64{
		ino, extent, uint64(offset), uint64(size),
	})
}

func (b *batchMarker) Walk(f func(i int, ino, extent uint64, offset, size int64) bool) {
	if f == nil {
		return
	}
	for i := 0; i < len(*b); i++ {
		if !f(i, (*b)[i][0], (*b)[i][1], int64((*b)[i][2]), int64((*b)[i][3])) {
			break
		}
	}
}

func BatchMarker(capacity int) Marker {
	b := batchMarker(make([][4]uint64, 0, capacity))
	return &b
}

type singleMarker [4]uint64

func (b *singleMarker) Len() int {
	return 1
}

func (b *singleMarker) Get(i int) (ino, extent uint64, offset, size int64) {
	if i != 0 {
		panic("single marker only have one record")
	}
	return b[0], b[1], int64(b[2]), int64(b[3])
}

func (b *singleMarker) Add(_, _ uint64, _, _ int64) {
	panic("single marker do not allow to add record")
}

func (b *singleMarker) Walk(f func(i int, ino uint64, extent uint64, offset int64, size int64) bool) {
	if f != nil {
		f(0, (*b)[0], (*b)[1], int64((*b)[2]), int64((*b)[3]))
	}
}

func SingleMarker(ino, extent uint64, offset, size int64) Marker {
	b := singleMarker{ino, extent, uint64(offset), uint64(size)}
	return &b
}
