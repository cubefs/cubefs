// Copyright 2018 The TigLabs raft Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package wal

import (
	"io/ioutil"
	"os"
	"path"
	"syscall"
	"testing"
)

var datas = []byte("abcjklds;lsdgkldsjgkdlsjglsdjqroeioewrotl;sgkdjkgsjkdgjsk129428309583908593952abcjklds;lsdgkldsjgkdlsjglsdjqroeioewrotl;sgkdjkgsjkdgjsk129428309583908593952\n")

func BenchmarkFSync(b *testing.B) {
	dir, err := ioutil.TempDir(os.TempDir(), "fbase_test_rafts_sync_")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	f, err := os.Create(path.Join(dir, "test_fsync.data"))
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := f.Write(datas)
		if err != nil {
			b.Error(err)
		}
		err = f.Sync()
		if err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkFDataSync(b *testing.B) {
	dir, err := ioutil.TempDir(os.TempDir(), "fbase_test_rafts_sync_")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	f, err := os.Create(path.Join(dir, "test_fdatasync.data"))
	err = syscall.Fallocate(int(f.Fd()), 0, 0, 1024*1024*10)
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := f.Write(datas)
		if err != nil {
			b.Error(err)
		}
		err = syscall.Fdatasync(int(f.Fd()))
		if err != nil {
			b.Error(err)
		}
	}
}
