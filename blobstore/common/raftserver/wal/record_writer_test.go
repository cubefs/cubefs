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

package wal

import (
	"os"
	"path"
	"testing"

	pb "go.etcd.io/etcd/raft/v3/raftpb"
)

func TestRecordWriter(t *testing.T) {
	dir := "/tmp/raftwal-" + string(genRandomBytes(8))
	name := logName{1, 100}
	file := path.Join(dir, name.String())

	os.RemoveAll(dir)
	err := InitPath(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	f, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	recWriter := newRecordWriter(f)
	defer f.Close()

	for i := 0; i < 10000; i++ {
		entry := Entry{
			Entry: pb.Entry{
				Term:  1,
				Index: uint64(i + 1),
				Type:  pb.EntryNormal,
				Data:  genRandomBytes(1024),
			},
		}
		if err = recWriter.Write(EntryType, &entry); err != nil {
			t.Fatal(err)
		}
	}

	if err = recWriter.Sync(); err != nil {
		t.Fatal(err)
	}

	recWriter.Truncate(1024)
	recWriter.Close()
}
