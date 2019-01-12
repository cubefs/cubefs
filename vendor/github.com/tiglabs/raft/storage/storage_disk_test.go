// Copyright 2015 The etcd Authors
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

package storage

import (
	"math"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/tiglabs/raft/proto"
)

const (
	test_baseDir = "/tmp/shardDB_storage"
)

var (
	test_shardId          uint64
	test_raftLog          *DiskRotateStorage
	test_entries          []*proto.Entry
	test_entryNum         = Log_MaxEntryNum + Log_EntryCacheNum
	test_entryDataMaxSize = 65536
)

func TestInit(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	// rand.Seed(0)
	test_shardId = uint64(rand.Int63())
	e := os.MkdirAll(test_baseDir, 0755)
	if e != nil {
		t.Fatalf("MkdirAll %v failed, %v", test_baseDir, e)
	}
	test_entries = makeEntries(1, test_entryNum, test_entryDataMaxSize)
}

func Test_Interface(t *testing.T) {
	var (
		si Storage
		e  error
	)
	test_raftLog, e = NewDiskRotateStorage(test_baseDir, test_shardId)
	if e != nil {
		t.Fatalf("NewDiskRotateStorage %v %v failed, %v", test_baseDir, test_shardId, e)
	}
	si = test_raftLog
	t.Logf("NewDiskRotateStorage return %v, %v", si, e)
}

func Test_Write(t *testing.T) {
	raftLog := test_raftLog
	if raftLog == nil {
		t.Fatalf("NewDiskRotateStorage %v %v failed, exit.", test_baseDir, test_shardId)
	}

	e := raftLog.StoreEntries(test_entries)
	if e != nil {
		t.Fatalf("StoreEntries test_entries(%d) failed, %v", len(test_entries), e)
	}

	lhs := [][2]uint64{
		{1, Log_EntryCacheNum},
		{1, Log_MaxEntryNum + Log_EntryCacheNum},
		{1, Log_MaxEntryNum},
		{Log_MaxEntryNum + 1, Log_MaxEntryNum + Log_EntryCacheNum},
	}
	for i := range lhs {
		lo, hi := lhs[i][0], lhs[i][1]
		entries := test_entries[lo-1 : hi]
		e := raftLog.StoreEntries(entries)
		if e != nil {
			t.Fatalf("StoreEntries test_entries[%d:%d] len=%d failed, %v", lo, hi, len(entries), e)
		}
		t.Logf("StoreEntries[%d, %d] len=%d", lo, hi, len(entries))
	}
}

func Test_Read(t *testing.T) {
	raftLog := test_raftLog
	if raftLog == nil {
		t.Fatalf("NewDiskRotateStorage %v %v failed, exit.", test_baseDir, test_shardId)
	}

	e := raftLog.StoreEntries(test_entries)
	if e != nil {
		t.Fatalf("StoreEntries test_entries(%d) failed, %v", len(test_entries), e)
	}

	lhs := [][2]uint64{
		{1, Log_EntryCacheNum},
		{1, Log_MaxEntryNum},
		{1, Log_MaxEntryNum + Log_EntryCacheNum},
		{Log_MaxEntryNum + 1, Log_MaxEntryNum + Log_EntryCacheNum},
	}
	for i := range lhs {
		lo, hi := lhs[i][0], lhs[i][1]+1
		es, isCompact, e := raftLog.Entries(lo, hi, math.MaxUint32)
		if e != nil {
			t.Fatalf("Entries[%d, %d) test_entries(%d) failed, %v", lo, hi, len(test_entries), e)
		}
		t.Logf("Entries[%d, %d), isCompact=%v, entryNum=%d", lo, hi, isCompact, len(es))
	}
}

func TestCleanup(t *testing.T) {
	if test_raftLog != nil {
		test_raftLog.Close()
	}
	os.RemoveAll(test_baseDir)
}

func makeEntries(startIndex uint64, entriesNum, maxDataSize int) []*proto.Entry {
	data := make([]byte, maxDataSize)
	entries := make([]*proto.Entry, entriesNum)
	for i := range entries {
		entries[i] = &proto.Entry{
			Index: uint64(i) + startIndex,
			Term:  1,
			Type:  proto.EntryNormal,
			Data:  data[:rand.Intn(maxDataSize)],
		}
	}
	return entries
}
