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
	"math"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/tiglabs/raft/proto"
)

func TestMeta(t *testing.T) {
	dir, err := ioutil.TempDir(os.TempDir(), "fbase_test_meta_")
	if err != nil {
		t.Fatal(err)
	}

	defer os.RemoveAll(dir)

	err = initDir(dir)
	if err != nil {
		t.Fatal(err)
	}

	hsf, hs, meta, err := openMetaFile(dir)
	if err != nil {
		t.Error(err)
	}

	if hs.Commit != 0 || hs.Term != 0 || hs.Vote != 0 {
		t.Errorf("expect empty hardstate, actual:%v", hs)
	}
	if meta.truncIndex != 0 || meta.truncTerm != 0 {
		t.Errorf("expect empty meta, actual:%v", meta)
	}

	nhs := proto.HardState{
		Commit: 1,
		Term:   2,
		Vote:   3,
	}
	err = hsf.SaveHardState(nhs)
	if err != nil {
		t.Error(err)
	}

	nmeta := truncateMeta{
		truncIndex: 5,
		truncTerm:  6,
	}
	err = hsf.SaveTruncateMeta(nmeta)

	err = hsf.Sync()
	if err != nil {
		t.Error(err)
	}

	err = hsf.Close()
	if err != nil {
		t.Error(err)
	}

	hsf, hs, meta, err = openMetaFile(dir)
	if err != nil {
		t.Error(err)
	}

	if hs.Commit != nhs.Commit || hs.Vote != nhs.Vote || hs.Term != nhs.Term {
		t.Errorf("unexpect hardstate: %v, expected:%v", hs, nhs)
	}

	if meta.truncIndex != nmeta.truncIndex || meta.truncTerm != nmeta.truncTerm {
		t.Errorf("unexpect truncmeta: %v, expected:%v", meta, nmeta)
	}
}

func TestLogFileName(t *testing.T) {
	checkFunc := func(seq uint64, index uint64) {
		name := logFileName{seq: seq, index: index}
		s := name.String()
		var actualName logFileName
		if !actualName.ParseFrom(s) {
			t.Error("not ok")
		}
		if actualName.seq != seq {
			t.Errorf("wrong seq. expect: %d, actual: %d", seq, actualName.seq)
		}
		if actualName.index != index {
			t.Errorf("wrong index. expect: %d, actual: %d", index, actualName.index)
		}
	}

	checkFunc(0, 0)
	checkFunc(0, 1)
	checkFunc(1, 0)
	checkFunc(123, 456)
	checkFunc(math.MaxUint64, math.MaxUint64)
}

func TestLogEntryRecord(t *testing.T) {
	dir, err := ioutil.TempDir(os.TempDir(), "fbase_test_logentry_record_")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("TestPath: %v", dir)

	defer os.RemoveAll(dir)

	f, err := createLogEntryFile(dir, logFileName{1, 1})
	if err != nil {
		t.Fatal(err)
	}

	var lo uint64 = 1
	var hi uint64 = 100
	toWrite := genLogEntries(lo, hi)
	for _, ent := range toWrite {
		err = f.Save(ent)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Sync(); err != nil {
		t.Error(err)
	}

	for i := lo; i < hi; i++ {
		ent, err := f.Get(uint64(i))
		if err != nil {
			t.Fatal(err)
		}
		if err := compapreEntry(ent, toWrite[i-lo]); err != nil {
			t.Logf("failed at %d", i)
			t.Fatal(err)
		}
	}
}

func TestLogStorage1(t *testing.T) {
	var err error
	dir, err := ioutil.TempDir(os.TempDir(), "fbase_test_log_storage_1_")
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("TestPath: %v", dir)
	// defer func() {
	// 	if err == nil {
	// 		os.RemoveAll(dir)
	// 	}
	// }()

	c := &Config{
		FileSize: 1000,
	}

	s, err := NewStorage(dir, c)
	if err != nil {
		t.Fatal(err)
	}

	var lo uint64 = 1
	var hi uint64 = 100
	toWrite := genLogEntries(lo, hi)
	err = s.StoreEntries(toWrite)
	if err != nil {
		t.Fatal(err)
	}

	var ents []*proto.Entry
	var isCompact bool

	ents, isCompact, err = s.Entries(lo, hi, 1024)
	if err != nil {
		t.Error(err)
	}
	if isCompact {
		t.Error("expect not compact")
	}

	err = compareEntries(ents, toWrite)
	if err != nil {
		t.Error(err)
	}

	ents, isCompact, err = s.Entries(0, hi, 1024)
	if err != nil {
		t.Error(err)
	}
	if !isCompact {
		t.Error("expect compact")
	}
	if ents != nil {
		t.Error("expect nil")
	}

	ents, isCompact, err = s.Entries(20, 50, 10)
	if err != nil {
		t.Error(err)
	}
	if isCompact {
		t.Error("expect not compact")
	}
	if len(ents) != 10 {
		t.Errorf("expect entry len:%d, actual:%d", 10, len(ents))
	}
	err = compareEntries(ents, toWrite[19:29])
	if err != nil {
		t.Error(err)
	}

	var term uint64
	for i := lo; i < hi; i++ {
		term, isCompact, err = s.Term(i)
		if err != nil {
			t.Fatalf("term %d error:%v", i, err)
		}
		if isCompact {
			t.Error("expect not compact")
		}
		if term != toWrite[i-lo].Term {
			t.Errorf("exptect term: %d, actual:%d", toWrite[i-lo].Term, term)
		}
	}
	term, isCompact, err = s.Term(0)
	if err != nil {
		t.Error(err)
	}
	// if !isCompact {
	// 	t.Error("expect compact")
	// }
	if term != 0 {
		t.Error("expect 0")
	}

	s.Close()

	t.Log("Test Close Reopen.......")
	s, err = NewStorage(dir, nil)
	if err != nil {
		t.Fatal(err)
	}

	err = s.StoreEntries([]*proto.Entry{&proto.Entry{Index: hi}})
	if err != nil {
		t.Fatal(err)
	}

	ents, isCompact, err = s.Entries(lo, hi+1, 1024)
	if err != nil {
		t.Error(err)
	}
	if isCompact {
		t.Error("expect not compact")
	}
	if ents[len(ents)-1].Index != hi {
		t.Error("wrong index")
	}
	ents = ents[:len(ents)-1]

	err = compareEntries(ents, toWrite)
	if err != nil {
		t.Error(err)
	}

	ents, isCompact, err = s.Entries(0, hi, 1024)
	if err != nil {
		t.Error(err)
	}
	if !isCompact {
		t.Error("expect compact")
	}
	if ents != nil {
		t.Error("expect nil")
	}

	ents, isCompact, err = s.Entries(20, 50, 10)
	if err != nil {
		t.Error(err)
	}
	if isCompact {
		t.Error("expect not compact")
	}
	if len(ents) != 10 {
		t.Errorf("expect entry len:%d, actual:%d", 10, len(ents))
	}
	err = compareEntries(ents, toWrite[19:29])
	if err != nil {
		t.Error(err)
	}

	for i := lo; i < hi; i++ {
		term, isCompact, err = s.Term(i)
		if err != nil {
			t.Error(err)
		}
		if isCompact {
			t.Error("expect not compact")
		}
		if term != toWrite[i-lo].Term {
			t.Errorf("exptect term: %d, actual:%d", toWrite[i-lo].Term, term)
		}
	}
	term, isCompact, err = s.Term(0)
	if err != nil {
		t.Error(err)
	}
	// if !isCompact {
	// 	t.Error("expect compact")
	// }
	if term != 0 {
		t.Error("expect 0")
	}
}

func TestLogOpenTruncate(t *testing.T) {
	var err error
	dir, err := ioutil.TempDir(os.TempDir(), "fbase_test_rlog_open_truncate_")
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("TestPath: %v", dir)
	defer func() {
		if err == nil {
			os.RemoveAll(dir)
		}
	}()

	c := &Config{
		FileSize: 1000,
	}

	s, err := NewStorage(dir, c)
	if err != nil {
		t.Fatal(err)
	}

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	var ents []*proto.Entry
	var lo uint64 = 1
	i := lo
	for {
		ent := genLogEntry(rnd, i)
		err = s.StoreEntries([]*proto.Entry{ent})
		if err != nil {
			t.Fatal(err)
		}
		// 有新文件了
		if s.ls.size() >= 2 {
			t.Logf("new log file at index: %d", i)
			if err = s.ls.truncateBack(i - 1); err != nil {
				t.Fatal(err)
			}
			if s.ls.size() != 1 {
				t.Fatalf("expect logfile size = 1, current: %d", s.ls.size())
			}
			break
		} else {
			ents = append(ents, ent)
			i++
		}
	}
	s.Close()
	i--
	ents = ents[:len(ents)-1]

	// 重新打开
	s, err = NewStorage(dir, c)
	if err != nil {
		t.Fatal(err)
	}

	// truncate后再写
	ents2, isCompact, err := s.Entries(lo, i, uint64(len(ents)+100))
	if err != nil {
		t.Fatal(err)
	}
	if isCompact {
		t.Error("should not compact")
	}
	if err = compareEntries(ents, ents2); err != nil {
		t.Error(err)
	}

	for j := 0; j < 5; j++ {
		ent := genLogEntry(rnd, i)
		err = s.StoreEntries([]*proto.Entry{ent})
		if err != nil {
			t.Fatal(err)
		}
		ents = append(ents, ent)
		i++
	}

	ents2, isCompact, err = s.Entries(lo, i, uint64(len(ents)+100))
	if err != nil {
		t.Fatal(err)
	}
	if isCompact {
		t.Error("should not compact")
	}
	if err = compareEntries(ents, ents2); err != nil {
		t.Error(err)
	}
}

func TestTruncateOld(t *testing.T) {
	var err error
	dir, err := ioutil.TempDir(os.TempDir(), "fbase_test_rlog_truncate_old_")
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("TestPath: %v", dir)

	defer func() {
		if err == nil {
			os.RemoveAll(dir)
		}
	}()

	c := &Config{
		FileSize: 5000,
	}

	s, err := NewStorage(dir, c)
	if err != nil {
		t.Fatal(err)
	}

	var lo uint64 = 1
	var hi uint64 = 200
	toWrite := genLogEntries(lo, hi)
	if err = s.StoreEntries(toWrite); err != nil {
		t.Fatal(err)
	}

	if err = s.Truncate(100); err != nil {
		t.Fatal(err)
	}

	ents, isCompact, err := s.Entries(100, hi, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if !isCompact {
		t.Error("should compacted")
	}
	_ = ents

	ents, isCompact, err = s.Entries(101, hi, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if isCompact {
		t.Error("should not compacted")
	}
	if err = compareEntries(toWrite[101-lo:], ents); err != nil {
		t.Error(err)
	}
	s.Close()

	s, err = NewStorage(dir, c)
	if err != nil {
		t.Fatal(err)
	}
	// 重新打开后获取
	err = s.StoreEntries([]*proto.Entry{&proto.Entry{Index: hi}})
	if err != nil {
		t.Fatal(err)
	}
	ents, isCompact, err = s.Entries(100, hi, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if !isCompact {
		t.Error("should compacted")
	}
	_ = ents

	ents, isCompact, err = s.Entries(101, hi, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if isCompact {
		t.Error("should not compacted")
	}
	if err = compareEntries(toWrite[101-lo:], ents); err != nil {
		t.Error(err)
	}
	s.Close()
}

func TestBenchmark(t *testing.T) {
	var err error
	dir, err := ioutil.TempDir(os.TempDir(), "fbase_test_rlog_benchmark_")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("TestPath: %v", dir)
	defer os.RemoveAll(dir)

	c := &Config{
		FileSize: 1024 * 1024 * 10,
	}
	s, err := NewStorage(dir, c)
	if err != nil {
		t.Fatal(err)
	}

	// 100条
	var lo uint64 = 1
	hi := uint64(100 + 1)
	ents := genLogEntries(lo, hi)
	start := time.Now()
	for i := 0; i < int(hi-lo); i++ {
		if err = s.StoreEntries(ents[i : i+1]); err != nil {
			t.Fatal(err)
		}
	}

	t.Logf("write %d entries sync nodelay take: %s", hi-lo, time.Since(start).String())
	s.Close()

	c = &Config{
		FileSize: 1024 * 1024 * 10,
	}
	s, err = NewStorage(dir, c)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Truncate(hi - 1); err != nil {
		t.Fatal(err)
	}
	start = time.Now()
	for i := 0; i < int(hi-lo); i++ {
		if err = s.StoreEntries(ents[i : i+1]); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("write %d entries sync delayed take: %s", hi-lo, time.Since(start).String())
	s.Close()
}
