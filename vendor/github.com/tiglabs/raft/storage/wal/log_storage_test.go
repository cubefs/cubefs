package wal

import (
	"context"
	"math"
	"os"
	"path"
	"testing"

	"github.com/tiglabs/raft/proto"
)

func TestLogEntryStorage_AsyncRotate(t *testing.T) {
	var err error
	var testPath = path.Join(os.TempDir(), "test_log_entry_storage_async_rotate")
	_ = os.RemoveAll(testPath)
	if err = os.MkdirAll(testPath, os.ModePerm); err != nil {
		t.Fatalf("prepare test path fail: %v", err)
	}

	var ls *logEntryStorage
	if ls, err = openLogStorage(testPath, &Storage{c: &Config{SyncRotate: false}}); err != nil {
		t.Fatalf("open log storage fail: %v", err)
	}

	defer ls.Close()

	const term uint64 = 1
	const startI uint64 = 1
	const endI uint64 = 10005
	for index := startI; index <= endI; index++ {
		e := &proto.Entry{Index: index, Term: term, Data: make([]byte, 1024*4)}
		if index%2000 == 0 {
			if err = ls.rotate(nil); err != nil {
				t.Fatalf("rotate fail: %v", err)
			}
		}
		if err = ls.saveEntry(context.Background(), e); err != nil {
			t.Fatalf("save entry [index:%v] fail: %v", index, err)
		}
		if err = ls.last.Flush(context.Background()); err != nil {
			t.Fatalf("flush last entry file fail: %v", err)
		}
		if _, _, err = ls.Entries(index, index+1, 1); err != nil {
			t.Fatalf("read entries [%v, %v) fail: %v", index, index+1, err)
		}
	}

	var entries []*proto.Entry
	if entries, _, err = ls.Entries(startI, endI+1, math.MaxUint64); err != nil {
		t.Fatalf("read entries fail: %v", err)
	}
	if len(entries) != int(endI-startI+1) {
		t.Fatalf("entries count mismatch: expect %v, actual %v", int(endI-startI+1), len(entries))
	}
}

func TestLogEntryStorage_AsyncRotateWithTruncateBack(t *testing.T) {
	var err error
	var testPath = path.Join(os.TempDir(), "test_log_entry_storage_async_rotate_with_truncate_back")
	_ = os.RemoveAll(testPath)
	if err = os.MkdirAll(testPath, os.ModePerm); err != nil {
		t.Fatalf("prepare test path fail: %v", err)
	}

	var ls *logEntryStorage
	if ls, err = openLogStorage(testPath, &Storage{c: &Config{SyncRotate: false}}); err != nil {
		t.Fatalf("open log storage fail: %v", err)
	}

	defer ls.Close()

	const term uint64 = 1
	const startI uint64 = 1
	const endI uint64 = 10000
	var data = make([]byte, 1024)
	for index := startI; index <= endI; index++ {
		e := &proto.Entry{Index: index, Term: term, Data: data}
		if err = ls.saveEntry(context.Background(), e); err != nil {
			t.Fatalf("save entry [index:%v] fail: %v", index, err)
		}
	}
	if err = ls.rotate(context.Background()); err != nil {
		t.Fatalf("rotate fail: %v", err)
	}
	if err = ls.truncateBack(endI - 2); err != nil {
		t.Fatalf("truncate back fail: %v", err)
	}
	if err = ls.saveEntry(context.Background(), &proto.Entry{Index: endI - 2, Term: term, Data: data}); err != nil {
		t.Fatalf("save entry [index:%v] fail: %v", endI-2, err)
	}
}
