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

func TestLogEntryStorage_TruncateAll(t *testing.T) {
	var err error
	var testPath = path.Join(os.TempDir(), "TestLogEntryStorage_TruncateAll")
	_ = os.RemoveAll(testPath)
	if err = os.MkdirAll(testPath, os.ModePerm); err != nil {
		t.Fatalf("prepare test path fail: %v", err)
	}
	var ls *logEntryStorage
	if ls, err = openLogStorage(testPath, &Storage{c: &Config{SyncRotate: true}}); err != nil {
		t.Fatalf("open log storage fail: %v", err)
	}
	defer ls.Close()

	var data = make([]byte, 1024)
	for i := uint64(1); i <= 2; i++ {
		if err = ls.saveEntry(context.Background(), &proto.Entry{Index: i, Term: 1, Data: data}); err != nil {
			t.Fatalf("save entry [index: 1, term: 1] fail: %v", err)
		}
	}

	if err = ls.rotate(context.Background()); err != nil {
		t.Fatalf("rotate fail: %v", err)
	}

	var li, lt uint64
	li = ls.LastIndex()
	if lt, _, err = ls.Term(li); err != nil {
		t.Fatalf("get term for last index %v fail: %v", li, err)
	}
	if li != 2 || lt != 1 {
		t.Fatalf("last index or term mismatch, expect [index: 2, term: 1], actual [index: %v, term: %v]", li, lt)
	}

	if err = ls.TruncateAll(); err != nil {
		t.Fatalf("truncate all fail: %v", err)
	}

	for i := uint64(1); i <= 3; i++ {
		if err = ls.saveEntry(context.Background(), &proto.Entry{Index: i, Term: 2, Data: data}); err != nil {
			t.Fatalf("save entry [index: 1, term: 1] fail: %v", err)
		}
	}

	if err = ls.rotate(context.Background()); err != nil {
		t.Fatalf("rotate fail: %v", err)
	}

	li = ls.LastIndex()
	if lt, _, err = ls.Term(li); err != nil {
		t.Fatalf("get term for last index %v fail: %v", li, err)
	}
	if li != 3 || lt != 2 {
		t.Fatalf("last index or term mismatch, expect [index: 3, term: 2], actual [index: %v, term: %v]", li, lt)
	}

}

func TestLogEntryStorage_OpenWithBrokenFiles(t *testing.T) {
	var err error
	var testPath = path.Join(os.TempDir(), "TestLogEntryStorage_OpenWithBrokenFiles")
	t.Log(testPath)
	_ = os.RemoveAll(testPath)
	if err = os.MkdirAll(testPath, os.ModePerm); err != nil {
		t.Fatalf("prepare test path fail: %v", err)
	}
	var ls *logEntryStorage
	if ls, err = openLogStorage(testPath, &Storage{c: &Config{SyncRotate: true}}); err != nil {
		t.Fatalf("open log storage fail: %v", err)
	}

	// Step 1. 初始化数据
	// 初始化后的数据目录如下:
	// -rw-------  1 zhangmofei  staff   161K Aug 25 14:29 0000000000000001-0000000000000001.log
	// -rw-------  1 zhangmofei  staff   161K Aug 25 14:29 0000000000000002-00000000000009c5.log
	// -rw-------  1 zhangmofei  staff   161K Aug 25 14:29 0000000000000003-0000000000001389.log
	// -rw-------  1 zhangmofei  staff   161K Aug 25 14:29 0000000000000004-0000000000001d4d.log
	// -rw-------  1 zhangmofei  staff     0B Aug 25 14:29 0000000000000005-0000000000002711.log
	const (
		term   uint64 = 1
		startI uint64 = 1
		endI   uint64 = 10000
	)

	var data = make([]byte, 16)
	for index := startI; index <= endI; index++ {
		e := &proto.Entry{Index: index, Term: term, Data: data}
		if err = ls.saveEntry(context.Background(), e); err != nil {
			t.Fatalf("save entry [index:%v] fail: %v", index, err)
		}
		if index%2500 == 0 {
			if err = ls.rotate(context.Background()); err != nil {
				t.Fatalf("rotate fail: %v", err)
			}
		}
	}

	ls.Close()

	// Step 2. 破坏导数第二个文件(0000000000000004-0000000000001d4d.log)的数据, 导致第二个文件数据丢失。
	// 破坏后的数据目录如下所示:
	// -rw-------  1 zhangmofei  staff   161K Aug 25 14:29 0000000000000001-0000000000000001.log
	// -rw-------  1 zhangmofei  staff   161K Aug 25 14:29 0000000000000002-00000000000009c5.log
	// -rw-------  1 zhangmofei  staff   161K Aug 25 14:29 0000000000000003-0000000000001389.log
	// -rw-------  1 zhangmofei  staff    81K Aug 25 14:29 0000000000000004-0000000000001d4d.log  <- 被破坏的文件
	// -rw-------  1 zhangmofei  staff     0B Aug 25 14:29 0000000000000005-0000000000002711.log
	const brokenFileName = "0000000000000004-0000000000001d4d.log"
	var f *os.File
	if f, err = os.OpenFile(path.Join(testPath, brokenFileName), os.O_RDWR, 0666); err != nil {
		t.Fatalf("open file failed: %v", err)
	}
	var finfo os.FileInfo
	if finfo, err = f.Stat(); err != nil {
		t.Fatalf("get file stat failed: %v", err)
	}
	if err = f.Truncate(finfo.Size() / 2); err != nil {
		t.Fatalf("truncate file failed: %v", err)
	}
	_ = f.Close()

	// Step 3. 在此打开数据目录，验证log storage是否发现数据目录存在损坏.
	if ls, err = openLogStorage(testPath, &Storage{c: &Config{SyncRotate: true}}); err == nil {
		t.Fatalf("unecpected result on open storage with broken log files")
	}
}
