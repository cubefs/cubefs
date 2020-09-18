package raftstore

import (
	"os"
	"path"
	"testing"
	"time"
)

// test auto remove raft log 7 days ago
func TestCleanRaftLog(t *testing.T) {
	dir := path.Join("/tmp/raft", "logs")
	_, err := os.Stat(dir)
	if os.IsNotExist(err) {
		os.MkdirAll(dir, 0755)
	}

	logFilePath1 := path.Join(dir, "test_clean.log.old")
	if err = createFile(logFilePath1, true); err != nil {
		t.Errorf("create file[%v] err[%v]", logFilePath1, err)
		return
	}
	logFilePath2 := path.Join(dir, "test_clean.log")
	if err = createFile(logFilePath2, true); err != nil {
		t.Errorf("create file[%v] err[%v]", logFilePath2, err)
		return
	}
	logFilePath3 := path.Join(dir, "test_clean.log.new")
	if err = createFile(logFilePath3, false); err != nil {
		t.Errorf("create file[%v] err[%v]", logFilePath3, err)
		return
	}

	newRaftLogger("/tmp/raft")
	time.Sleep(time.Second * 10)

	_, err = os.Stat(logFilePath1)
	if !os.IsNotExist(err) {
		t.Errorf("expect file[%v] doesn't exist but err is [%v]", logFilePath1, err)
		return
	}
	_, err = os.Stat(logFilePath2)
	if err != nil {
		t.Errorf("expect file[%v] exists but err is [%v]", logFilePath2, err)
		return
	}
	_, err = os.Stat(logFilePath3)
	if err != nil {
		t.Errorf("expect file[%v] exists but err is [%v]", logFilePath3, err)
		return
	}

	os.RemoveAll(dir)
}

// create file and modify modTime to 7 days ago
func createFile(logFilePath string, modTime bool) (err error) {
	_, err = os.Create(logFilePath)
	if err != nil {
		return
	}
	info, err := os.Stat(logFilePath)
	if err != nil {
		return
	}
	if modTime {
		err = os.Chtimes(logFilePath, info.ModTime().AddDate(0, 0, -7), info.ModTime().AddDate(0, 0, -7))
		if err != nil {
			return
		}
	}
	return
}
