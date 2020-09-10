package raftstore

import (
	"os"
	"path"
	"testing"
	"time"
)

func TestCleanRaftLog(t *testing.T) {
	dir := path.Join("/tmp/raft", "logs")
	_, err := os.Stat(dir)
	if os.IsNotExist(err) {
		os.MkdirAll(dir, 0755)
	}
	logFilePath := path.Join(dir, "test_clean.log")
	_, err = os.Create(logFilePath)
	if err != nil {
		t.Errorf("create file[%v] err[%v]", logFilePath, err)
		return
	}
	info, err := os.Stat(logFilePath)
	if err != nil {
		t.Errorf("stat file[%v] err[%v]", logFilePath, err)
		return
	}
	err = os.Chtimes(logFilePath, info.ModTime().AddDate(0, 0, -7), info.ModTime().AddDate(0, 0, -7))
	if err != nil {
		t.Errorf("change file[%v] modify time err[%v]", logFilePath, err)
		return
	}
	newRaftLogger("/tmp/raft")
	time.Sleep(time.Second * 10)
	_, err = os.Stat(logFilePath)
	if !os.IsNotExist(err) {
		t.Errorf("expect file doesn't exist but err is [%v]", err)
		return
	}
}
