package cfs

import (
	"testing"
	"time"
)

func TestCheckThenRestartNode(t *testing.T) {
	s := &ChubaoFSMonitor{}
	highLoadNodeSolver := &ChubaoFSHighLoadNodeSolver{
		DBConfigDSN: "root:1qaz@WSX@tcp(11.13.125.198:80)/storage_sre?charset=utf8mb4&parseTime=True&loc=Local",
	}
	go highLoadNodeSolver.startChubaoFSHighLoadNodeSolver()
	if highLoadNodeSolver != nil {
		s.RestartNodeMaxCountIn24Hour = 1
		s.highLoadNodeSolver = highLoadNodeSolver
	}
	time.Sleep(time.Second)
	err := s.checkThenRestartNode("192.168.1.1:6000", "test.com")
	if err != nil {
		t.Log(err)
	}
}

func TestCheckThenRestartNodeWithNilDB(t *testing.T) {
	s := &ChubaoFSMonitor{}
	highLoadNodeSolver := &ChubaoFSHighLoadNodeSolver{}
	go highLoadNodeSolver.startChubaoFSHighLoadNodeSolver()
	if highLoadNodeSolver != nil {
		s.RestartNodeMaxCountIn24Hour = 1
		s.highLoadNodeSolver = highLoadNodeSolver
	}
	s.checkThenRestartNode("192.168.1.1:6000", "test.com")
}
