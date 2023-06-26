package main

import (
	"sync"
	"testing"
	"time"
)

var (
	putCount = 3200
	dataSize = 16 * 1024
	groupNum = 10
)

func TestPutBigValue(t *testing.T) {
	tests := []RaftTestConfig{
		{
			name: "putBigValue_default",
			mode: StandardMode,
		},
		{
			name: "putBigValue_strict",
			mode: StrictMode,
		},
		{
			name: "putBigValue_mix",
			mode: MixMode,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			putBigValue(t, tt.name, tt.mode)
		})
	}
}

func putBigValue(t *testing.T, testName string, mode RaftMode) {
	servers := initTestServer(peers, true, true, groupNum, mode)
	f, w := getLogFile("", testName+".log")
	time.Sleep(time.Second)

	defer func() {
		w.Flush()
		f.Close()
		// end
		for _, s := range servers {
			s.raft.Stop()
		}
		dataType = 0
	}()

	leaderMap := make(map[uint64]*testServer)
	for i := 1; i <= groupNum; i++ {
		leaderMap[uint64(i)] = waitElect(servers, uint64(i), w)
	}
	dataType = 1

	// 10 partitions: put big data
	var wg sync.WaitGroup
	wg.Add(groupNum)
	for i := 1; i <= groupNum; i++ {
		go func(raftId uint64) {
			defer wg.Done()
			leadServer := leaderMap[raftId]
			for n := 0; n < putCount; n++ {
				if err := leadServer.putOneBigData(raftId, dataSize); err != nil {
					output("raft[%v] put data fail: %v", raftId, err)
					leadServer = waitElect(servers, raftId, w)
				}
			}
		}(uint64(i))
	}
	time.Sleep(1 * time.Second)
	index := 0
	for i := 1; i <= groupNum; i++ {
		s := servers[index]
		s.raft.TryToLeader(uint64(i))
		index++
		if index == 3 {
			index = 0
		}
	}
	wg.Wait()
	printStatus(servers, w)
}
