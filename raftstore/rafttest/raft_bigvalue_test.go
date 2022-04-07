package main

import (
	"fmt"
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
	servers := initTestServer(peers, true, false, groupNum)
	f, w := getLogFile("", "putBigValue.log")
	time.Sleep(time.Second)

	defer func() {
		w.Flush()
		f.Close()
		// end
		for _, s := range servers {
			s.raft.Stop()
		}
	}()

	leaderMap := make(map[uint64]*testServer)
	for i := 1; i <= groupNum; i++ {
		leaderMap[uint64(i)] = waitElect(servers, uint64(i), w)
	}
	dataType = 1

	// 50 partitions: put big data
	var wg sync.WaitGroup
	wg.Add(groupNum)
	for i := 1; i <= groupNum; i++ {
		go func(raftId uint64) {
			defer wg.Done()
			leadServer := leaderMap[raftId]
			for n := 0; n < putCount; n++ {
				if err := leadServer.putOneBigData(raftId, dataSize); err != nil {
					fmt.Printf("raft[%v] put data fail: %v\n", raftId, err)
					leadServer = waitElect(servers, raftId, w)
				}
			}
		}(uint64(i))
	}
	time.Sleep(5 * time.Second)
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
