package main

import (
	"testing"
	"time"
)



func Benchmark_Raft(b *testing.B) {
	servers := initTestServer(peers, true, true, 1)
	f, w := getLogFile("/tmp", "raftBenchmark.log")
	defer func() {
		w.Flush()
		f.Close()
		// end
		for _, s := range servers {
			s.raft.Stop()
		}
		dataType = 0
	}()

	leadServer := waitElect(servers, 1, w)
	printStatus(servers, w)
	time.Sleep(time.Second)

	dataType = 1
	storageType = 3
	dataLen := 1024 * 16
	dataBytes := make([]byte, dataLen)
	for i := 0; i < dataLen; i++ {
		dataBytes[i] = '1'
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp := leadServer.raft.Submit(nil, 1, dataBytes)
		_, err := resp.Response()
		if err != nil {
			b.Fatalf("Benchmark_Raft: err(%v)", err)
		}
	}
	b.ReportAllocs()
}
