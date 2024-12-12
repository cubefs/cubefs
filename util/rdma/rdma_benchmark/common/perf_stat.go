package common

import (
	"fmt"
	"sync/atomic"
	"time"
)

type NetPerfStat struct {
	num    int64
	size   int64
	start  int64
	tmCost int64
}

var stat = NetPerfStat{}

func Stat() *NetPerfStat {
	return &stat
}

func (s *NetPerfStat) AddSum(size int) {
	atomic.AddInt64(&s.num, 1)
	atomic.AddInt64(&s.size, int64(size)*2) // send and recv
	if atomic.LoadInt64(&s.start) == 0 {    // Although there may be a potential race condition, it only occurs when start = 0, so I won't implement a lock here.
		atomic.StoreInt64(&s.start, time.Now().Unix())
	}
}

func (s *NetPerfStat) AddSumTime(size int, tm int64) {
	s.AddSum(size)
	atomic.AddInt64(&s.tmCost, tm)
}
func (s *NetPerfStat) Print() {
	tmCost := time.Now().Unix() - atomic.LoadInt64(&s.start)
	//io ps  band w
	num := atomic.LoadInt64(&s.num)
	totaltmCost := atomic.LoadInt64(&s.tmCost)
	size := atomic.LoadInt64(&s.size)

	fmt.Printf("IOPS=[%.2f], TOTAL_BPS=[%.2fM], AVG_TM=[%.2fus]\n", float64(num)/float64(tmCost),
		float64(size)/(1024*1024)/float64(tmCost), float64(totaltmCost)/float64(s.num))
}
