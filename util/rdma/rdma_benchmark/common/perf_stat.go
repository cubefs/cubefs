package common

import (
	"fmt"
	"time"
)

type NetPerfStat struct {
	num int64
	size int64
	start int64
	tmCost int64
}

var stat = NetPerfStat{}

func Stat() *NetPerfStat {
	return &stat
}

func (s *NetPerfStat)AddSum(size int) {
	s.num++
	s.size += 2*int64(size) //send and recv
	if s.start == 0 {
		s.start = time.Now().Unix()
	}
}

func (s *NetPerfStat)AddSumTime(size int, tm int64) {
	s.AddSum(size)
	s.tmCost += tm
}
func (s *NetPerfStat)Print() {
	tmCost := time.Now().Unix() - s.start
	//io ps  band w
	fmt.Printf("IOPS=[%.2f], TOTAL_BPS=[%.2fM], AVG_TM=[%.2fus]\n", float64(s.num)/float64(tmCost),
		float64(s.size)/(1024*1024)/float64(tmCost), float64(s.tmCost)/float64(s.num))
}


