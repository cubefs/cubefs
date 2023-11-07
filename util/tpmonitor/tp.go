package tpmonitor

import (
	"sort"
	"sync"
)

const (
	percent90  = (100.0 - 90) / 100
	percent99  = (100.0 - 99) / 100
	percent999 = (100.0 - 99.9) / 100
)

var scale = make([]int, 2400)

type TpResult struct {
	Max  int
	Avg  int
	Tp99 int
}

type TpMonitor struct {
	sync.Mutex
	max            int
	sum            int
	count          int
	countContainer []int
}

func init() {
	for i := 0; i < 1000; i++ {
		scale[i] = i
	}
	for i := 1000; i < 1900; i++ {
		scale[i] = 1000 + 10*(i-999)
	}
	for i := 1900; i < 2400; i++ {
		scale[i] = 10000 + 100*(i-1899)
	}
}

//NewTpMonitor
//unit: millisecond
//min: 0s
//max: 60s
//step=1: 0-999 ms
//step=10: 1010-10000 ms
//step=100: 10100-60000 ms
func NewTpMonitor() (monitor *TpMonitor) {
	monitor = &TpMonitor{
		countContainer: make([]int, 2400),
	}
	return monitor
}

func (monitor *TpMonitor) Accumulate(milliseconds int) {
	monitor.Lock()
	defer monitor.Unlock()
	monitor.sum += milliseconds
	if milliseconds > monitor.max {
		monitor.max = milliseconds
	}
	monitor.count++
	index := monitor.locateDelayIndex(milliseconds)
	if index > len(monitor.countContainer)-1 {
		index = len(monitor.countContainer) - 1
	}
	monitor.countContainer[index]++
}

func (monitor *TpMonitor) TpReset() TpResult {
	if monitor.count <= 0 {
		return TpResult{}
	}
	monitor.Lock()
	defer monitor.Unlock()
	defer func() {
		monitor.countContainer = make([]int, 2400)
		monitor.count = 0
		monitor.sum = 0
		monitor.max = 0
	}()
	position := monitor.adjust(int(float64(monitor.count)*percent99), monitor.count)
	scanned := 0
	length := len(monitor.countContainer)
	for index := length - 1; index >= 0; index-- {
		if 0 == monitor.countContainer[index] {
			continue
		}
		scanned += monitor.countContainer[index]
		if scanned > position {
			return TpResult{
				Max:  monitor.max,
				Avg:  monitor.sum / monitor.count,
				Tp99: scale[index],
			}
		}
	}
	return TpResult{}
}

func (monitor *TpMonitor) adjust(input, max int) int {
	if input <= 1 {
		return 1
	} else if input >= max {
		return max
	} else {
		return input
	}
}

func (monitor *TpMonitor) locateDelayIndex(delay int) int {
	if delay > scale[len(scale)-1] {
		return len(scale) - 1
	} else if delay < scale[0] {
		return 0
	}
	return sort.SearchInts(scale, delay)
}
