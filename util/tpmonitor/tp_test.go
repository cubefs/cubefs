package tpmonitor

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestTpMonitor(t *testing.T) {
	tp := NewTpMonitor()
	now := time.Now()
	for i := 0; i < 100; i++ {
		tp.Accumulate(i, now)
	}
	max, avg, tp99 := tp.CalcTp()
	t.Logf("%d\t%d\t%d", max, avg, tp99)
	//assert.Equal(t, 100, tp99)
	//assert.Equal(t, 99, max)
	//assert.Equal(t, 99/2, avg)

	for i := 0; i < 1000; i++ {
		tp.Accumulate(i, now)
	}
	max, avg, tp99 = tp.CalcTp()
	t.Logf("%d\t%d\t%d", max, avg, tp99)
	//assert.Equal(t, 989, tp99)
	//assert.Equal(t, 999, max)
	//assert.Equal(t, 999/2, avg)

	for i := 0; i < 990; i++ {
		tp.Accumulate(0, now)
	}
	for i := 990; i < 1000; i++ {
		tp.Accumulate(120 * 1000, now)
	}
	max, avg, tp99 = tp.CalcTp()
	t.Logf("%d\t%d\t%d", max, avg, tp99)

	//assert.Equal(t, 0, tp99)
	//assert.Equal(t, 120*1000, max)
	//assert.Equal(t, 120*1000*10/1000, avg)
	now = time.Now()
	for i := 0; i < 989; i++ {
		tp.Accumulate(0, now)
	}
	for i := 989; i < 1000; i++ {
		tp.Accumulate(120 * 1000, now)
	}
	now2 := time.Now()

	t.Logf("%d\t%d\t%d, cost:%v, cal:%v", max, avg, tp99, time.Since(now), time.Since(now2))
	//assert.Equal(t, 60*1000, tp99)
	//assert.Equal(t, 120*1000, max)
	//assert.Equal(t, 120*1000*11/1000, avg)
}

func TestTpMonitorCost(t *testing.T) {
	tp := NewTpMonitor()
	var wg sync.WaitGroup
	tot := int64(0)
	delayArray := make([][]int, 0)
	rand.Seed(time.Now().Unix())
	goCount := 1000

	for index := 0; index < goCount; index++ {
		delayArray = append(delayArray, make([]int, 0))
		for i := 0; i < 1000; i++ {
			delayArray[index] = append(delayArray[index], rand.Int() % CostStep100sEnd + 1)
		}
	}

	for cnt := 0; cnt < goCount; cnt++ {
		wg.Add(1)
		go func(index int) {
			now := time.Now()
			curDelArr := delayArray[index]
			for i := 0; i < 1000; i++ {
				tp.Accumulate(curDelArr[i], now)
			}
			cost := time.Since(now)
			atomic.AddInt64(&tot, cost.Microseconds())
			wg.Done()
		}(cnt)
	}
	wg.Wait()
	max, avg, tp99 := tp.CalcTp()
	t.Logf("\ncost:%v\ncal:max:%v avg:%v tp99:%v\n", tot, max, avg, tp99)
}