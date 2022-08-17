package routinepool

import (
	"sync"
	"testing"
)

func TestExample(t *testing.T) {
	maxRoutineNum := 10
	pool := NewRoutinePool(maxRoutineNum)

	var max int32 = 0
	totalRoutineNum := 100
	lock := &sync.Mutex{}

	value := 0

	for i := 0; i < totalRoutineNum; i++ {
		pool.Submit(func() {
			currentNum := pool.RunningNum()
			lock.Lock()
			value++
			if currentNum > max {
				max = currentNum
			}
			lock.Unlock()
		})
	}

	pool.WaitAndClose()

	if max != int32(maxRoutineNum) {
		t.Fatalf("expected max running num(%v), got(%v)", maxRoutineNum, max)
	}

	if value != totalRoutineNum {
		t.Fatalf("expected value(%v), got(%v)", totalRoutineNum, value)
	}
}
