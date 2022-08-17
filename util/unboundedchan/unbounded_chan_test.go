package unboundedchan

import (
	"sync"
	"testing"
)

func TestWriteReadUnboundedChan(t *testing.T) {
	//concurrent write and read unbounded chan
	UChan := NewUnboundedChan(10)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			t.Logf("In <- %v", i)
			UChan.In <- i
		}(i)
	}

	go func() {
		var total int
		values := make(map[int]int)
		for v := range UChan.Out {
			val, ok := v.(int)
			t.Logf("val(%v), ok(%v)", val, ok)
			v1, ok := values[val]
			if ok {
				t.Fatalf("value(%v) is not expected to be in UnboundedChan multiple times", v1)
			} else {
				values[val] = val
			}
			if v1 >= 100 || v1 < 0 {
				t.Fatalf("value(%v) is expected in range 0-99", v1)
			}
			total++
		}
		if total != 100 {
			t.Fatalf("expected total num(%v), got(%v)", 100, total)
		}
		t.Logf("total: %v", total)
	}()

	wg.Wait()
	close(UChan.In)

}
