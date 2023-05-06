package metanode

import (
	"testing"
	"time"
)

func TestGetCurrentTimeUnix(t *testing.T) {
	t1 := Now.GetCurrentTimeUnix()
	t2 := Now.GetCurrentTime().Unix()
	n := time.Now().Unix()
	if n-t1 > 1 || n-t2 > 1 {
		t.Errorf("wrong time: %d %d %d", n, t1, t2)
	}

	time.Sleep(time.Second * 2)
	t1 = Now.GetCurrentTimeUnix()
	t2 = Now.GetCurrentTime().Unix()
	n = time.Now().Unix()
	if n-t1 > 1 || n-t2 > 1 {
		t.Errorf("wrong time: %d %d %d", n, t1, t2)
	}
}

func BenchmarkGetCurrentTimeUnix(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Now.GetCurrentTimeUnix()
	}
}

func BenchmarkGetCurrentTime(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Now.GetCurrentTime().Unix()
	}
}

func BenchmarkGetNowTime(b *testing.B) {
	for i := 0; i < b.N; i++ {
		time.Now().Unix()
	}
}
