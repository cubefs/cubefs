package exporter

import (
	"testing"
	"fmt"
)

func TestRegistGauge(t *testing.T) {
	N := 100
	exitCh := make(chan int, 100)
	for i:=0; i<N; i++ {
		go func() {
			m := RegistGauge(fmt.Sprintf("name_%d", i%7))
			if m != nil {
				t.Logf("metric: %v", m.Desc().String())
			}
			exitCh <- i

		} ()
	}

	x := 0
	select {
	case  <- exitCh:
		x += 1
		if x == N {
			return
		}
	}
}


func TestRegistTp(t *testing.T) {
	N := 100
	exitCh := make(chan int, 100)
	for i:=0; i<N; i++ {
		go func() {
			m := RegistTp(fmt.Sprintf("name_%d", i%7))
			if m != nil {
				t.Logf("metric: %v", m.metricName)
			}

			defer m.CalcTp()

			exitCh <- i
		} ()
	}

	x := 0
	select {
	case  <- exitCh:
		x += 1
		if x == N {
			return
		}
	}


}