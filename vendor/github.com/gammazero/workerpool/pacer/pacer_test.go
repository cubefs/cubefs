package pacer

import (
	"sync"
	"testing"
	"time"

	"github.com/gammazero/workerpool"
)

func TestPacedWorkers(t *testing.T) {
	t.Parallel()

	delay1 := 100 * time.Millisecond
	delay2 := 300 * time.Millisecond
	wp := workerpool.New(5)
	defer wp.Stop()

	pacer := NewPacer(delay1)
	defer pacer.Stop()

	slowPacer := NewPacer(delay2)
	defer slowPacer.Stop()

	tasksDone := new(sync.WaitGroup)
	tasksDone.Add(20)
	start := time.Now()

	pacedTask := pacer.Pace(func() {
		//fmt.Println("Task")
		tasksDone.Done()
	})

	slowPacedTask := slowPacer.Pace(func() {
		//fmt.Println("SlowTask")
		tasksDone.Done()
	})

	// Cause worker to be created, and available for reuse before next task.
	for i := 0; i < 10; i++ {
		wp.Submit(pacedTask)
		wp.Submit(slowPacedTask)
	}

	time.Sleep(500 * time.Millisecond)
	pacer.Pause()
	time.Sleep(time.Second)
	if !pacer.IsPaused() {
		t.Fatal("should be paused")
	}
	pacer.Resume()
	if pacer.IsPaused() {
		t.Fatal("should not be paused")
	}

	tasksDone.Wait()
	elapsed := time.Since(start)
	// 9 times delay2 since no wait for first task, and pacer and slowPacer run
	// currently so only limiter is slowPacer.
	if elapsed < 9*delay2 {
		t.Fatal("Did not pace tasks correctly - finished too soon:", elapsed)
	}
}
