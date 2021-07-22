package ump

import (
	"fmt"
	"sync"
	"testing"
)

func init() {
	InitUmp("datanode")
}

func BenchmarkAfterTPUs(b *testing.B) {
	var wg sync.WaitGroup
	b.ResetTimer()
	for i := 0; i < 10000; i++ {
		wg.Add(1)
		go parallelUmpWrite(b, &wg)
	}
	wg.Wait()

}

func parallelUmpWrite(b *testing.B, wg *sync.WaitGroup) {
	key := fmt.Sprintf("datanode_write")
	for i := 0; i < b.N; i++ {
		o := BeforeTP(key)
		AfterTPUs(o, nil)
	}
	wg.Done()
}

func BenchmarkAfterTPUsGroupBy(b *testing.B) {
	var wg sync.WaitGroup
	b.ResetTimer()
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go parallelUmpWriteGroupBy(b, &wg)
	}
	wg.Wait()

}

func parallelUmpWriteGroupBy(b *testing.B, wg *sync.WaitGroup) {
	key := fmt.Sprintf("datanode_write")
	for i := 0; i < b.N; i++ {
		o := BeforeTP(key)
		AfterTPUs(o, nil)
	}
	wg.Done()
}
