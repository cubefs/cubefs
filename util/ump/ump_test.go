package ump

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/chubaofs/chubaofs/proto"
)

func init() {
	InitUmp("datanode", "jdos_chubao-node")
	checkUmpWaySleepTime = 100 * time.Millisecond
	writeTpSleepTime = time.Millisecond
	aliveTickerTime = time.Millisecond
	alarmTickerTime = time.Millisecond
}

//func BenchmarkAfterTPUsOld(b *testing.B) {
//	var wg sync.WaitGroup
//	b.ResetTimer()
//	for i := 0; i < 10000; i++ {
//		wg.Add(1)
//		go parallelUmpWrite(b, &wg)
//	}
//	wg.Wait()
//
//}
//
//func parallelUmpWrite(b *testing.B, wg *sync.WaitGroup) {
//	key := fmt.Sprintf("datanode_write")
//	for i := 0; i < b.N; i++ {
//		o := BeforeTP(key)
//		AfterTPUsOld(o, nil)
//	}
//	wg.Done()
//}
//
//func BenchmarkAfterTPUsOldGroupBy(b *testing.B) {
//	var wg sync.WaitGroup
//	b.ResetTimer()
//	for i := 0; i < 1000; i++ {
//		wg.Add(1)
//		go parallelUmpWriteGroupBy(b, &wg)
//	}
//	wg.Wait()
//
//}
//
//func parallelUmpWriteGroupBy(b *testing.B, wg *sync.WaitGroup) {
//	key := fmt.Sprintf("datanode_write")
//	for i := 0; i < b.N; i++ {
//		o := BeforeTP(key)
//		AfterTPUsOld(o, nil)
//	}
//	wg.Done()
//}

func BenchmarkAfterTPUsGroupByV629(b *testing.B) {
	var wg sync.WaitGroup
	b.ResetTimer()
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go parallelUmpWriteGroupByV629(b, &wg)
	}
	wg.Wait()
	//time.Sleep(2 * time.Second)
}

func parallelUmpWriteGroupByV629(b *testing.B, wg *sync.WaitGroup) {
	var key string
	writeKey := fmt.Sprintf("datanode_write")
	readKey := fmt.Sprintf("datanode_read")
	for i := 0; i < b.N; i++ {
		if i%2 != 0 {
			key = readKey
		} else {
			key = writeKey
		}
		o := BeforeTP(key)
		AfterTPUs(o, nil)
	}
	wg.Done()
}

func BenchmarkSystemAliveByV629(b *testing.B) {
	var wg sync.WaitGroup
	b.ResetTimer()
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go parallelUmpWriteSystemAliveV629(b, &wg)
	}
	wg.Wait()
	//time.Sleep(25 * time.Second)
}

func parallelUmpWriteSystemAliveV629(b *testing.B, wg *sync.WaitGroup) {
	var key string
	writeKey := fmt.Sprintf("datanode_write")
	readKey := fmt.Sprintf("datanode_read")
	for i := 0; i < b.N; i++ {
		if i%2 != 0 {
			key = readKey
		} else {
			key = writeKey
		}
		Alive(key)
	}
	wg.Done()
}

func BenchmarkBusinessAlarmV629(b *testing.B) {
	var wg sync.WaitGroup
	b.ResetTimer()
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go parallelUmpWriteBusinessAlarmV629(b, &wg)
	}
	wg.Wait()
	//time.Sleep(2 * time.Second)
}

func parallelUmpWriteBusinessAlarmV629(b *testing.B, wg *sync.WaitGroup) {
	var key string
	writeKey := fmt.Sprintf("dbbak_master_warning")
	readKey := fmt.Sprintf("spark_master_warning")
	for i := 0; i < b.N; i++ {
		if i%2 != 0 {
			key = readKey
		} else {
			key = writeKey
		}
		Alarm(key, "heartbeat failed")
	}
	wg.Done()
}

func TestUmp(t *testing.T) {
	sendUmp()

	SetUmpJmtpAddr(testJmtpAddr)
	SetUmpCollectWay(proto.UmpCollectByJmtpClient)
	sendUmp()

	SetUmpCollectWay(proto.UmpCollectByFile)
	sendUmp()
}

func sendUmp() {
	count := 100
	for i := 0; i < count; i++ {
		tpObject := BeforeTP(fmt.Sprintf("tp key %d", i))
		AfterTP(tpObject, nil)
		Alive(fmt.Sprintf("alive key %d", i))
		Alarm(fmt.Sprintf("alarm key %d", i), fmt.Sprintf("alarm detail %d", i))
	}
	time.Sleep(time.Duration(count) * writeTpSleepTime)
	FlushAlarm()
}
