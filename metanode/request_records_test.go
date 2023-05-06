package metanode

import (
	"reflect"
	"sort"
	"testing"
	"time"
)

func TestRequestRecords(t *testing.T) {
	batchRequestInfo := genBatchRequestInfo(1000, true)
	reqRecords := InitRequestRecords(batchRequestInfo)
	if _, isDup := reqRecords.IsDupReq(batchRequestInfo[50]); !isDup {
		t.Errorf("expect: is dup req")
		return
	}

	for index := 0; index < 200; index++ {
		reqRecords.Update(genRequestInfo(true))
		time.Sleep(time.Millisecond * 10)
	}

	reqRecordMaxCount.Store(500)
	reqRecordReserveMin.Store(1)

	timestamp := reqRecords.GetEvictTimestamp()
	reqRecords.EvictByTime(timestamp)
	if reqRecords.Count() > 500 {
		t.Errorf("req record count error, expect:less than 500, actual:%v", reqRecords.Count())
		return
	}
}

func TestRequestRecords_Update(t *testing.T) {
	batchRequestInfo := genBatchRequestInfo(20, true)
	reqRecords := InitRequestRecords(batchRequestInfo)

	batchRequestInfo = genBatchRequestInfo(10, true)

	for index := 0; index < 200; index++ {
		reqRecords.Update(genRequestInfo(true))
		time.Sleep(time.Millisecond * 10)
	}

	for _, reqInfo := range batchRequestInfo {
		reqRecords.Update(reqInfo)
	}

	aggResult := reqRecords.AggRequestsByRequestTime()
	tArr := make([]int64, 0, len(aggResult))
	for timeStamp := range aggResult {
		tArr = append(tArr, timeStamp)
	}
	sort.Slice(tArr, func(i, j int) bool {
		return tArr[i] < tArr[j]
	})

	cnt := 0
	index := 0
	reqRecords.RangeList(func(req *RequestInfo) (ok bool) {
		if index >= len(tArr) {
			t.Errorf("index out of range")
			return false
		}
		expectRequestTime := tArr[index]
		if req.RequestTime != expectRequestTime {

		}
		cnt++
		if cnt == len(aggResult[expectRequestTime]) {
			index++
		}
		return true
	})
}

func TestRequestRecords_Evict(t *testing.T) {
	batchRequestInfo := genBatchRequestInfo(1000, true)
	reqRecords := InitRequestRecords(batchRequestInfo)

	aggResult := reqRecords.AggRequestsByRequestTime()
	tArr := make([]int64, 0, len(aggResult))
	for timeStamp := range aggResult {
		tArr = append(tArr, timeStamp)
	}
	sort.Slice(tArr, func(i, j int) bool {
		return tArr[i] < tArr[j]
	})

	reqRecordMaxCount.Store(500)
	reqRecordReserveMin.Store(1)
	evictTimestamp := reqRecords.GetEvictTimestamp()
	reqRecords.EvictByTime(evictTimestamp)

	if reqRecords.Count() > 500 {
		t.Errorf("count mismatch, expect:less or equal 500, actual:%v", reqRecords.Count())
		return
	}

	time.Sleep(time.Minute)

	evictTimestamp = reqRecords.GetEvictTimestamp()
	reqRecords.EvictByTime(evictTimestamp)

	if reqRecords.Count() != 0 {
		t.Errorf("count mismatch, expect:0, actual:%v", reqRecords.Count())
		return
	}
}

func TestRequestRecords_Remove(t *testing.T) {
	batchReqInfo := genBatchRequestInfo(128, true)
	reqRecords := InitRequestRecords(batchReqInfo)
	req := genRequestInfo(true)
	reqRecords.Update(req)
	if _, dup := reqRecords.IsDupReq(req); !dup {
		t.Errorf("expect dup req, but not")
		return
	}
	reqRecords.Remove(req)

	if _, dup := reqRecords.IsDupReq(req); dup {
		t.Errorf("expect not dup req, but dup")
		return
	}
	reqRecords.RangeList(func(r *RequestInfo) (ok bool) {
		if reflect.DeepEqual(r, req) {
			t.Errorf("dup req, expect not dup")
			t.FailNow()
		}
		return true
	})
}