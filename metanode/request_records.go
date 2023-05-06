package metanode

import (
	"container/list"
	"go.uber.org/atomic"
	"sort"
	"sync"
	"time"
)

const (
	defaultRecordReservedMin  = 5
	defaultReqRecordsMaxCount = 5000
)

var reqRecordMaxCount = atomic.NewInt32(defaultReqRecordsMaxCount)
var reqRecordReserveMin = atomic.NewInt32(defaultRecordReservedMin)

type RequestRecords struct {
	reqTree *BTree
	reqList *list.List
	rwLock  sync.RWMutex
}

func NewRequestRecords() *RequestRecords {
	return &RequestRecords{
		reqTree: NewBtree(),
		reqList: list.New(),
	}
}

func InitRequestRecords(requestInfos RequestInfoBatch) (reqRecords *RequestRecords) {
	reqRecords = NewRequestRecords()
	if len(requestInfos) == 0 {
		return
	}
	sort.Slice(requestInfos, func(i, j int) bool {
		return requestInfos[i].RequestTime < requestInfos[j].RequestTime
	})

	for _, reqInfo := range requestInfos {
		reqRecords.reqTree.ReplaceOrInsert(reqInfo, true)
		reqRecords.reqList.PushBack(reqInfo)
	}
	return
}

func (records *RequestRecords) Count() int {
	records.rwLock.RLock()
	defer records.rwLock.RUnlock()
	return records.reqList.Len()
}

func (records *RequestRecords) ReqBTreeSnap() *BTree {
	records.rwLock.RLock()
	defer records.rwLock.RUnlock()
	return records.reqTree.GetTree()
}

func (records *RequestRecords) IsDupReq(req *RequestInfo) (previousRespCode uint8, dup bool) {
	if req == nil || !req.EnableRemoveDupReq {
		return
	}
	records.rwLock.RLock()
	defer records.rwLock.RUnlock()
	existReq := records.reqTree.Get(req)
	if existReq == nil {
		return
	}

	dup = true
	previousRespCode = existReq.(*RequestInfo).RespCode
	return
}

func (records *RequestRecords) Update(req *RequestInfo) {
	if req == nil || !req.EnableRemoveDupReq {
		return
	}
	records.rwLock.Lock()
	defer records.rwLock.Unlock()
	if records.reqTree.Has(req) {
		return
	}
	element:= records.reqList.Back()
	for element != nil {
		reqInfo := element.Value.(*RequestInfo)
		if reqInfo.RequestTime < req.RequestTime {
			break
		}
		element = element.Prev()
	}
	if element == nil {
		records.reqList.PushFront(req)
	} else {
		records.reqList.InsertAfter(req, element)
	}
	records.reqTree.ReplaceOrInsert(req, true)
}

func (records *RequestRecords) Remove(req *RequestInfo) {
	if req == nil || !req.EnableRemoveDupReq {
		return
	}
	records.rwLock.Lock()
	defer records.rwLock.Unlock()
	if !records.reqTree.Has(req) {
		return
	}
	records.reqTree.Delete(req)
	element:= records.reqList.Back()
	for element != nil {
		reqInfo := element.Value.(*RequestInfo)
		if reqInfo.RequestTime == req.RequestTime && reqInfo.ClientIP == req.ClientIP && reqInfo.ClientID == req.ClientID &&
			reqInfo.DataCrc == reqInfo.DataCrc && reqInfo.ReqID == req.ReqID && reqInfo.ClientStartTime == reqInfo.ClientStartTime {
			records.reqList.Remove(element)
		}
		element = element.Prev()
	}
	return
}

func (records *RequestRecords) GetEvictTimestamp() (evictTimestamp int64) {
	records.rwLock.RLock()
	defer records.rwLock.RUnlock()
	ttlTimestamp := time.Now().Add(-time.Minute * time.Duration(reqRecordReserveMin.Load())).UnixMilli()
	realCnt := records.reqList.Len()
	expectCnt := int(reqRecordMaxCount.Load())
	for element := records.reqList.Front(); realCnt > expectCnt && element != nil; element = element.Next() {
		realCnt--
		evictTimestamp = element.Value.(*RequestInfo).RequestTime
	}
	if evictTimestamp < ttlTimestamp {
		evictTimestamp = ttlTimestamp
	}
	return
}

func (records *RequestRecords) EvictByTime(evictTimestamp int64) {
	records.rwLock.Lock()
	defer records.rwLock.Unlock()
	for {
		element := records.reqList.Front()
		if element == nil {
			break
		}

		if element.Value.(*RequestInfo).RequestTime > evictTimestamp {
			break
		}
		records.reqTree.Delete(element.Value.(*RequestInfo))
		records.reqList.Remove(element)
	}
}

func (records *RequestRecords) AggRequestsByRequestTime() (r map[int64]RequestInfoBatch) {
	records.rwLock.RLock()
	defer records.rwLock.RUnlock()
	r = make(map[int64]RequestInfoBatch, 0)

	for element := records.reqList.Front(); element != nil; {
		requestInfo := element.Value.(*RequestInfo)
		if _, ok := r[requestInfo.RequestTime]; !ok {
			r[requestInfo.RequestTime] = make(RequestInfoBatch, 0)
		}
		r[requestInfo.RequestTime] = append(r[requestInfo.RequestTime], requestInfo)
		element = element.Next()
	}
	return
}

func (records *RequestRecords) GetRequestsByRequestTime(requestTime int64) (reqInfos RequestInfoBatch) {
	records.rwLock.RLock()
	defer records.rwLock.RUnlock()
	for element := records.reqList.Back(); element != nil; {
		requestInfo := element.Value.(*RequestInfo)
		if requestInfo.RequestTime == requestTime {
			reqInfos = append(reqInfos, requestInfo)
		}
		element = element.Prev()
	}
	return
}

func (records *RequestRecords) RangeList(f func(req *RequestInfo) (ok bool)) {
	records.rwLock.RLock()
	defer records.rwLock.RUnlock()
	for element := records.reqList.Front(); element != nil; {
		requestInfo := element.Value.(*RequestInfo)
		if ok := f(requestInfo); !ok {
			break
		}
		element = element.Next()
	}
}

func (records *RequestRecords) RangeTree(f func(req *RequestInfo) (ok bool)) {
	records.rwLock.RLock()
	defer records.rwLock.RUnlock()
	records.reqTree.Ascend(func(i BtreeItem) bool {
		return f(i.(*RequestInfo))
	})
}
