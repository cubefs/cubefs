package metanode

import (
	"encoding/binary"
	"math"
	"math/rand"
	"reflect"
	"testing"
	"time"
)

func TestRequestInfo_MarshalBinaryAndUnmarshal(t *testing.T) {
	reqInfo := genRequestInfo(false)
	data := reqInfo.MarshalBinary()
	reqByUnmarshal := &RequestInfo{}
	if err := reqByUnmarshal.Unmarshal(data); err != nil {
		t.Errorf("unmarshal failed:%v", err)
		return
	}
	if !reflect.DeepEqual(reqInfo, reqByUnmarshal) {
		t.Errorf("reqInfo not equal, expect:%v, actual:%v", reqInfo, reqByUnmarshal)
	}
}

func TestRequestInfoBatch_MarshalBinaryAndUnmarshal(t *testing.T) {
	batchReqInfo := genBatchRequestInfo(4, false)
	data := batchReqInfo.MarshalBinary()
	batchReqInfoUnmarshal, err := UnmarshalBatchRequestInfo(data)
	if err != nil {
		t.Errorf("unmarshal failed:%v", err)
		return
	}
	if !reflect.DeepEqual(batchReqInfo, batchReqInfoUnmarshal) {
		t.Errorf("batchReqInfo not equal, expect:%v, actual:%v", batchReqInfo, batchReqInfoUnmarshal)
		return
	}
}

func genBatchRequestInfo(count int, removeDupEnableState bool) RequestInfoBatch {
	batchReqInfo := make(RequestInfoBatch, 0, count)
	for index := 0; index < count; index++ {
		batchReqInfo = append(batchReqInfo, genRequestInfo(removeDupEnableState))
		time.Sleep(time.Microsecond * 100)
	}
	return batchReqInfo
}

func genRequestInfo(removeDupEnableState bool) *RequestInfo {
	rand.Seed(time.Now().UnixMicro())
	return &RequestInfo{
		ClientID:           uint64(rand.Intn(65535)),
		ReqID:              rand.Int63n(math.MaxInt64),
		ClientIP:           binary.BigEndian.Uint32([]byte{byte(rand.Intn(256)), byte(rand.Intn(256)), byte(rand.Intn(256)), byte(rand.Intn(256))}),
		DataCrc:            uint32(rand.Int31n(math.MaxInt32)),
		RequestTime:        time.Now().UnixMilli(),
		EnableRemoveDupReq: removeDupEnableState,
	}
}