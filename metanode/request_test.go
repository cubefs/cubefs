package metanode

import (
	"encoding/binary"
	"math"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/proto"
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

func TestRequestRecords(t *testing.T) {
	records := NewRequestRecords()
	now := time.Now().Unix()
	records.Update(&RequestInfo{RequestTime: now, EnableRemoveDupReq: true})

	_, dup := records.IsDupReq(&RequestInfo{RequestTime: now, EnableRemoveDupReq: true})
	require.True(t, dup)

	_, dup = records.IsDupReq(&RequestInfo{RequestTime: now, EnableRemoveDupReq: true, ReqID: 1})
	require.False(t, dup)

	records.Update(&RequestInfo{RequestTime: now, ReqID: 1, EnableRemoveDupReq: true})
	records.Update(&RequestInfo{RequestTime: time.Now().Add(time.Duration(2)).Unix(), EnableRemoveDupReq: true})
	records.Update(&RequestInfo{RequestTime: time.Now().Add(time.Duration(3)).Unix(), EnableRemoveDupReq: true})

	require.Equal(t, 2, len(records.GetRequestsByRequestTime(now)))

	records.EvictByTime(now)
	require.Equal(t, 0, len(records.GetRequestsByRequestTime(now)))

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
		ClientInfo: proto.ClientInfo{
			ClientID: uint64(rand.Intn(65535)),
			ClientIP: binary.BigEndian.Uint32([]byte{byte(rand.Intn(256)),
				byte(rand.Intn(256)), byte(rand.Intn(256)), byte(rand.Intn(256))}),
		},

		ReqID:              rand.Int63n(math.MaxInt64),
		DataCrc:            uint32(rand.Int31n(math.MaxInt32)),
		RequestTime:        time.Now().UnixMilli(),
		EnableRemoveDupReq: removeDupEnableState,
	}
}
