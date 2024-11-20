package bcache

import (
	"reflect"
	"testing"

	"github.com/cubefs/cubefs/blobstore/util/bytespool"
)

func TestPutCacheRequest(t *testing.T) {
	putReq := PutCacheRequest{
		CacheKey: "test_key",
		Data:     []byte("test_data"),
		VolName:  "test_vol",
	}
	data, err := putReq.Marshal()
	if err != nil {
		t.Fatalf("Marshal PutCacheRequest failed: %v", err)
	}
	var putReq2 PutCacheRequest
	err = putReq2.UnmarshalValue(data)
	if err != nil {
		t.Fatalf("UnmarshalValue PutCacheRequest failed: %v", err)
	}
	if !reflect.DeepEqual(putReq, putReq2) {
		t.Fatalf("result mismatch:\n\tpart1:%v\n\tpart2:%v", putReq, putReq2)
	}
	bytespool.Free(data)
}

func TestGetCacheRequest(t *testing.T) {
	putReq := GetCacheRequest{
		CacheKey: "test_key",
		Offset:   1,
		Size:     1024,
	}
	data, err := putReq.Marshal()
	if err != nil {
		t.Fatalf("Marshal PutCacheRequest failed: %v", err)
	}
	var putReq2 GetCacheRequest
	err = putReq2.UnmarshalValue(data)
	if err != nil {
		t.Fatalf("UnmarshalValue PutCacheRequest failed: %v", err)
	}
	if !reflect.DeepEqual(putReq, putReq2) {
		t.Fatalf("result mismatch:\n\tpart1:%v\n\tpart2:%v", putReq, putReq2)
	}
	bytespool.Free(data)
}

func TestGetCachePathResponse(t *testing.T) {
	putReq := GetCachePathResponse{
		CachePath: "test_key",
	}
	data, err := putReq.Marshal()
	if err != nil {
		t.Fatalf("Marshal PutCacheRequest failed: %v", err)
	}
	var putReq2 GetCachePathResponse
	err = putReq2.UnmarshalValue(data)
	if err != nil {
		t.Fatalf("UnmarshalValue PutCacheRequest failed: %v", err)
	}
	if !reflect.DeepEqual(putReq, putReq2) {
		t.Fatalf("result mismatch:\n\tpart1:%v\n\tpart2:%v", putReq, putReq2)
	}
	bytespool.Free(data)
}

func TestDelCacheRequest(t *testing.T) {
	putReq := DelCacheRequest{
		CacheKey: "test_key",
	}
	data, err := putReq.Marshal()
	if err != nil {
		t.Fatalf("Marshal PutCacheRequest failed: %v", err)
	}
	var putReq2 DelCacheRequest
	err = putReq2.UnmarshalValue(data)
	if err != nil {
		t.Fatalf("UnmarshalValue PutCacheRequest failed: %v", err)
	}
	if !reflect.DeepEqual(putReq, putReq2) {
		t.Fatalf("result mismatch:\n\tpart1:%v\n\tpart2:%v", putReq, putReq2)
	}
	bytespool.Free(data)
}
