package proto

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func generateRandomSources(numOfSources int) []*DataSource {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	dss := make([]*DataSource, numOfSources)
	for i := 0; i < numOfSources; i++ {
		dss[i] = &DataSource{
			FileOffset:   r.Uint64(),
			Size_:        r.Uint64(),
			PartitionID:  r.Uint64(),
			ExtentID:     r.Uint64(),
			ExtentOffset: r.Uint64(),
			Hosts: []string{
				fmt.Sprintf("%d.%d.%d.%d:17031", 192, 168, i%3, 1+i%256),
				fmt.Sprintf("%d.%d.%d.%d:17031", 192, 168, i%3+1, 1+i%256),
				fmt.Sprintf("%d.%d.%d.%d:17031", 192, 168, i%3+2, 1+i%256),
			},
		}
	}
	return dss
}

func generateRandomCacheRequest(num int) *CacheRequest {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	sources := generateRandomSources(num)
	req := &CacheRequest{
		Volume:          "tone-test",
		Inode:           r.Uint64(),
		FixedFileOffset: r.Uint64(),
		Version:         ComputeSourcesVersion(sources),
	}
	req.Sources = sources
	return req
}

func generateRequest(num int) *CacheReadRequest {
	return generateRandomCacheReadRequest(num)
}

func generateRandomCacheReadRequest(num int) *CacheReadRequest {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return &CacheReadRequest{
		Offset:       r.Uint64(),
		Size_:        r.Uint64(),
		CacheRequest: generateRandomCacheRequest(num),
	}
}

func BenchmarkDistributedCacheEncode(b *testing.B) {
	b.Run("binary_X1", func(b *testing.B) {
		cacheReadRequestBinaryEncodeBase(b, 1)
	})
	b.Run("proto_buffer_X1", func(b *testing.B) {
		cacheReadRequestProtobufEncodeBase(b, 1)
	})
	b.Run("binary_X100", func(b *testing.B) {
		cacheReadRequestBinaryEncodeBase(b, 100)
	})
	b.Run("proto_buffer_X100", func(b *testing.B) {
		cacheReadRequestProtobufEncodeBase(b, 100)
	})
	b.Run("binary_X5000", func(b *testing.B) {
		cacheReadRequestBinaryEncodeBase(b, 5000)
	})
	b.Run("proto_buffer_X5000", func(b *testing.B) {
		cacheReadRequestProtobufEncodeBase(b, 5000)
	})
}

func cacheReadRequestBinaryEncodeBase(b *testing.B, num int) {
	request := generateRequest(num)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		binary := make([]byte, request.EncodeBinaryLen())
		request.EncodeBinaryTo(binary)
		b.ReportMetric(float64(len(binary)), "bytes/op")
	}
	b.ReportAllocs()
}

func cacheReadRequestProtobufEncodeBase(b *testing.B, num int) {
	request := generateRequest(num)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data, _ := request.Marshal()
		b.ReportMetric(float64(len(data)), "bytes/op")
	}
	b.ReportAllocs()
}

func BenchmarkDistributedCacheDecode(b *testing.B) {
	b.Run("binary_X1", func(b *testing.B) {
		cacheReadRequestBinaryDecodeBase(b, 1)
	})
	b.Run("proto_buffer_X1", func(b *testing.B) {
		cacheReadRequestProtobufDecodeBase(b, 1)
	})
	b.Run("binary_X100", func(b *testing.B) {
		cacheReadRequestBinaryDecodeBase(b, 100)
	})
	b.Run("proto_buffer_X100", func(b *testing.B) {
		cacheReadRequestProtobufDecodeBase(b, 100)
	})
	b.Run("binary_X5000", func(b *testing.B) {
		cacheReadRequestBinaryDecodeBase(b, 5000)
	})
	b.Run("proto_buffer_X5000", func(b *testing.B) {
		cacheReadRequestProtobufDecodeBase(b, 5000)
	})
}

func cacheReadRequestBinaryDecodeBase(b *testing.B, num int) {
	request := generateRequest(num)
	binary := make([]byte, request.EncodeBinaryLen())
	request.EncodeBinaryTo(binary)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		newReq := new(CacheReadRequest)
		newReq.DecodeBinaryFrom(binary)
	}
	b.ReportAllocs()
}

func cacheReadRequestProtobufDecodeBase(b *testing.B, num int) {
	request := generateRequest(num)
	data, _ := request.Marshal()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reqPb := new(CacheReadRequest)
		reqPb.Unmarshal(data)
	}
	b.ReportAllocs()
}
