package data

import (
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/flash"
	"reflect"
	"testing"
)

func Test_getCacheReadRequests(t *testing.T) {
	t.Skip()
	data := make([]byte, 4*proto.CACHE_BLOCK_SIZE, 4*proto.CACHE_BLOCK_SIZE)
	source := &proto.DataSource{}
	cRequests := []*proto.CacheRequest{
		&proto.CacheRequest{FixedFileOffset: 0},
		&proto.CacheRequest{FixedFileOffset: proto.CACHE_BLOCK_SIZE, Sources: []*proto.DataSource{source}},
		&proto.CacheRequest{FixedFileOffset: 2 * proto.CACHE_BLOCK_SIZE},
		&proto.CacheRequest{FixedFileOffset: 3 * proto.CACHE_BLOCK_SIZE, Sources: []*proto.DataSource{source}},
	}

	cReadReq0 := &flash.CacheReadRequest{
		CacheReadRequest: proto.CacheReadRequest{
			CacheRequest: cRequests[0],
			Offset:       0,
			Size_:        proto.CACHE_BLOCK_SIZE,
		},
		Data: data[0:proto.CACHE_BLOCK_SIZE],
	}
	cReadReq1 := &flash.CacheReadRequest{
		CacheReadRequest: proto.CacheReadRequest{
			CacheRequest: cRequests[1],
			Offset:       0,
			Size_:        1024,
		},
		Data: data[proto.CACHE_BLOCK_SIZE : proto.CACHE_BLOCK_SIZE+1024],
	}
	cReadReq2 := &flash.CacheReadRequest{
		CacheReadRequest: proto.CacheReadRequest{
			CacheRequest: cRequests[1],
			Offset:       0,
			Size_:        proto.CACHE_BLOCK_SIZE,
		},
		Data: data[1*proto.CACHE_BLOCK_SIZE : 2*proto.CACHE_BLOCK_SIZE],
	}
	cReadReq3 := &flash.CacheReadRequest{
		CacheReadRequest: proto.CacheReadRequest{
			CacheRequest: cRequests[2],
			Offset:       0,
			Size_:        proto.CACHE_BLOCK_SIZE,
		},
		Data: data[2*proto.CACHE_BLOCK_SIZE : 3*proto.CACHE_BLOCK_SIZE],
	}
	cReadReq4 := &flash.CacheReadRequest{
		CacheReadRequest: proto.CacheReadRequest{
			CacheRequest: cRequests[3],
			Offset:       0,
			Size_:        proto.CACHE_BLOCK_SIZE - 1024,
		},
		Data: data[3*proto.CACHE_BLOCK_SIZE : 4*proto.CACHE_BLOCK_SIZE-1024],
	}
	cReadReq5 := &flash.CacheReadRequest{
		CacheReadRequest: proto.CacheReadRequest{
			CacheRequest: cRequests[3],
			Offset:       0,
			Size_:        proto.CACHE_BLOCK_SIZE,
		},
		Data: data[3*proto.CACHE_BLOCK_SIZE : 4*proto.CACHE_BLOCK_SIZE],
	}
	cReadReq6 := &flash.CacheReadRequest{
		CacheReadRequest: proto.CacheReadRequest{
			CacheRequest: cRequests[1],
			Offset:       1024,
			Size_:        proto.CACHE_BLOCK_SIZE - 1024,
		},
		Data: data[proto.CACHE_BLOCK_SIZE+1024 : 2*proto.CACHE_BLOCK_SIZE],
	}
	cReadReq7 := &flash.CacheReadRequest{
		CacheReadRequest: proto.CacheReadRequest{
			CacheRequest: cRequests[0],
			Offset:       1024,
			Size_:        proto.CACHE_BLOCK_SIZE - 1024,
		},
		Data: data[1024:proto.CACHE_BLOCK_SIZE],
	}

	testCases := []struct {
		cacheRequests []*proto.CacheRequest
		offset        uint64
		size          uint64
		expect        []*flash.CacheReadRequest
	}{
		{
			cacheRequests: cRequests[:2],
			offset:        0,
			size:          proto.CACHE_BLOCK_SIZE + 1024,
			expect:        []*flash.CacheReadRequest{cReadReq0, cReadReq1},
		},
		{
			cacheRequests: cRequests[1:],
			offset:        proto.CACHE_BLOCK_SIZE,
			size:          3*proto.CACHE_BLOCK_SIZE - 1024,
			expect:        []*flash.CacheReadRequest{cReadReq2, cReadReq3, cReadReq4},
		},
		{
			cacheRequests: cRequests[2:],
			offset:        2 * proto.CACHE_BLOCK_SIZE,
			size:          2*proto.CACHE_BLOCK_SIZE + 1024,
			expect:        []*flash.CacheReadRequest{cReadReq3, cReadReq5},
		},
		{
			cacheRequests: cRequests[1:2],
			offset:        proto.CACHE_BLOCK_SIZE + 1024,
			size:          proto.CACHE_BLOCK_SIZE,
			expect:        []*flash.CacheReadRequest{cReadReq6},
		},
		{
			cacheRequests: cRequests[0:1],
			offset:        1024,
			size:          proto.CACHE_BLOCK_SIZE - 1024,
			expect:        []*flash.CacheReadRequest{cReadReq7},
		},
	}

	for i, tt := range testCases {
		actual := getCacheReadRequests(tt.offset, tt.size, data, tt.cacheRequests)
		if !compareSlice(actual, tt.expect) {
			t.Errorf("testCase(%d) failed: actual(%+v), except(%+v)", i, actual, tt.expect)
		}
	}
}

func compareSlice(actual, expect interface{}) bool {
	a := reflect.ValueOf(actual)
	e := reflect.ValueOf(expect)
	if a.Kind() != reflect.Slice || e.Kind() != reflect.Slice {
		return false
	}
	if a.Len() != e.Len() {
		return false
	}
	for i := 0; i < a.Len(); i++ {
		if !reflect.DeepEqual(a.Index(i).Interface(), e.Index(i).Interface()) {
			return false
		}
	}
	return true
}
