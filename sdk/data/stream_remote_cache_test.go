package data

import (
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/unit"
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

	cReadReq0 := &proto.CacheReadRequest{
		CacheRequest: cRequests[0],
		Offset:       0,
		Size_:         4 * unit.MB,
		Data:         data[0:proto.CACHE_BLOCK_SIZE],
	}
	cReadReq1 := &proto.CacheReadRequest{
		CacheRequest: cRequests[1],
		Offset:       0,
		Size_:         4 * unit.MB,
		Data:         data[proto.CACHE_BLOCK_SIZE : 2*proto.CACHE_BLOCK_SIZE],
	}
	cReadReq2 := &proto.CacheReadRequest{
		CacheRequest: cRequests[2],
		Offset:       0,
		Size_:         4 * unit.MB,
		Data:         data[2*proto.CACHE_BLOCK_SIZE : 3*proto.CACHE_BLOCK_SIZE],
	}
	cReadReq3 := &proto.CacheReadRequest{
		CacheRequest: cRequests[3],
		Offset:       0,
		Size_:         1 * unit.MB,
		Data:         data[3*proto.CACHE_BLOCK_SIZE : 3*proto.CACHE_BLOCK_SIZE+unit.MB],
	}
	cReadReq4 := &proto.CacheReadRequest{
		CacheRequest: cRequests[3],
		Offset:       0,
		Size_:         1024,
		Data:         data[3*proto.CACHE_BLOCK_SIZE : 3*proto.CACHE_BLOCK_SIZE+1024],
	}
	cReadReq5 := &proto.CacheReadRequest{
		CacheRequest: cRequests[0],
		Offset:       1 * unit.MB,
		Size_:         3 * unit.MB,
		Data:         data[0 : 3*unit.MB],
	}
	cReadReq6 := &proto.CacheReadRequest{
		CacheRequest: cRequests[3],
		Offset:       0,
		Size_:         1*unit.MB - 1024,
		Data:         data[11*unit.MB : 12*unit.MB-1024],
	}
	cReadReq7 := &proto.CacheReadRequest{
		CacheRequest: cRequests[0],
		Offset:       2 * unit.MB,
		Size_:         2 * unit.MB,
		Data:         data[0 : 2*unit.MB],
	}
	cReadReq8 := &proto.CacheReadRequest{
		CacheRequest: cRequests[1],
		Offset:       1 * unit.MB,
		Size_:         1 * unit.MB,
		Data:         data[0 : 1*unit.MB],
	}
	cReadReq9 := &proto.CacheReadRequest{
		CacheRequest: cRequests[1],
		Offset:       1 * unit.MB,
		Size_:         3 * unit.MB,
		Data:         data[0 : 3*unit.MB],
	}
	cReadReq10 := &proto.CacheReadRequest{
		CacheRequest: cRequests[0],
		Offset:       4096,
		Size_:         proto.CACHE_BLOCK_SIZE - 4096,
		Data:         data[0 : proto.CACHE_BLOCK_SIZE-4096],
	}

	testCases := []struct {
		cacheRequests []*proto.CacheRequest
		offset        uint64
		size          uint64
		expect        []*proto.CacheReadRequest
	}{
		{
			cacheRequests: cRequests,
			offset:        0,
			size:          12*unit.MB + 1024,
			expect:        []*proto.CacheReadRequest{cReadReq0, cReadReq1, cReadReq2, cReadReq4},
		},
		{
			cacheRequests: cRequests,
			offset:        1 * unit.MB,
			size:          12*unit.MB - 1024,
			expect:        []*proto.CacheReadRequest{cReadReq5, cReadReq1, cReadReq2, cReadReq6},
		},
		{
			cacheRequests: cRequests,
			offset:        2 * unit.MB,
			size:          11 * unit.MB,
			expect:        []*proto.CacheReadRequest{cReadReq7, cReadReq1, cReadReq2, cReadReq3},
		},
		{
			cacheRequests: cRequests[1:2],
			offset:        5 * unit.MB,
			size:          1 * unit.MB,
			expect:        []*proto.CacheReadRequest{cReadReq8},
		},
		{
			cacheRequests: cRequests[1:],
			offset:        5 * unit.MB,
			size:          8*unit.MB - 1024,
			expect:        []*proto.CacheReadRequest{cReadReq9, cReadReq2, cReadReq6},
		},
		{
			cacheRequests: cRequests[:3],
			offset:        4096,
			size:          12*unit.MB - 4096,
			expect:        []*proto.CacheReadRequest{cReadReq10, cReadReq1, cReadReq2},
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
