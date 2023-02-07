package data

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/chubaofs/chubaofs/proto"
)

func Test_PrepareRequests(t *testing.T) {
	type args struct {
		offset uint64
		size   int
		data   []byte
	}

	var eks []proto.ExtentKey
	ek1 := proto.ExtentKey{FileOffset: 0, PartitionId: 1, ExtentId: 1, ExtentOffset: 0, Size: 20}
	ek2 := proto.ExtentKey{FileOffset: 50, PartitionId: 2, ExtentId: 1002, ExtentOffset: 0, Size: 20}
	ek3 := proto.ExtentKey{FileOffset: 100, PartitionId: 3, ExtentId: 1003, ExtentOffset: 0, Size: 20}
	eks = append(eks, ek1, ek2, ek3)
	testExtentCache := NewExtentCache(1)
	testExtentCache.update(0, 0, eks)

	testCases := []struct {
		name string
		args args
		want []*proto.ExtentKey
	}{
		{
			name: "appendWrite",
			args: args{offset: 20, size: 20, data: make([]byte, 20)},
			want: []*proto.ExtentKey{nil},
		},
		{
			name: "overwrite",
			args: args{offset: 50, size: 20, data: make([]byte, 20)},
			want: []*proto.ExtentKey{{FileOffset: 50, PartitionId: 2, ExtentId: 1002, ExtentOffset: 0, Size: 20, CRC: 0}},
		},
		{
			name: "overwrite && appendWrite",
			args: args{offset: 100, size: 30, data: make([]byte, 30)},
			want: []*proto.ExtentKey{{FileOffset: 100, PartitionId: 3, ExtentId: 1003, ExtentOffset: 0, Size: 20, CRC: 0}, nil},
		},
	}
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			get, _ := testExtentCache.PrepareRequests(tt.args.offset, tt.args.size, tt.args.data)
			fmt.Printf("extent request: %v\n", get)
			for i, req := range get {
				if !reflect.DeepEqual(req.ExtentKey, tt.want[i]) {
					t.Errorf("testCaseName(%v) failed: getEk(%v), but want(%v)", tt.name, req.ExtentKey, tt.want[i])
				}
			}
		})
	}
}

func Test_ExtentRangePerformance(t *testing.T)  {
	extentCache := NewExtentCache(2)
	ekLen := 500000
	for i := 0; i < ekLen; i++ {
		ek := proto.ExtentKey{
			FileOffset:   uint64(i)*4096,
			PartitionId:  uint64(i+1),
			ExtentId:     uint64(i+1),
			Size:         4096,
		}
		extentCache.Insert(&ek, true)
	}
	fmt.Println("slice size: ", extentCache.root.Len())

	rand.Seed(time.Now().UnixNano())
	round := ekLen/100
	offsetSlice := make([]uint64, 0)
	for i := 0; i < round; i++ {
		off := rand.Intn(ekLen)
		offsetSlice = append(offsetSlice, uint64(off))
	}

	size := 4096
	data := make([]byte, size)
	start := time.Now()
	for _, off := range offsetSlice {
		start := off*4096
		extentCache.PrepareRequests(start, size, data)
	}
	if cost := time.Since(start)/time.Duration(round); cost > 20 * time.Microsecond {
		t.Fatalf("Test_ExtentRangePerformance range extents cost too long: %v, ekLen(%v)", cost, ekLen)
	}
}