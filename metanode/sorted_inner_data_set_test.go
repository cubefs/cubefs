package metanode

import (
	"context"
	"github.com/chubaofs/chubaofs/proto"
	se "github.com/chubaofs/chubaofs/util/sortedextent"
	"math"
	"reflect"
	"testing"
)

var data = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func TestSortedInnerDataSet_Insert01(t *testing.T) {
	si := NewSortedInnerDataSet()
	records := []*proto.InnerDataSt{
		{FileOffset: 8, Size: 8, Data:data[8:16]},
		{FileOffset: 16, Size: 8, Data:data[16:24]},
		{FileOffset: 0, Size: 8, Data:data[0:8]},
		{FileOffset: 24, Size: 8, Data:data[24:32]},
		{FileOffset: 8, Size: 16, Data:data[8:24]},
		{FileOffset: 0, Size: 32, Data:data[0:32]},
		{FileOffset: 48, Size: 8, Data:data[48:56]},
		{FileOffset: 40, Size: 8, Data:data[40:48]},
	}
	for _, record := range records {
		si.Insert(record)
	}

	expect := []*proto.InnerDataSt{
		{FileOffset: 0, Size: 32, Data:data[0:32]},
		{FileOffset: 40, Size: 8, Data:data[40:48]},
		{FileOffset: 48, Size: 8, Data:data[48:56]},

	}
	if len(si.innerDataSet) != len(expect) {
		t.Fatalf("number of inner data array mismatch: expect %v, actual %v", len(expect), len(si.innerDataSet))
	}
	for i := 0; i < len(expect); i++ {
		if !reflect.DeepEqual(si.innerDataSet[i], expect[i]) {
			t.Fatalf("innerDataArr[%v] mismatch: expect %v, actual %v", i, expect[i], si.innerDataSet[i])
		}
	}
}

func TestSortedInnerDataSet_Insert02(t *testing.T) {
	si := NewSortedInnerDataSet()
	records := []*proto.InnerDataSt{
		{FileOffset: 8, Size: 8, Data:data[8:16]},
		{FileOffset: 16, Size: 8, Data:data[16:24]},
		{FileOffset: 0, Size: 12, Data:data[0:12]},
		{FileOffset: 20, Size: 20, Data:data[20:40]},
		{FileOffset: 8, Size: 24, Data:data[8:32]},
		{FileOffset: 24, Size: 24, Data:data[24:48]},
		{FileOffset: 8, Size: 8, Data:data[8:16]},
		{FileOffset: 16, Size: 8, Data:data[16:24]},
		{FileOffset: 48, Size: 8, Data:data[48:56]},
		{FileOffset: 56, Size: 8, Data:data[56:64]},
		{FileOffset: 20, Size: 12, Data:data[20:32]},
	}
	for _, record := range records {
		si.Insert(record)
	}

	expect := []*proto.InnerDataSt{
		{FileOffset: 0, Size: 8, Data:data[0:8]},
		{FileOffset: 8, Size: 8, Data:data[8:16]},
		{FileOffset: 16, Size: 4, Data:data[16:20]},
		{FileOffset: 20, Size: 12, Data:data[20:32]},
		{FileOffset: 32, Size: 16, Data:data[32:48]},
		{FileOffset: 48, Size: 8, Data:data[48:56]},
		{FileOffset: 56, Size: 8, Data:data[56:64]},
	}
	if len(si.innerDataSet) != len(expect) {
		t.Fatalf("number of inner data array mismatch: expect %v, actual %v", len(expect), len(si.innerDataSet))
	}
	for i := 0; i < len(expect); i++ {
		if !reflect.DeepEqual(si.innerDataSet[i], expect[i]) {
			t.Fatalf("innerDataArr[%v] mismatch: expect %v, actual %v", i, expect[i], si.innerDataSet[i])
		}
	}
}

func TestSortedInnerDataSet_Insert03(t *testing.T) {
	si := NewSortedInnerDataSet()
	records := []*proto.InnerDataSt{
		{FileOffset: 0, Size: 64, Data:data[0:64]},
		{FileOffset: 16, Size: 8, Data:data[16:24]},
	}
	for _, record := range records {
		si.Insert(record)
	}

	expect := []*proto.InnerDataSt{
		{FileOffset: 0, Size: 16, Data:data[0:16]},
		{FileOffset: 16, Size: 8, Data:data[16:24]},
		{FileOffset: 24, Size: 40, Data:data[24:64]},

	}
	if len(si.innerDataSet) != len(expect) {
		t.Fatalf("number of inner data array mismatch: expect %v, actual %v", len(expect), len(si.innerDataSet))
	}
	for i := 0; i < len(expect); i++ {
		if !reflect.DeepEqual(si.innerDataSet[i], expect[i]) {
			t.Fatalf("innerDataArr[%v] mismatch: expect %v, actual %v", i, expect[i], si.innerDataSet[i])
		}
	}
}

func TestSortedInnerDataSet_Insert04(t *testing.T) {
	si := NewSortedInnerDataSet()
	records := []*proto.InnerDataSt{
		{FileOffset: 0, Size: 64, Data:data[0:64]},
		{FileOffset: 16, Size: 48, Data:data[16:64]},
	}
	for _, record := range records {
		si.Insert(record)
	}

	expect := []*proto.InnerDataSt{
		{FileOffset: 0, Size: 16, Data:data[0:16]},
		{FileOffset: 16, Size: 48, Data:data[16:64]},

	}
	if len(si.innerDataSet) != len(expect) {
		t.Fatalf("number of inner data array mismatch: expect %v, actual %v", len(expect), len(si.innerDataSet))
	}
	for i := 0; i < len(expect); i++ {
		if !reflect.DeepEqual(si.innerDataSet[i], expect[i]) {
			t.Fatalf("innerDataArr[%v] mismatch: expect %v, actual %v", i, expect[i], si.innerDataSet[i])
		}
	}
}

func TestSortedInnerDataSet_Insert05(t *testing.T) {
	si := NewSortedInnerDataSet()
	records := []*proto.InnerDataSt{
		{FileOffset: 0, Size: 64, Data:data[0:64]},
		{FileOffset: 0, Size: 8, Data:data[0:8]},
	}
	for _, record := range records {
		si.Insert(record)
	}

	expect := []*proto.InnerDataSt{
		{FileOffset: 0, Size: 8, Data:data[0:8]},
		{FileOffset: 8, Size: 56, Data:data[8:64]},

	}
	if len(si.innerDataSet) != len(expect) {
		t.Fatalf("number of inner data array mismatch: expect %v, actual %v", len(expect), len(si.innerDataSet))
	}
	for i := 0; i < len(expect); i++ {
		if !reflect.DeepEqual(si.innerDataSet[i], expect[i]) {
			t.Fatalf("innerDataArr[%v] mismatch: expect %v, actual %v", i, expect[i], si.innerDataSet[i])
		}
	}
}

/*
inner data:
|====|
     |====|
          |====|

merge result:
|============|
*/
func TestSortedInnerDataSet_Merge01(t *testing.T) {
	si := NewSortedInnerDataSet()
	records := []*proto.InnerDataSt{
		{FileOffset: 0, Size: 8, Data:data[0:8]},
		{FileOffset: 8, Size: 8, Data:data[8:16]},
		{FileOffset: 16, Size: 8, Data:data[16:24]},
	}
	for _, record := range records {
		si.Insert(record)
	}
	si.Merge()

	expect := []*proto.InnerDataSt{
		{FileOffset: 0, Size: 24, Data:data[0:24]},
	}
	if len(si.innerDataSet) != len(expect) {
		t.Fatalf("number of inner data array mismatch: expect %v, actual %v", len(expect), len(si.innerDataSet))
	}
	for i := 0; i < len(expect); i++ {
		if !reflect.DeepEqual(si.innerDataSet[i], expect[i]) {
			t.Fatalf("innerDataSet[%v] mismatch: expect %v, actual %v", i, expect[i], si.innerDataSet[i])
		}
	}
}

/*
inner data:
|====|
         |====|
              |====|
                       |====|

merge result:
|====|  |========|   |====|
*/
func TestSortedInnerDataSet_Merge02(t *testing.T) {
	si := NewSortedInnerDataSet()
	records := []*proto.InnerDataSt{
		{FileOffset: 0, Size: 8, Data:data[0:8]},
		{FileOffset: 16, Size: 8, Data:data[16:24]},
		{FileOffset: 24, Size: 8, Data:data[24:32]},
		{FileOffset: 40, Size: 8, Data:data[40:48]},
	}
	for _, record := range records {
		si.Insert(record)
	}
	si.Merge()

	expect := []*proto.InnerDataSt{
		{FileOffset: 0, Size: 8, Data:data[0:8]},
		{FileOffset: 16, Size: 16, Data:data[16:32]},
		{FileOffset: 40, Size: 8, Data:data[40:48]},
	}
	if len(si.innerDataSet) != len(expect) {
		t.Fatalf("number of inner data array mismatch: expect %v, actual %v", len(expect), len(si.innerDataSet))
	}
	for i := 0; i < len(expect); i++ {
		if !reflect.DeepEqual(si.innerDataSet[i], expect[i]) {
			t.Fatalf("innerDataSet[%v] mismatch: expect %v, actual %v", i, expect[i], si.innerDataSet[i])
		}
	}
}

/*
inner data:
|====|
         |====|
              |====|
                       |====|
                            |====|
                                 |====|

merge result:
|====|  |========|   |============|
*/
func TestSortedInnerDataSet_Merge03(t *testing.T) {
	si := NewSortedInnerDataSet()
	records := []*proto.InnerDataSt{
		{FileOffset: 0, Size: 8, Data:data[0:8]},
		{FileOffset: 16, Size: 8, Data:data[16:24]},
		{FileOffset: 24, Size: 8, Data:data[24:32]},
		{FileOffset: 40, Size: 8, Data:data[40:48]},
		{FileOffset: 48, Size: 8, Data:data[48:56]},
		{FileOffset: 56, Size: 8, Data:data[56:64]},
	}
	for _, record := range records {
		si.Insert(record)
	}
	si.Merge()

	expect := []*proto.InnerDataSt{
		{FileOffset: 0, Size: 8, Data:data[0:8]},
		{FileOffset: 16, Size: 16, Data:data[16:32]},
		{FileOffset: 40, Size: 24, Data:data[40:64]},
	}
	if len(si.innerDataSet) != len(expect) {
		t.Fatalf("number of inner data array mismatch: expect %v, actual %v", len(expect), len(si.innerDataSet))
	}
	for i := 0; i < len(expect); i++ {
		if !reflect.DeepEqual(si.innerDataSet[i], expect[i]) {
			t.Fatalf("innerDataSet[%v] mismatch: expect %v, actual %v", i, expect[i], si.innerDataSet[i])
		}
	}
}

func TestSortedInnerDataSet_ReshuffleInnerDataSet(t *testing.T) {
	si := NewSortedInnerDataSet()
	records := []*proto.InnerDataSt{
		{FileOffset: 8, Size: 32, Data:data[8:40]},
		{FileOffset: 64, Size: 8, Data:data[64:72]},
		{FileOffset: 88, Size: 8, Data:data[88:96]},
		{FileOffset: 128, Size: 8, Data:data[128:136]},
		{FileOffset: 144, Size: 8, Data:data[144:152]},
	}
	for _, record := range records {
		si.Insert(record)
	}
	si.Merge()

	eks := []proto.ExtentKey{
		{FileOffset: 0, Size: 16, PartitionId: math.MaxUint64, ExtentId: math.MaxUint64, ExtentOffset: 0, StoreType: proto.InnerData},
		{FileOffset: 16, Size: 4, PartitionId: 1, ExtentId: 1, ExtentOffset: 0, StoreType: proto.NormalData},
		{FileOffset: 20, Size: 4, PartitionId: math.MaxUint64, ExtentId: math.MaxUint64, ExtentOffset: 20, StoreType: proto.InnerData},
		{FileOffset: 24, Size: 4, PartitionId: 1, ExtentId: 2, ExtentOffset: 0, StoreType: proto.NormalData},
		{FileOffset: 28, Size: 4, PartitionId: math.MaxUint64, ExtentId: math.MaxUint64, ExtentOffset: 28, StoreType: proto.InnerData},
		{FileOffset: 32, Size: 4, PartitionId: 1, ExtentId: 3, ExtentOffset: 0, StoreType: proto.NormalData},
		{FileOffset: 36, Size: 12, PartitionId: math.MaxUint64, ExtentId: math.MaxUint64, ExtentOffset: 36, StoreType: proto.InnerData},
		{FileOffset: 48, Size: 8, PartitionId: 1, ExtentId: 4, ExtentOffset: 0, StoreType: proto.NormalData},
		{FileOffset: 56, Size: 24, PartitionId: math.MaxUint64, ExtentId: math.MaxUint64, ExtentOffset: 56, StoreType: proto.InnerData},
		{FileOffset: 80, Size: 8, PartitionId: 1, ExtentId: 5, ExtentOffset: 0, StoreType: proto.NormalData},
		{FileOffset: 88, Size: 8, PartitionId: math.MaxUint64, ExtentId: math.MaxUint64, ExtentOffset: 88, StoreType: proto.InnerData},
		{FileOffset: 96, Size: 8, PartitionId: 1, ExtentId: 6, ExtentOffset: 0, StoreType: proto.NormalData},
		{FileOffset: 104, Size: 8, PartitionId: math.MaxUint64, ExtentId: math.MaxUint64, ExtentOffset: 104, StoreType: proto.InnerData},
		{FileOffset: 112, Size: 8, PartitionId: 1, ExtentId: 7, ExtentOffset: 0, StoreType: proto.NormalData},
		{FileOffset: 120, Size: 64, PartitionId: math.MaxUint64, ExtentId: math.MaxUint64, ExtentOffset: 120, StoreType: proto.InnerData},
		{FileOffset: 184, Size: 8, PartitionId: 1, ExtentId: 8, ExtentOffset: 0, StoreType: proto.NormalData},
	}
	ctx := context.Background()
	sortedEKs := se.NewSortedExtents()
	for _, ek := range eks {
		sortedEKs.Insert(ctx, ek)
	}

	si.ReshuffleInnerDataSet(sortedEKs)
	expect := []*proto.InnerDataSt{
		{FileOffset: 8, Size: 8, Data:data[8:16]},
		{FileOffset: 20, Size: 4, Data:data[20:24]},
		{FileOffset: 28, Size: 4, Data:data[28:32]},
		{FileOffset: 36, Size: 4, Data:data[36:40]},
		{FileOffset: 64, Size: 8, Data:data[64:72]},
		{FileOffset: 88, Size: 8, Data:data[88:96]},
		{FileOffset: 128, Size: 8, Data:data[128:136]},
		{FileOffset: 144, Size: 8, Data:data[144:152]},
	}

	if len(si.innerDataSet) != len(expect) {
		t.Fatalf("number of inner data array mismatch: expect %v, actual %v", len(expect), len(si.innerDataSet))
	}
	for i := 0; i < len(expect); i++ {
		if !reflect.DeepEqual(si.innerDataSet[i], expect[i]) {
			t.Fatalf("innerDataSet[%v] mismatch: expect %v, actual %v", i, expect[i], si.innerDataSet[i])
		}
	}
}

/*
already exist inner data :
     |================|          file offset:8,  size:32

sorted eks:
|========|                       file offset:0,  size:16, type:inner data
         |==|                    file offset:16, size:4,  type:normal data
            |==|                 file offset:20, size:4,  type:inner data
               |==|              file offset:24, size:4,  type:normal data
                  |==|           file offset:28, size:4,  type:inner data
                     |==|        file offset:32, size:4,  type:normal data
                        |======| file offset:36, size:12, type:inner data

expect result:
     |====|                      file offset:8,  size:8
            |==|                 file offset:20, size:4
                 |==|            file offset:28, size:4
                       |==|      file offset:36, size:4
*/
func TestSortedInnerDataSet_ReshuffleInnerDataSet01(t *testing.T) {
	si := NewSortedInnerDataSet()
	si.Insert(&proto.InnerDataSt{FileOffset: 8, Size: 32, Data:data[8:40]})
	eks := []proto.ExtentKey{
		{FileOffset: 0, Size: 16, PartitionId: math.MaxUint64, ExtentId: math.MaxUint64, ExtentOffset: 0, StoreType: proto.InnerData},
		{FileOffset: 16, Size: 4, PartitionId: 1, ExtentId: 1, ExtentOffset: 0, StoreType: proto.NormalData},
		{FileOffset: 20, Size: 4, PartitionId: math.MaxUint64, ExtentId: math.MaxUint64, ExtentOffset: 20, StoreType: proto.InnerData},
		{FileOffset: 24, Size: 4, PartitionId: 1, ExtentId: 2, ExtentOffset: 0, StoreType: proto.NormalData},
		{FileOffset: 28, Size: 4, PartitionId: math.MaxUint64, ExtentId: math.MaxUint64, ExtentOffset: 28, StoreType: proto.InnerData},
		{FileOffset: 32, Size: 4, PartitionId: 1, ExtentId: 3, ExtentOffset: 0, StoreType: proto.NormalData},
		{FileOffset: 36, Size: 12, PartitionId: math.MaxUint64, ExtentId: math.MaxUint64, ExtentOffset: 36, StoreType: proto.InnerData},
	}
	ctx := context.Background()
	sortedEKs := se.NewSortedExtents()
	for _, ek := range eks {
		sortedEKs.Insert(ctx, ek)
	}

	si.ReshuffleInnerDataSet(sortedEKs)
	expect := []*proto.InnerDataSt{
		{FileOffset: 8, Size: 8, Data: data[8:16]},
		{FileOffset: 20, Size: 4, Data: data[20:24]},
		{FileOffset: 28, Size: 4, Data: data[28:32]},
		{FileOffset: 36, Size: 4, Data: data[36:40]},
	}
	if len(si.innerDataSet) != len(expect) {
		t.Fatalf("number of inner data array mismatch: expect %v, actual %v", len(expect), len(si.innerDataSet))
	}
	for i := 0; i < len(expect); i++ {
		if !reflect.DeepEqual(si.innerDataSet[i], expect[i]) {
			t.Fatalf("innerDataSet[%v] mismatch: expect %v, actual %v", i, expect[i], si.innerDataSet[i])
		}
	}
}

/*
already exist inner data :
     |====|          file offset:64, size:8

sorted eks:
  |========|         file offset:56, size:24, type:inner data

expect result:
     |====|          file offset:64,  size:8
*/
func TestSortedInnerDataSet_ReshuffleInnerDataSet02(t *testing.T) {
	si := NewSortedInnerDataSet()
	si.Insert(&proto.InnerDataSt{FileOffset: 64, Size: 8, Data:data[64:72]})
	eks := []proto.ExtentKey{
		{FileOffset: 56, Size: 24, PartitionId: math.MaxUint64, ExtentId: math.MaxUint64, ExtentOffset: 56, StoreType: proto.InnerData},
	}
	ctx := context.Background()
	sortedEKs := se.NewSortedExtents()
	for _, ek := range eks {
		sortedEKs.Insert(ctx, ek)
	}

	si.ReshuffleInnerDataSet(sortedEKs)
	expect := []*proto.InnerDataSt{
		{FileOffset: 64, Size: 8, Data:data[64:72]},
	}
	if len(si.innerDataSet) != len(expect) {
		t.Fatalf("number of inner data array mismatch: expect %v, actual %v", len(expect), len(si.innerDataSet))
	}
	for i := 0; i < len(expect); i++ {
		if !reflect.DeepEqual(si.innerDataSet[i], expect[i]) {
			t.Fatalf("innerDataSet[%v] mismatch: expect %v, actual %v", i, expect[i], si.innerDataSet[i])
		}
	}
}

/*
already exist inner data :
     |====|          file offset:88, size:8

sorted eks:
     |====|          file offset:88, size:8, type:inner data

expect result:
     |====|          file offset:88,  size:8
*/
func TestSortedInnerDataSet_ReshuffleInnerDataSet03(t *testing.T) {
	si := NewSortedInnerDataSet()
	si.Insert(&proto.InnerDataSt{FileOffset: 88, Size: 8, Data:data[88:96]})
	eks := []proto.ExtentKey{
		{FileOffset: 88, Size: 8, PartitionId: math.MaxUint64, ExtentId: math.MaxUint64, ExtentOffset: 88, StoreType: proto.InnerData},
	}
	ctx := context.Background()
	sortedEKs := se.NewSortedExtents()
	for _, ek := range eks {
		sortedEKs.Insert(ctx, ek)
	}

	si.ReshuffleInnerDataSet(sortedEKs)
	expect := []*proto.InnerDataSt{
		{FileOffset: 88, Size: 8, Data:data[88:96]},
	}
	if len(si.innerDataSet) != len(expect) {
		t.Fatalf("number of inner data array mismatch: expect %v, actual %v", len(expect), len(si.innerDataSet))
	}
	for i := 0; i < len(expect); i++ {
		if !reflect.DeepEqual(si.innerDataSet[i], expect[i]) {
			t.Fatalf("innerDataSet[%v] mismatch: expect %v, actual %v", i, expect[i], si.innerDataSet[i])
		}
	}
}

/*
already exist inner data :
     null

sorted eks:
     |====|        file offset:104, size:8, type:inner data

expect result:
     null
*/
func TestSortedInnerDataSet_ReshuffleInnerDataSet04(t *testing.T) {
	si := NewSortedInnerDataSet()
	eks := []proto.ExtentKey{
		{FileOffset: 104, Size: 8, PartitionId: math.MaxUint64, ExtentId: math.MaxUint64, ExtentOffset: 104, StoreType: proto.InnerData},
	}
	ctx := context.Background()
	sortedEKs := se.NewSortedExtents()
	for _, ek := range eks {
		sortedEKs.Insert(ctx, ek)
	}

	si.ReshuffleInnerDataSet(sortedEKs)
	expect := make([]*proto.InnerDataSt, 0)
	if len(si.innerDataSet) != len(expect) {
		t.Fatalf("number of inner data array mismatch: expect %v, actual %v", len(expect), len(si.innerDataSet))
	}
	for i := 0; i < len(expect); i++ {
		if !reflect.DeepEqual(si.innerDataSet[i], expect[i]) {
			t.Fatalf("innerDataSet[%v] mismatch: expect %v, actual %v", i, expect[i], si.innerDataSet[i])
		}
	}
}

/*
already exist inner data :
     |====|                              file offset:128, size:8,
             |====|                      file offset:144, size:8,

sorted eks:
|===============================|        file offset:120, size:64, type:inner data

expect result:
     |====|                              file offset:128, size:8,
             |====|                      file offset:144, size:8,
*/
func TestSortedInnerDataSet_ReshuffleInnerDataSet05(t *testing.T) {
	si := NewSortedInnerDataSet()
	si.Insert(&proto.InnerDataSt{FileOffset: 128, Size: 8, Data:data[128:136]})
	si.Insert(&proto.InnerDataSt{FileOffset: 144, Size: 8, Data:data[144:152]})

	eks := []proto.ExtentKey{
		{FileOffset: 120, Size: 64, PartitionId: math.MaxUint64, ExtentId: math.MaxUint64, ExtentOffset: 120, StoreType: proto.InnerData},
	}
	ctx := context.Background()
	sortedEKs := se.NewSortedExtents()
	for _, ek := range eks {
		sortedEKs.Insert(ctx, ek)
	}

	si.ReshuffleInnerDataSet(sortedEKs)
	expect := []*proto.InnerDataSt{
		{FileOffset: 128, Size: 8, Data:data[128:136]},
		{FileOffset: 144, Size: 8, Data:data[144:152]},
	}
	if len(si.innerDataSet) != len(expect) {
		t.Fatalf("number of inner data array mismatch: expect %v, actual %v", len(expect), len(si.innerDataSet))
	}
	for i := 0; i < len(expect); i++ {
		if !reflect.DeepEqual(si.innerDataSet[i], expect[i]) {
			t.Fatalf("innerDataSet[%v] mismatch: expect %v, actual %v", i, expect[i], si.innerDataSet[i])
		}
	}
}

/*
already exist inner data :
     |====|                                file offset:116, size:8,
                |====|                     file offset:128, size:8,
                             |====|        file offset:144, size:8,

sorted eks:
        |=======================|          file offset:120, size:28, type:inner data

expect result:
        |==|                               file offset:120, size:4,
                |====|                     file offset:128, size:8,
                              |==|         file offset:144, size:4,
*/
func TestSortedInnerDataSet_ReshuffleInnerDataSet06(t *testing.T) {
	si := NewSortedInnerDataSet()
	si.Insert(&proto.InnerDataSt{FileOffset: 116, Size: 8, Data:data[116:124]})
	si.Insert(&proto.InnerDataSt{FileOffset: 128, Size: 8, Data:data[128:136]})
	si.Insert(&proto.InnerDataSt{FileOffset: 144, Size: 8, Data:data[144:152]})

	eks := []proto.ExtentKey{
		{FileOffset: 120, Size: 28, PartitionId: math.MaxUint64, ExtentId: math.MaxUint64, ExtentOffset: 120, StoreType: proto.InnerData},
	}
	ctx := context.Background()
	sortedEKs := se.NewSortedExtents()
	for _, ek := range eks {
		sortedEKs.Insert(ctx, ek)
	}

	si.ReshuffleInnerDataSet(sortedEKs)
	expect := []*proto.InnerDataSt{
		{FileOffset: 120, Size: 4, Data:data[120:124]},
		{FileOffset: 128, Size: 8, Data:data[128:136]},
		{FileOffset: 144, Size: 4, Data:data[144:148]},
	}
	if len(si.innerDataSet) != len(expect) {
		t.Fatalf("number of inner data array mismatch: expect %v, actual %v", len(expect), len(si.innerDataSet))
	}
	for i := 0; i < len(expect); i++ {
		if !reflect.DeepEqual(si.innerDataSet[i], expect[i]) {
			t.Fatalf("innerDataSet[%v] mismatch: expect %v, actual %v", i, expect[i], si.innerDataSet[i])
		}
	}
}

func TestSortedInnerDataSet_ReadInnerData(t *testing.T) {
	si := NewSortedInnerDataSet()
	si.Insert(&proto.InnerDataSt{FileOffset: 116, Size: 8, Data:data[116:124]})
	si.Insert(&proto.InnerDataSt{FileOffset: 128, Size: 8, Data:data[128:136]})
	si.Insert(&proto.InnerDataSt{FileOffset: 144, Size: 8, Data:data[144:152]})
	d, err := si.ReadInnerData(118, 2)
	if err != nil {
		t.Errorf("read inner data failed：%v", err)
		return
	}
	if !dataConsistent(118, 2, d) {
		t.Errorf("inner data mismatch, expect:%v, actual:%v", data[118:120], d)
		return
	}

	d, err = si.ReadInnerData(128, 8)
	if err != nil {
		t.Errorf("read inner data failed:%v", err)
		return
	}
	if !dataConsistent(128, 8, d) {
		t.Errorf("inner data mismatch, expect:%v, actual:%v", data[128:136], d)
		return
	}

	d, err = si.ReadInnerData(126, 4)
	if err == nil || err.Error() != "read start file offset out of bound" {
		t.Errorf("error mismatch, expect:read start file offset out of bound, actual:%v", err)
		return
	}

	d, err = si.ReadInnerData(144, 12)
	if err == nil || err.Error() != "read end file offset out of bound" {
		t.Errorf("error mismatch, expect:read end file offset out of bound actual:%v", err)
		return
	}

	d, err = si.ReadInnerData(156, 12)
	if err == nil || err.Error() != "inner data not exist, read out of bound" {
		t.Errorf("error mismatch, expect:inner data not exist, read out of bound, actual:%v", err)
		return
	}

}

func dataConsistent(offset, size uint64, d []byte) bool {
	if len(d) != int(size) {
		return false
	}

	for _, b := range d {
		if b != data[offset] {
			return false
		}
		offset++
	}
	return true
}
