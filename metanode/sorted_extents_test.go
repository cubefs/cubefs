package metanode

import (
	"context"
	"reflect"
	"testing"

	"github.com/chubaofs/chubaofs/proto"
)

func TestAppend01(t *testing.T) {
	ctx := context.Background()
	se := NewSortedExtents()
	se.Append(ctx, proto.ExtentKey{FileOffset: 0, Size: 1000, ExtentId: 1})
	se.Append(ctx, proto.ExtentKey{FileOffset: 2000, Size: 1000, ExtentId: 2})
	se.Append(ctx, proto.ExtentKey{FileOffset: 4000, Size: 1000, ExtentId: 3})
	se.Append(ctx, proto.ExtentKey{FileOffset: 3000, Size: 500, ExtentId: 4})
	t.Logf("\neks: %v\n", se.eks)
	if se.Size() != 5000 || len(se.eks) != 4 || se.eks[2].ExtentId != 4 {
		t.Fail()
	}
	t.Logf("%v\n", se.Size())
}

// The same extent file is extended
func TestAppend02(t *testing.T) {
	ctx := context.Background()
	se := NewSortedExtents()
	delExtents := se.Append(ctx, proto.ExtentKey{FileOffset: 0, Size: 1000, ExtentId: 1})
	delExtents = se.Append(ctx, proto.ExtentKey{FileOffset: 0, Size: 2000, ExtentId: 1})
	t.Logf("\ndel: %v\neks: %v", delExtents, se.eks)
	if len(delExtents) != 0 || se.Size() != 2000 {
		t.Fail()
	}
	delExtents = se.Append(ctx, proto.ExtentKey{FileOffset: 0, Size: 2000, ExtentId: 2})
	t.Logf("\ndel: %v\neks: %v", delExtents, se.eks)
	if len(delExtents) != 1 || delExtents[0].ExtentId != 1 || se.eks[0].ExtentId != 2 {
		t.Fail()
	}
	t.Logf("%v\n", se.Size())
}

func TestAppend03(t *testing.T) {
	ctx := context.Background()
	se := NewSortedExtents()
	delExtents := se.Append(ctx, proto.ExtentKey{FileOffset: 0, Size: 1000, ExtentId: 1})
	t.Logf("\ndel: %v\neks: %v", delExtents, se.eks)
	delExtents = se.Append(ctx, proto.ExtentKey{FileOffset: 0, Size: 1000, ExtentId: 2})
	t.Logf("\ndel: %v\neks: %v", delExtents, se.eks)
	if len(delExtents) != 1 || delExtents[0].ExtentId != 1 ||
		se.eks[0].ExtentId != 2 || se.Size() != 1000 {
		t.Fail()
	}
}

// This is the case when multiple clients are writing to the same file
// with an overlapping file range. The final file data is not guaranteed
// for such case, but we should be aware of what the extents look like.
func TestAppend04(t *testing.T) {
	ctx := context.Background()
	se := NewSortedExtents()
	delExtents := se.Append(ctx, proto.ExtentKey{FileOffset: 0, Size: 1000, ExtentId: 1})
	t.Logf("\ndel: %v\neks: %v", delExtents, se.eks)
	delExtents = se.Append(ctx, proto.ExtentKey{FileOffset: 1000, Size: 1000, ExtentId: 2})
	t.Logf("\ndel: %v\neks: %v", delExtents, se.eks)
	delExtents = se.Append(ctx, proto.ExtentKey{FileOffset: 1500, Size: 4000, ExtentId: 3})
	t.Logf("\ndel: %v\neks: %v", delExtents, se.eks)
	delExtents = se.Append(ctx, proto.ExtentKey{FileOffset: 500, Size: 4000, ExtentId: 4})
	t.Logf("\ndel: %v\neks: %v", delExtents, se.eks)
	if len(delExtents) != 1 || delExtents[0].ExtentId != 2 ||
		len(se.eks) != 3 || se.Size() != 5500 ||
		se.eks[0].ExtentId != 1 || se.eks[1].ExtentId != 4 ||
		se.eks[2].ExtentId != 3 {
		t.Fail()
	}
	t.Logf("%v\n", se.Size())
}

func TestTruncate01(t *testing.T) {
	ctx := context.Background()
	se := NewSortedExtents()
	delExtents := se.Append(ctx, proto.ExtentKey{FileOffset: 0, Size: 1000, ExtentId: 1})
	t.Logf("\ndel: %v\neks: %v", delExtents, se.eks)
	delExtents = se.Append(ctx, proto.ExtentKey{FileOffset: 2000, Size: 1000, ExtentId: 2})
	t.Logf("\ndel: %v\neks: %v", delExtents, se.eks)
	delExtents = se.Truncate(500)
	t.Logf("\ndel: %v\neks: %v", delExtents, se.eks)
	if len(delExtents) != 1 || delExtents[0].ExtentId != 2 ||
		len(se.eks) != 1 || se.eks[0].ExtentId != 1 ||
		se.Size() != 500 {
		t.Fail()
	}
}

// Scenario:
//   Inserts 5 non-overlapping extent key out of order.
//
// Sample Format:
//   FileOffset_PartitionId_ExtentId_ExtentOffset_Size
//
// Insert order:
//   0.        |=====| 100_2_1_0_100
//   1.                    |=| 300_4_1_0_20
//   2.  |=====| 0_1_1_0_100
//   3.              |===| 200_3_1_0_60
//   4.                          |=| 400_4_1_0_20
//
// Expected result:
//       |=====|=====|===| |=|   |=|
//          ↑     ↑    ↑    ↑     ↑
//         ek1   ek2  ek3  ek4   ek5
//
//    ek1(0_1_1_0_100)
//    ek2(100_2_1_0_1000)
//    ek3(200_3_1_0_60)
//    ek4(300_4_1_0_20)
//    ek5(400_4_1_0_20)
//
// Expected deleted extent keys:
//       none
//
// Reference:
//       *-----+-----+-----+-----+-----+--->
//       0    100   200   300   400   500
func TestSortedExtents_Insert01(t *testing.T) {
	// Samples
	var (
		order = []proto.ExtentKey{
			{FileOffset: 100, PartitionId: 2, ExtentId: 1, ExtentOffset: 0, Size: 100},
			{FileOffset: 300, PartitionId: 4, ExtentId: 1, ExtentOffset: 0, Size: 20},
			{FileOffset: 0, PartitionId: 1, ExtentId: 1, ExtentOffset: 0, Size: 100},
			{FileOffset: 200, PartitionId: 3, ExtentId: 1, ExtentOffset: 0, Size: 60},
			{FileOffset: 400, PartitionId: 4, ExtentId: 1, ExtentOffset: 0, Size: 20},
		}
	)
	// Expected
	var (
		expectedEks = []proto.ExtentKey{
			{FileOffset: 0, PartitionId: 1, ExtentId: 1, ExtentOffset: 0, Size: 100},
			{FileOffset: 100, PartitionId: 2, ExtentId: 1, ExtentOffset: 0, Size: 100},
			{FileOffset: 200, PartitionId: 3, ExtentId: 1, ExtentOffset: 0, Size: 60},
			{FileOffset: 300, PartitionId: 4, ExtentId: 1, ExtentOffset: 0, Size: 20},
			{FileOffset: 400, PartitionId: 4, ExtentId: 1, ExtentOffset: 0, Size: 20},
		}
		expectedDelEks []proto.ExtentKey
	)

	ctx := context.Background()
	se := NewSortedExtents()

	delEks := make([]proto.ExtentKey, 0)
	for _, ek := range order {
		delEks = append(delEks, se.Insert(ctx, ek)...)
	}

	// Validate result
	if len(se.eks) != len(expectedEks) {
		t.Fatalf("number of ek mismatch: expect %v, actual %v", len(expectedEks), len(se.eks))
	}
	for i := 0; i < len(expectedEks); i++ {
		if !reflect.DeepEqual(se.eks[i], expectedEks[i]) {
			t.Fatalf("ek[%v] mismatch: expect %v, actual %v", i, expectedEks[i], se.eks[i])
		}
	}

	if len(delEks) != len(expectedDelEks) {
		t.Fatalf("number of delete extents mismatch: expect %v, actual %v", len(expectedDelEks), len(delEks))
	}
}

// Scenario:
//   Multiple times to completely cover the same extent key.
//
// Sample Format:
//   FileOffset_PartitionId_ExtentId_ExtentOffset_Size
//
// Insert order:
//   0.  |=====| O_1_1_0_100
//   1.        |=====| 100_2_1_0_100
//   2.              |=====| 200_3_1_0_100
//   3.        |=====| 100_4_1_0_100
//   4.              |=====| 200_5_1_0_100
//   5.        |=====| 100_6_1_0_100
//
// Expected result:
//          100_6_1_0_100
//                ↓
//       |=====|=====|=====|
//          ↗           ↖
//    0_1_1_0_100   200_5_1_0_100
//
// Expected deleted extent keys:
//   1.        |=====| 100_2_1_0_100
//   2.              |=====| 200_3_1_0_100
//   2.        |=====| 100_4_1_0_100
//
// Reference:
//       *-----+-----+-----+-----+-----+--->
//       0    100   200   300   400   500
func TestSortedExtents_Insert02(t *testing.T) {
	// Samples
	var (
		order = []proto.ExtentKey{
			{FileOffset: 0, PartitionId: 1, ExtentId: 1, ExtentOffset: 0, Size: 100},
			{FileOffset: 100, PartitionId: 2, ExtentId: 1, ExtentOffset: 0, Size: 100},
			{FileOffset: 200, PartitionId: 3, ExtentId: 1, ExtentOffset: 0, Size: 100},
			{FileOffset: 100, PartitionId: 4, ExtentId: 1, ExtentOffset: 0, Size: 100},
			{FileOffset: 200, PartitionId: 5, ExtentId: 1, ExtentOffset: 0, Size: 100},
			{FileOffset: 100, PartitionId: 6, ExtentId: 1, ExtentOffset: 0, Size: 100},
		}
	)
	// Expected
	var (
		expectedEks = []proto.ExtentKey{
			{FileOffset: 0, PartitionId: 1, ExtentId: 1, ExtentOffset: 0, Size: 100},
			{FileOffset: 100, PartitionId: 6, ExtentId: 1, ExtentOffset: 0, Size: 100},
			{FileOffset: 200, PartitionId: 5, ExtentId: 1, ExtentOffset: 0, Size: 100},
		}
		expectedDelEks = []proto.ExtentKey{
			{FileOffset: 100, PartitionId: 2, ExtentId: 1, ExtentOffset: 0, Size: 100},
			{FileOffset: 200, PartitionId: 3, ExtentId: 1, ExtentOffset: 0, Size: 100},
			{FileOffset: 100, PartitionId: 4, ExtentId: 1, ExtentOffset: 0, Size: 100},
		}
	)

	ctx := context.Background()
	se := NewSortedExtents()
	delEks := make([]proto.ExtentKey, 0)
	for _, ek := range order {
		delEks = append(delEks, se.Insert(ctx, ek)...)
	}

	// Validate result
	if len(se.eks) != len(expectedEks) {
		t.Fatalf("number of ek mismatch: expect %v, actual %v", len(expectedEks), len(se.eks))
	}
	for i := 0; i < len(expectedEks); i++ {
		if !reflect.DeepEqual(se.eks[i], expectedEks[i]) {
			t.Fatalf("ek[%v] mismatch: expect %v, actual %v", i, expectedEks[i], se.eks[i])
		}
	}

	if len(delEks) != len(expectedDelEks) {
		t.Fatalf("number of delete extents mismatch: expect %v, actual %v", len(expectedDelEks), len(delEks))
	}

	for i := 0; i < len(expectedDelEks); i++ {
		if !reflect.DeepEqual(delEks[i], expectedDelEks[i]) {
			t.Fatalf("deleted ek[%v] mismatch: expect %v, actual %v", i, expectedDelEks[i], delEks[i])
		}
	}
}

// Scenario:
//  Complex insert scene.
//  1. Insert a new ek that affects multiple but does not completely cover the existing ek.
//  2. Insert a new ek that affects multiple at the same time and will cause the existing ek to completely fail.
//  3. Insert a non-continuous ek at the end.
//  4. Insert a new ek that will split the existing ek.
//
// Sample Format:
//   FileOffset_PartitionId_ExtentId_ExtentOffset_Size
//
// Insert order:
//   0.  |=====| 0_1_1_0_100
//   1.        |=====| 100_2_1_0_100
//   2.              |=====| 200_3_1_0_100
//   3.     |=====| 60_4_1_0_100
//   4.  |======| 0_6_1_0_120
//   5.                       |==| 360_7_1_0_40
//   6.    |=| 40_8_1_0_20
//   7.  |=| 0_9_1_0_40
//
// Expected result:
//     ek1   ek3  ek5
//       ↘    ↓    ↓
//       |=|=|==|=|==|=====|  |==|
//          ↑    4      ↑      ↑
//         ek2  ek3    ek6    ek7
//  ek1(0_8_1_0_40)
//  ek2(40_7_1_0_20)
//  ek3(60_5_1_60_60)
//  ek4(120_4_1_60_40)
//  ek5(160_2_1_60_40)
//  ek6(200_3_1_0_100)
//  ek7(360_6_1_0_40)
//
// Expected deleted extent keys:
//    1. |=====| 0_1_1_0_100
//
// Reference:
//       *-----+-----+-----+-----+-----+--->
//       0    100   200   300   400   500
func TestSortedExtents_Insert03(t *testing.T) {
	// Samples
	var (
		order = []proto.ExtentKey{
			{FileOffset: 0, ExtentOffset: 0, Size: 100, PartitionId: 1, ExtentId: 1},
			{FileOffset: 100, ExtentOffset: 0, Size: 100, PartitionId: 2, ExtentId: 1},
			{FileOffset: 200, ExtentOffset: 0, Size: 100, PartitionId: 3, ExtentId: 1},
			{FileOffset: 60, ExtentOffset: 0, Size: 100, PartitionId: 4, ExtentId: 1},
			{FileOffset: 0, ExtentOffset: 0, Size: 120, PartitionId: 5, ExtentId: 1},
			{FileOffset: 360, ExtentOffset: 0, Size: 40, PartitionId: 6, ExtentId: 1},
			{FileOffset: 40, ExtentOffset: 0, Size: 20, PartitionId: 7, ExtentId: 1},
			{FileOffset: 0, ExtentOffset: 0, Size: 40, PartitionId: 8, ExtentId: 1},
		}
	)
	// Expected
	var (
		expectedEks = []proto.ExtentKey{
			{FileOffset: 0, PartitionId: 8, ExtentId: 1, ExtentOffset: 0, Size: 40},
			{FileOffset: 40, PartitionId: 7, ExtentId: 1, ExtentOffset: 0, Size: 20},
			{FileOffset: 60, PartitionId: 5, ExtentId: 1, ExtentOffset: 60, Size: 60},
			{FileOffset: 120, PartitionId: 4, ExtentId: 1, ExtentOffset: 60, Size: 40},
			{FileOffset: 160, PartitionId: 2, ExtentId: 1, ExtentOffset: 60, Size: 40},
			{FileOffset: 200, PartitionId: 3, ExtentId: 1, ExtentOffset: 0, Size: 100},
			{FileOffset: 360, PartitionId: 6, ExtentId: 1, ExtentOffset: 0, Size: 40},
		}
		expectedEksSize uint64 = 400
		expectedDelEks         = []proto.ExtentKey{
			{PartitionId: 1, ExtentId: 1},
		}
	)

	ctx := context.Background()
	se := NewSortedExtents()

	delEks := make([]proto.ExtentKey, 0)
	for _, ek := range order {
		delEks = append(delEks, se.Insert(ctx, ek)...)
	}
	// Validate result
	if len(se.eks) != len(expectedEks) {
		t.Fatalf("number of ek mismatch: expect %v, actual %v", len(expectedEks), len(se.eks))
	}
	if se.Size() != expectedEksSize {
		t.Fatalf("size of eks mismatch: expect %v, actual %v", expectedEksSize, se.Size())
	}
	for i := 0; i < len(expectedEks); i++ {
		if !reflect.DeepEqual(se.eks[i], expectedEks[i]) {
			t.Fatalf("ek[%v] mismatch: expect %v, actual %v", i, expectedEks[i], se.eks[i])
		}
	}

	if len(delEks) != len(expectedDelEks) {
		t.Fatalf("number of delete extents mismatch: expect %v, actual %v", len(expectedDelEks), len(delEks))
	}

	for i := 0; i < len(expectedDelEks); i++ {
		if delEks[i].PartitionId != expectedDelEks[i].PartitionId ||
			delEks[i].ExtentId != expectedDelEks[i].ExtentId {
			t.Fatalf("deleted ek[%v] mismatch: expect %v_%v, actual %v_%v",
				i, expectedDelEks[i].PartitionId, expectedDelEks[i].ExtentId, delEks[i].PartitionId, delEks[i].ExtentId)
		}
	}
}

// Scenario:
//   Insert a new ek with the same upper boundary as the existing ek.
//
// Sample Format:
//   FileOffset_PartitionId_ExtentId_ExtentOffset_Size
//
// Insert order:
//   0.  |=====| 0_1_1_0_100
//   1.        |=====| 100_2_1_0_100
//   2.              |=====| 200_3_1_0_100
//   3.        |==| 100_4_1_0_40
//   4.              |==| 200_5_1_0_40
//
// Expected result:
//    100_4_1_0_40    200_5_1_0_40
//              ↘      ↙
//       |=====|==|==|==|==|
//         ↗        ↑     ↖
//  0_1_1_0_100     ↑   240_3_1_40_60
//            140_2_1_40_60
//
// Expected deleted extent keys:
//   none
//
// Reference:
//       *-----+-----+-----+-----+-----+--->
//       0    100   200   300   400   500
func TestSortedExtents_Insert04(t *testing.T) {
	// Samples
	var (
		order = []proto.ExtentKey{
			{FileOffset: 0, PartitionId: 1, ExtentId: 1, ExtentOffset: 0, Size: 100},
			{FileOffset: 100, PartitionId: 2, ExtentId: 1, ExtentOffset: 0, Size: 100},
			{FileOffset: 200, PartitionId: 3, ExtentId: 1, ExtentOffset: 0, Size: 100},
			{FileOffset: 100, PartitionId: 4, ExtentId: 1, ExtentOffset: 0, Size: 40},
			{FileOffset: 200, PartitionId: 5, ExtentId: 1, ExtentOffset: 0, Size: 40},
		}
	)
	// Expected
	var (
		expectedEks = []proto.ExtentKey{
			{FileOffset: 0, PartitionId: 1, ExtentId: 1, ExtentOffset: 0, Size: 100},
			{FileOffset: 100, PartitionId: 4, ExtentId: 1, ExtentOffset: 0, Size: 40},
			{FileOffset: 140, PartitionId: 2, ExtentId: 1, ExtentOffset: 40, Size: 60},
			{FileOffset: 200, PartitionId: 5, ExtentId: 1, ExtentOffset: 0, Size: 40},
			{FileOffset: 240, PartitionId: 3, ExtentId: 1, ExtentOffset: 40, Size: 60},
		}
		expectedDelEks []proto.ExtentKey
	)

	ctx := context.Background()
	se := NewSortedExtents()
	delEks := make([]proto.ExtentKey, 0)
	for _, ek := range order {
		delEks = append(delEks, se.Insert(ctx, ek)...)
	}

	// Validate result
	if len(se.eks) != len(expectedEks) {
		t.Fatalf("number of ek mismatch: expect %v, actual %v", len(expectedEks), len(se.eks))
	}
	for i := 0; i < len(expectedEks); i++ {
		if !reflect.DeepEqual(se.eks[i], expectedEks[i]) {
			t.Fatalf("ek[%v] mismatch: expect %v, actual %v", i, expectedEks[i], se.eks[i])
		}
	}

	if len(delEks) != len(expectedDelEks) {
		t.Fatalf("number of delete extents mismatch: expect %v, actual %v", len(expectedDelEks), len(delEks))
	}

	for i := 0; i < len(expectedDelEks); i++ {
		if !reflect.DeepEqual(delEks[i], expectedDelEks[i]) {
			t.Fatalf("deleted ek[%v] mismatch: expect %v, actual %v", i, expectedDelEks[i], delEks[i])
		}
	}
}

// Scenario:
//   Insert the ek that is continuous with the adjacent ek, the new ek will merge with the adjacent ek.
//
// Sample Format:
//   FileOffset_PartitionId_ExtentId_ExtentOffset_Size
//
// Insert order:
//   0.  |=====| 0_1_1_0_100
//   1.              |=====| 200_1_1_200_100
//   2.        |=====| 100_1_1_100_100
//   3.                          |=====| 400_1_1_400_100
//   4.                                |=====| 500_2_1024_0_100
//   5.                    |=====| 300_1_1_300_100
//
// Expected result:
//
//       |=============================|=====|
//                       ↑                ↑
//                  0_1_1_0_500        500_2_1024_0_100
//
//
// Expected deleted extent keys:
//   none
//
// Reference:
//       *-----+-----+-----+-----+-----+--->
//       0    100   200   300   400   500
func TestSortedExtents_Insert05(t *testing.T) {
	// Samples
	var (
		order = []proto.ExtentKey{
			{FileOffset: 0, PartitionId: 1, ExtentId: 1, ExtentOffset: 0, Size: 100},
			{FileOffset: 200, PartitionId: 1, ExtentId: 1, ExtentOffset: 200, Size: 100},
			{FileOffset: 100, PartitionId: 1, ExtentId: 1, ExtentOffset: 100, Size: 100},
			{FileOffset: 400, PartitionId: 1, ExtentId: 1, ExtentOffset: 400, Size: 100},
			{FileOffset: 500, PartitionId: 2, ExtentId: 1024, ExtentOffset: 0, Size: 100},
			{FileOffset: 300, PartitionId: 1, ExtentId: 1, ExtentOffset: 300, Size: 100},
		}
	)
	// Expected
	var (
		expectedEks = []proto.ExtentKey{
			{FileOffset: 0, PartitionId: 1, ExtentId: 1, ExtentOffset: 0, Size: 500},
			{FileOffset: 500, PartitionId: 2, ExtentId: 1024, ExtentOffset: 0, Size: 100},
		}
		expectedDelEks []proto.ExtentKey
	)

	ctx := context.Background()
	se := NewSortedExtents()
	delEks := make([]proto.ExtentKey, 0)
	for _, ek := range order {
		delEks = append(delEks, se.Insert(ctx, ek)...)
	}

	// Validate result
	if len(se.eks) != len(expectedEks) {
		t.Fatalf("number of ek mismatch: expect %v, actual %v", len(expectedEks), len(se.eks))
	}
	for i := 0; i < len(expectedEks); i++ {
		if !reflect.DeepEqual(se.eks[i], expectedEks[i]) {
			t.Fatalf("ek[%v] mismatch: expect %v, actual %v", i, expectedEks[i], se.eks[i])
		}
	}

	if len(delEks) != len(expectedDelEks) {
		t.Fatalf("number of delete extents mismatch: expect %v, actual %v", len(expectedDelEks), len(delEks))
	}

	for i := 0; i < len(expectedDelEks); i++ {
		if !reflect.DeepEqual(delEks[i], expectedDelEks[i]) {
			t.Fatalf("deleted ek[%v] mismatch: expect %v, actual %v", i, expectedDelEks[i], delEks[i])
		}
	}
}

func BenchmarkSortedExtents_Insert(b *testing.B) {
	ctx := context.Background()
	se := NewSortedExtents()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		se.Insert(ctx, proto.ExtentKey{
			FileOffset:   uint64(i * 100),
			PartitionId:  1,
			ExtentId:     uint64(i + 1),
			ExtentOffset: 0,
			Size:         100,
		})
	}
	b.ReportAllocs()
}

func BenchmarkSortedExtents_Append(b *testing.B) {
	ctx := context.Background()
	se := NewSortedExtents()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		se.Append(ctx, proto.ExtentKey{
			FileOffset:   uint64(i * 100),
			PartitionId:  1,
			ExtentId:     uint64(i + 1),
			ExtentOffset: 0,
			Size:         100,
		})
	}
	b.ReportAllocs()
}
