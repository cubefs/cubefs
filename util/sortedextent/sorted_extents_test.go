package sortedextent

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/chubaofs/chubaofs/proto"
)

const (
	maxTruncateEKLen   = 100 * 10000 //
	maxTruncateEKCount = 100
)

func TestAppend01(t *testing.T) {
	ctx := context.Background()
	se := NewSortedExtents()
	se.Append(ctx, proto.ExtentKey{FileOffset: 0, Size: 1000, ExtentId: 1}, 1)
	se.Append(ctx, proto.ExtentKey{FileOffset: 2000, Size: 1000, ExtentId: 2}, 1)
	se.Append(ctx, proto.ExtentKey{FileOffset: 4000, Size: 1000, ExtentId: 3}, 1)
	se.Append(ctx, proto.ExtentKey{FileOffset: 3000, Size: 500, ExtentId: 4}, 1)
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
	delExtents := se.Append(ctx, proto.ExtentKey{FileOffset: 0, Size: 1000, ExtentId: 1}, 1)
	delExtents = se.Append(ctx, proto.ExtentKey{FileOffset: 0, Size: 2000, ExtentId: 1}, 1)
	t.Logf("\ndel: %v\neks: %v", delExtents, se.eks)
	if len(delExtents) != 0 || se.Size() != 2000 {
		t.Fail()
	}
	delExtents = se.Append(ctx, proto.ExtentKey{FileOffset: 0, Size: 2000, ExtentId: 2}, 1)
	t.Logf("\ndel: %v\neks: %v", delExtents, se.eks)
	if len(delExtents) != 1 || delExtents[0].ExtentId != 1 || se.eks[0].ExtentId != 2 {
		t.Fail()
	}
	t.Logf("%v\n", se.Size())
}

func TestAppend03(t *testing.T) {
	ctx := context.Background()
	se := NewSortedExtents()
	delExtents := se.Append(ctx, proto.ExtentKey{FileOffset: 0, Size: 1000, ExtentId: 1}, 1)
	t.Logf("\ndel: %v\neks: %v", delExtents, se.eks)
	delExtents = se.Append(ctx, proto.ExtentKey{FileOffset: 0, Size: 1000, ExtentId: 2}, 1)
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
	delExtents := se.Append(ctx, proto.ExtentKey{FileOffset: 0, Size: 1000, ExtentId: 1}, 1)
	t.Logf("\ndel: %v\neks: %v", delExtents, se.eks)
	delExtents = se.Append(ctx, proto.ExtentKey{FileOffset: 1000, Size: 1000, ExtentId: 2}, 1)
	t.Logf("\ndel: %v\neks: %v", delExtents, se.eks)
	delExtents = se.Append(ctx, proto.ExtentKey{FileOffset: 1500, Size: 4000, ExtentId: 3}, 1)
	t.Logf("\ndel: %v\neks: %v", delExtents, se.eks)
	delExtents = se.Append(ctx, proto.ExtentKey{FileOffset: 500, Size: 4000, ExtentId: 4}, 1)
	t.Logf("\ndel: %v\neks: %v", delExtents, se.eks)
	if len(delExtents) != 1 || delExtents[0].ExtentId != 2 ||
		len(se.eks) != 3 || se.Size() != 5500 ||
		se.eks[0].ExtentId != 1 || se.eks[1].ExtentId != 4 ||
		se.eks[2].ExtentId != 3 {
		t.Fail()
	}
	t.Logf("%v\n", se.Size())
}

func appendCalStartAndInvalid(se *SortedExtents, ek proto.ExtentKey) (startIndex int, invalidExtents []proto.ExtentKey) {
	startIndex = 0
	invalidExtents = make([]proto.ExtentKey, 0)
	endOffset := ek.FileOffset + uint64(ek.Size)

	for idx, key := range se.eks {
		if ek.FileOffset > key.FileOffset {
			startIndex = idx + 1
			continue
		}
		if endOffset >= key.FileOffset+uint64(key.Size) {
			invalidExtents = append(invalidExtents, key)
			continue
		}
		break
	}

	return
}

// This is the case that cause meta node panic  0108.
// Init the eks with product env. As multiple clients op, the eks are in wrong order.
// Append new ek
func TestAppend05(t *testing.T) {
	ctx := context.Background()
	se := NewSortedExtents()

	se.eks = append(se.eks, proto.ExtentKey{FileOffset: 0, PartitionId: 213872, ExtentId: 8, ExtentOffset: 42012672, Size: 131072})
	se.eks = append(se.eks, proto.ExtentKey{FileOffset: 131072, PartitionId: 189047, ExtentId: 24, ExtentOffset: 1477259264, Size: 65536})
	se.eks = append(se.eks, proto.ExtentKey{FileOffset: 196608, PartitionId: 213867, ExtentId: 17572, ExtentOffset: 0, Size: 393216})
	se.eks = append(se.eks, proto.ExtentKey{FileOffset: 589824, PartitionId: 189051, ExtentId: 135461, ExtentOffset: 0, Size: 65536})
	se.eks = append(se.eks, proto.ExtentKey{FileOffset: 655360, PartitionId: 189046, ExtentId: 137124, ExtentOffset: 0, Size: 65536})
	se.eks = append(se.eks, proto.ExtentKey{FileOffset: 720896, PartitionId: 213867, ExtentId: 17572, ExtentOffset: 524288, Size: 655360})
	se.eks = append(se.eks, proto.ExtentKey{FileOffset: 1048576, PartitionId: 201001, ExtentId: 39065, ExtentOffset: 0, Size: 11010048})
	se.eks = append(se.eks, proto.ExtentKey{FileOffset: 5505024, PartitionId: 213867, ExtentId: 17572, ExtentOffset: 5308416, Size: 6584931})
	se.eks = append(se.eks, proto.ExtentKey{FileOffset: 524288, PartitionId: 213873, ExtentId: 37, ExtentOffset: 42237952, Size: 65536})
	se.eks = append(se.eks, proto.ExtentKey{FileOffset: 655360, PartitionId: 200995, ExtentId: 57, ExtentOffset: 437841920, Size: 131072})
	se.eks = append(se.eks, proto.ExtentKey{FileOffset: 786432, PartitionId: 189045, ExtentId: 64, ExtentOffset: 1472925696, Size: 131072})
	se.eks = append(se.eks, proto.ExtentKey{FileOffset: 917504, PartitionId: 189051, ExtentId: 25, ExtentOffset: 1501118464, Size: 131072})
	se.eks = append(se.eks, proto.ExtentKey{FileOffset: 1048576, PartitionId: 201001, ExtentId: 39065, ExtentOffset: 0, Size: 393216})
	se.eks = append(se.eks, proto.ExtentKey{FileOffset: 1441792, PartitionId: 213868, ExtentId: 17688, ExtentOffset: 0, Size: 65536})
	se.eks = append(se.eks, proto.ExtentKey{FileOffset: 1507328, PartitionId: 201001, ExtentId: 39065, ExtentOffset: 458752, Size: 4915200})

	ek := proto.ExtentKey{FileOffset: 1048576, PartitionId: 201001, ExtentId: 39065, ExtentOffset: 0, Size: 11041379}

	start, invalid := appendCalStartAndInvalid(se, ek)
	if start+len(invalid) > len(se.eks) {
		t.Logf("\n*******This ek will panic: cal end:%d, eks len:%d********\n\n", start+len(invalid), len(se.eks))
	}

	se.Append(ctx, ek, 1)
	t.Logf("\neks: %v", se.eks)

	t.Logf("%v\n", se.Size())
}

// This is the case that cause meta node panic  0121.
// Init the eks with product env. As multiple clients op, the eks are in wrong order.
// Append new ek
func TestAppend06(t *testing.T) {
	ctx := context.Background()
	se := NewSortedExtents()

	se.eks = append(se.eks, proto.ExtentKey{FileOffset: 524288, PartitionId: 228513, ExtentId: 37, ExtentOffset: 247836672, Size: 65536})
	se.eks = append(se.eks, proto.ExtentKey{FileOffset: 589824, PartitionId: 231153, ExtentId: 27, ExtentOffset: 31907840, Size: 65536})
	se.eks = append(se.eks, proto.ExtentKey{FileOffset: 1769472, PartitionId: 228514, ExtentId: 33083, ExtentOffset: 0, Size: 65536})
	se.eks = append(se.eks, proto.ExtentKey{FileOffset: 2097152, PartitionId: 231158, ExtentId: 2494, ExtentOffset: 0, Size: 65536})
	se.eks = append(se.eks, proto.ExtentKey{FileOffset: 2162688, PartitionId: 231154, ExtentId: 2441, ExtentOffset: 0, Size: 65536})
	se.eks = append(se.eks, proto.ExtentKey{FileOffset: 2621440, PartitionId: 228513, ExtentId: 33320, ExtentOffset: 0, Size: 655360})
	se.eks = append(se.eks, proto.ExtentKey{FileOffset: 6160384, PartitionId: 228508, ExtentId: 33851, ExtentOffset: 0, Size: 2097152})
	se.eks = append(se.eks, proto.ExtentKey{FileOffset: 8257536, PartitionId: 231152, ExtentId: 2470, ExtentOffset: 1835008, Size: 3342336})
	se.eks = append(se.eks, proto.ExtentKey{FileOffset: 8519680, PartitionId: 228510, ExtentId: 33453, ExtentOffset: 0, Size: 65536})
	se.eks = append(se.eks, proto.ExtentKey{FileOffset: 12320768, PartitionId: 231156, ExtentId: 2473, ExtentOffset: 131072, Size: 7274496})
	se.eks = append(se.eks, proto.ExtentKey{FileOffset: 11599872, PartitionId: 231151, ExtentId: 2432, ExtentOffset: 0, Size: 720896})

	ek := proto.ExtentKey{FileOffset: 12189696, PartitionId: 231156, ExtentId: 2473, ExtentOffset: 0, Size: 7471104}

	start, invalid := appendCalStartAndInvalid(se, ek)
	if start+len(invalid) > len(se.eks) {
		t.Logf("\n*******This ek will panic: cal end:%d, eks len:%d********\n\n", start+len(invalid), len(se.eks))
	}
	se.Append(ctx, ek, 1)
	t.Logf("\neks: %v", se.eks)

	t.Logf("%v\n", se.Size())
}

func TestTruncate01(t *testing.T) {
	ctx := context.Background()
	se := NewSortedExtents()
	delExtents := se.Append(ctx, proto.ExtentKey{FileOffset: 0, Size: 1000, ExtentId: 1}, 1)
	t.Logf("\ndel: %v\neks: %v", delExtents, se.eks)
	delExtents = se.Append(ctx, proto.ExtentKey{FileOffset: 2000, Size: 1000, ExtentId: 2}, 1)
	t.Logf("\ndel: %v\neks: %v", delExtents, se.eks)
	delExtents = se.Truncate(500, 1)
	t.Logf("\ndel: %v\neks: %v", delExtents, se.eks)
	if len(delExtents) != 1 || delExtents[0].ExtentId != 2 ||
		len(se.eks) != 1 || se.eks[0].ExtentId != 1 ||
		se.Size() != 500 {
		t.Fail()
	}
}

func TestTruncate02(t *testing.T) {
	se := NewSortedExtents()
	for i := uint64(0); i < maxTruncateEKLen; i++ {
		se.eks = append(se.eks, proto.ExtentKey{FileOffset: i * 100, Size: 100, PartitionId: i + 1, ExtentId: i + 1})
	}
	t.Logf("before truncate eks len: %v", se.Len())
	start := time.Now()
	delExtents := se.Truncate(500, 1)
	cost := time.Since(start)
	if se.Len() != 5 || len(delExtents) != maxTruncateEKLen-5 {
		t.Errorf("truncate error, se len(exp:%d, now:%d), del len(exp:%d, now:%d)", 5, se.Len(), maxTruncateEKLen-5, len(delExtents))
	}
	if cost > time.Second*3 {
		t.Errorf("truncate %d eks timeout(3s), cost:%v", len(delExtents), cost)
		return
	}
	t.Logf("after  truncate eks len: %v, del len:%d, cost:%v", se.Len(), len(delExtents), cost)
}

func TestTruncate03(t *testing.T) {
	se := NewSortedExtents()

	for i := uint64(0); i < maxTruncateEKLen; i++ {
		se.eks = append(se.eks, proto.ExtentKey{FileOffset: i * 100, Size: 100, PartitionId: i%maxTruncateEKCount + 1, ExtentId: i%maxTruncateEKCount + 1})
	}
	t.Logf("before truncate eks len: %v", se.Len())
	start := time.Now()
	delExtents := se.Truncate(500, 1)
	cost := time.Since(start)
	if se.Len() != 5 || len(delExtents) != maxTruncateEKCount-5 {
		t.Errorf("truncate error, se len(exp:%d, now:%d), del len(exp:%d, now:%d)", 5, se.Len(), maxTruncateEKCount-5, len(delExtents))
	}
	if cost > time.Second*3 {
		t.Errorf("truncate %d eks timeout(3s), cost:%v", len(delExtents), cost)
		return
	}
	t.Logf("after  truncate eks len: %v, del len:%d, cost:%v", se.Len(), len(delExtents), cost)
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

	delEks := make([]proto.MetaDelExtentKey, 0)
	for _, ek := range order {
		delEks = append(delEks, se.Insert(ctx, ek, 1)...)
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
//   6.  |=================| 0_7_1_0_300
//
// Expected result:
//          0_7_1_0_300
//                ↓
//       |=================|
//
// Expected deleted extent keys:
//   1.        |=====| 100_2_1_0_100
//   2.              |=====| 200_3_1_0_100
//   3.        |=====| 100_4_1_0_100
//   4.  |=====| 0_1_1_0_100
//   5.              |=====| 200_5_1_0_100
//   6.        |=====| 100_6_1_0_100
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
			{FileOffset: 0, PartitionId: 7, ExtentId: 1, ExtentOffset: 0, Size: 300},
		}
	)
	// Expected
	var (
		expectedEks = []proto.ExtentKey{
			{FileOffset: 0, PartitionId: 7, ExtentId: 1, ExtentOffset: 0, Size: 300},
		}
		expectedDelEks = []proto.ExtentKey{
			{FileOffset: 100, PartitionId: 2, ExtentId: 1, ExtentOffset: 0, Size: 100},
			{FileOffset: 200, PartitionId: 3, ExtentId: 1, ExtentOffset: 0, Size: 100},
			{FileOffset: 100, PartitionId: 4, ExtentId: 1, ExtentOffset: 0, Size: 100},
			{FileOffset: 0, PartitionId: 1, ExtentId: 1, ExtentOffset: 0, Size: 100},
			{FileOffset: 200, PartitionId: 5, ExtentId: 1, ExtentOffset: 0, Size: 100},
			{FileOffset: 100, PartitionId: 6, ExtentId: 1, ExtentOffset: 0, Size: 100},
		}
	)

	ctx := context.Background()
	se := NewSortedExtents()
	delEks := make([]proto.MetaDelExtentKey, 0)
	for _, ek := range order {
		delEks = append(delEks, se.Insert(ctx, ek, 1)...)
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
		if !reflect.DeepEqual(delEks[i].ExtentKey, expectedDelEks[i]) {
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
//   4.  |======| 0_5_1_0_120
//   5.                       |==| 360_6_1_0_40
//   6.    |=| 40_7_1_0_20
//   7.  |=| 0_8_1_0_40
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

	delEks := make([]proto.MetaDelExtentKey, 0)
	for _, ek := range order {
		delEks = append(delEks, se.Insert(ctx, ek, 1)...)
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
		t.Fatalf("number of delete extents mismatch: expect %v, actual %v, del set:%v", len(expectedDelEks), len(delEks), delEks)
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
	delEks := make([]proto.MetaDelExtentKey, 0)
	for _, ek := range order {
		delEks = append(delEks, se.Insert(ctx, ek, 1)...)
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
	delEks := make([]proto.MetaDelExtentKey, 0)
	for _, ek := range order {
		delEks = append(delEks, se.Insert(ctx, ek, 1)...)
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
//   Insert the same ek repeatedly.
//
// Sample Format:
//   FileOffset_PartitionId_ExtentId_ExtentOffset_Size
//
// Insert order:
//   0.  |=====| 0_1_1_0_100
//   4.  |=====| 0_1_1_0_100
//   5.  |===| 0_1_1_0_60
//   6.    |===| 40_1_1_40_60
//
// Expected result:
//
//       |=====|
//          ↑
//     0_1_1_0_100
//
//
// Expected deleted extent keys:
//   none
//
// Reference:
//       *-----+-----+-----+-----+-----+--->
//       0    100   200   300   400   500
func TestSortedExtents_Insert06(t *testing.T) {
	// Samples
	var (
		order = []proto.ExtentKey{
			{FileOffset: 0, PartitionId: 1, ExtentId: 1, ExtentOffset: 0, Size: 100},
			{FileOffset: 0, PartitionId: 1, ExtentId: 1, ExtentOffset: 0, Size: 100},
			{FileOffset: 0, PartitionId: 1, ExtentId: 1, ExtentOffset: 0, Size: 60},
			{FileOffset: 40, PartitionId: 1, ExtentId: 1, ExtentOffset: 40, Size: 60},
		}
	)
	// Expected
	var (
		expectedEks = []proto.ExtentKey{
			{FileOffset: 0, PartitionId: 1, ExtentId: 1, ExtentOffset: 0, Size: 100},
		}
		expectedDelEks []proto.ExtentKey
	)

	ctx := context.Background()
	se := NewSortedExtents()
	delEks := make([]proto.MetaDelExtentKey, 0)
	for _, ek := range order {
		delEks = append(delEks, se.Insert(ctx, ek, 1)...)
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
//   Recurrence of production environment problem scenarios.
//
// Sample Format:
//   FileOffset_PartitionId_ExtentId_ExtentOffset_Size
//
// Insert order:
//   0.  |==================================| 0_2187_1062_0_12582912
//   4.  |=====| 0_2188_1069_0_16384
//   5.  |=====| 0_2187_2187_0_16384
//   6.              |=====| 32768_2190_6028_0_16384
//
// Expected result:
//   16384_2187_1062_16384_16384
//               ↘       ↙  32768_2190_6028_0_16384
//       |=====|=====|=====|================|
//          ↑                      ↑
//     0_2187_2187_0_16384     49152_2187_1062_49152_12533760
//
//
// Expected deleted extent keys:
//
//       |=====| 0_2188_1069_0_16384
//
// Reference:
//       *-----+-----+-----+-----+-----+--->
//       0    100   200   300   400   500
func TestSortedExtents_Insert07(t *testing.T) {
	// Samples
	var (
		order = []proto.ExtentKey{
			{FileOffset: 0, PartitionId: 2187, ExtentId: 1062, ExtentOffset: 0, Size: 12582912},
			{FileOffset: 0, PartitionId: 2188, ExtentId: 1069, ExtentOffset: 0, Size: 16384},
			{FileOffset: 0, PartitionId: 2187, ExtentId: 2187, ExtentOffset: 0, Size: 16384},
			{FileOffset: 32768, PartitionId: 2190, ExtentId: 6028, ExtentOffset: 0, Size: 16384},
		}
	)
	// Expected
	var (
		expectedEks = []proto.ExtentKey{
			{FileOffset: 0, PartitionId: 2187, ExtentId: 2187, ExtentOffset: 0, Size: 16384},
			{FileOffset: 16384, PartitionId: 2187, ExtentId: 1062, ExtentOffset: 16384, Size: 16384},
			{FileOffset: 32768, PartitionId: 2190, ExtentId: 6028, ExtentOffset: 0, Size: 16384},
			{FileOffset: 49152, PartitionId: 2187, ExtentId: 1062, ExtentOffset: 49152, Size: 12533760},
		}
		expectedDelEks = []proto.ExtentKey{
			{FileOffset: 0, PartitionId: 2188, ExtentId: 1069, ExtentOffset: 0, Size: 16384},
		}
	)

	ctx := context.Background()
	se := NewSortedExtents()
	delEks := make([]proto.MetaDelExtentKey, 0)
	for _, ek := range order {
		delEks = append(delEks, se.Insert(ctx, ek, 1)...)
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
		if !reflect.DeepEqual(delEks[i].ExtentKey, expectedDelEks[i]) {
			t.Fatalf("deleted ek[%v] mismatch: expect %v, actual %v", i, expectedDelEks[i], delEks[i])
		}
	}
}

func TestSortedExtents_InsertAndTruncate(t *testing.T) {
	var operations = `
ExtentsInsert:0_246490_1065_0_8388608
ExtentTruncate:9437184
ExtentsInsert:0_246490_1065_0_9437184
ExtentsInsert:1327104_246491_3949_0_16384
ExtentsInsert:1196032_246487_3957_0_16384
ExtentTruncate:10485760
ExtentsInsert:1343488_246490_1065_1343488_9142272
ExtentTruncate:98304
ExtentTruncate:98304
`
	var err error
	var eks = NewSortedExtents()
	var deletedEks = make([]proto.MetaDelExtentKey, 0)
	for _, operation := range strings.Split(operations, "\n") {
		operation = strings.TrimSpace(operation)
		if len(operation) == 0 {
			continue
		}
		parts := strings.Split(operation, ":")
		if len(parts) != 2 {
			continue
		}
		switch parts[0] {
		case "ExtentsInsert":
			parts := strings.Split(parts[1], "_")
			if len(parts) != 5 {
				continue
			}
			var ek proto.ExtentKey
			if ek.FileOffset, err = strconv.ParseUint(parts[0], 10, 64); err != nil {
				t.Fatalf("Parse FileOffset from %v failed: %v", operation, err)
			}
			if ek.PartitionId, err = strconv.ParseUint(parts[1], 10, 64); err != nil {
				t.Fatalf("Parse PartitionId from %v failed: %v", operation, err)
			}
			if ek.ExtentId, err = strconv.ParseUint(parts[2], 10, 64); err != nil {
				t.Fatalf("Parse ExtentId from %v failed: %v", operation, err)
			}
			if ek.ExtentOffset, err = strconv.ParseUint(parts[3], 10, 64); err != nil {
				t.Fatalf("Parse ExtentOffset from %v failed: %v", operation, err)
			}
			var size uint64
			if size, err = strconv.ParseUint(parts[4], 10, 64); err != nil {
				t.Fatalf("Parse Size from %v failed: %v", operation, err)
			}
			ek.Size = uint32(size)
			deletedEks = append(deletedEks, eks.Insert(context.Background(), ek, 1)...)
		case "ExtentTruncate":
			var size uint64
			if size, err = strconv.ParseUint(parts[1], 10, 64); err != nil {
				t.Fatalf("Parse truncate offset failed: %v", err)
			}
			deletedEks = append(deletedEks, eks.Truncate(size, 1)...)
		}
	}
	eks.Range(func(ek proto.ExtentKey) bool {
		for _, deletedEk := range deletedEks {
			if deletedEk.PartitionId == ek.PartitionId && deletedEk.ExtentId == ek.ExtentId {
				t.Fatalf("ExtentKey %v found in deleted extent keys", ek)
			}
		}
		return true
	})
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
		}, 1)
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
		}, 1)
	}
	b.ReportAllocs()
}

func TestSortedExtents_findEkIndex(t *testing.T) {
	ctx := context.Background()
	se := NewSortedExtents()
	length := 100
	for i := 0; i < length; i++ {
		se.Append(ctx, proto.ExtentKey{
			FileOffset: uint64(i),
		}, 1)
	}
	for i := 0; i < length; i++ {
		ek := &proto.ExtentKey{FileOffset: uint64(i)}
		actualIndex, ok := se.findEkIndex(ek)
		if !ok {
			t.Fatalf("ek[%v] should exist", ek)
		}
		if i != actualIndex {
			t.Fatalf("ek[%v] index expect:%v, actual:%v", ek, i, actualIndex)
		}
	}
}

func TestSortedExtents_Merge1(t *testing.T) {
	ctx := context.Background()
	se := NewSortedExtents()
	srcEks := []proto.ExtentKey{
		{FileOffset: 0, Size: 1, PartitionId: 2, ExtentId: 2},
		{FileOffset: 1, Size: 2, PartitionId: 3, ExtentId: 3},
		{FileOffset: 3, Size: 3, PartitionId: 4, ExtentId: 4},
		{FileOffset: 6, Size: 4, PartitionId: 5, ExtentId: 5},
		{FileOffset: 10, Size: 5, PartitionId: 6, ExtentId: 6},
		{FileOffset: 15, Size: 6, PartitionId: 7, ExtentId: 7},
		{FileOffset: 21, Size: 7, PartitionId: 8, ExtentId: 8},
		{FileOffset: 28, Size: 1, PartitionId: 9, ExtentId: 9},
		{FileOffset: 29, Size: 2, PartitionId: 10, ExtentId: 10},
		{FileOffset: 31, Size: 2, PartitionId: 11, ExtentId: 11},
		{FileOffset: 33, Size: 3, PartitionId: 12, ExtentId: 12},
	}
	for _, ek := range srcEks {
		se.Append(ctx, ek, 1)
	}
	newEk := proto.ExtentKey{FileOffset: 0, Size: 11, PartitionId: 100, ExtentId: 1}
	oldEks := []proto.ExtentKey{
		{FileOffset: 0, Size: 1, PartitionId: 2, ExtentId: 2},
		{FileOffset: 1, Size: 2, PartitionId: 3, ExtentId: 3},
		{FileOffset: 3, Size: 3, PartitionId: 4, ExtentId: 4},
		{FileOffset: 6, Size: 4, PartitionId: 5, ExtentId: 5},
	}
	deleteExtents, merged, msg := se.Merge([]proto.ExtentKey{newEk}, oldEks, 1)
	if !merged {
		t.Fatalf("There should be no error, current error:%v", msg)
	}
	if len(deleteExtents) != len(oldEks) {
		t.Fatalf("deleteExtents length expect:%v ,actual:%v", len(oldEks), len(deleteExtents))
	}
	newEk = proto.ExtentKey{FileOffset: 0, Size: 11, PartitionId: 100, ExtentId: 1}
	oldEks = []proto.ExtentKey{
		{FileOffset: 10, Size: 5, PartitionId: 6, ExtentId: 6},
		{FileOffset: 15, Size: 6, PartitionId: 7, ExtentId: 7},
	}
	deleteExtents, merged, msg = se.Merge([]proto.ExtentKey{newEk}, oldEks, 1)
	if !merged {
		t.Fatalf("There should be no error, current error:%v", msg)
	}
	if len(deleteExtents) != len(oldEks) {
		t.Fatalf("deleteExtents length expect:%v ,actual:%v", len(oldEks), len(deleteExtents))
	}
}

func TestSortedExtents_Merge2(t *testing.T) {
	ctx := context.Background()
	se := NewSortedExtents()
	srcEks := []proto.ExtentKey{
		{FileOffset: 0, Size: 1, PartitionId: 2, ExtentId: 2},
		{FileOffset: 1, Size: 2, PartitionId: 3, ExtentId: 3},
		{FileOffset: 3, Size: 3, PartitionId: 4, ExtentId: 4},
		{FileOffset: 6, Size: 4, PartitionId: 12, ExtentId: 12},
		{FileOffset: 10, Size: 5, PartitionId: 6, ExtentId: 6},
		{FileOffset: 15, Size: 6, PartitionId: 7, ExtentId: 7},
		{FileOffset: 21, Size: 7, PartitionId: 8, ExtentId: 8},
		{FileOffset: 28, Size: 1, PartitionId: 9, ExtentId: 9},
		{FileOffset: 29, Size: 2, PartitionId: 10, ExtentId: 10},
		{FileOffset: 31, Size: 2, PartitionId: 11, ExtentId: 11},
		{FileOffset: 33, Size: 3, PartitionId: 12, ExtentId: 12},
	}
	for _, ek := range srcEks {
		se.Append(ctx, ek, 1)
	}
	newEk := proto.ExtentKey{FileOffset: 0, Size: 11, PartitionId: 100, ExtentId: 1}
	shouldNotContainEk := proto.ExtentKey{FileOffset: 6, Size: 4, PartitionId: 12, ExtentId: 12}
	oldEks := []proto.ExtentKey{
		{FileOffset: 0, Size: 1, PartitionId: 2, ExtentId: 2},
		{FileOffset: 1, Size: 2, PartitionId: 3, ExtentId: 3},
		{FileOffset: 3, Size: 3, PartitionId: 4, ExtentId: 4},
		shouldNotContainEk,
	}
	deleteExtents, merged, msg := se.Merge([]proto.ExtentKey{newEk}, oldEks, 1)
	if !merged {
		t.Fatalf("There should be no error, current error:%v", msg)
	}
	if len(deleteExtents) != len(oldEks)-1 {
		t.Fatalf("deleteExtents length expect:%v ,actual:%v", len(oldEks)-1, len(deleteExtents))
	}
	for _, extent := range deleteExtents {
		if extent.PartitionId == shouldNotContainEk.PartitionId && extent.ExtentId == shouldNotContainEk.ExtentId {
			t.Fatalf("deleteExtents should not contain extentKey:%v", shouldNotContainEk)
		}
	}
}

func TestSortedExtents_Merge3(t *testing.T) {
	ctx := context.Background()
	se := NewSortedExtents()
	srcEks := []proto.ExtentKey{
		{FileOffset: 0, Size: 1, PartitionId: 2, ExtentId: 2},
		{FileOffset: 1, Size: 2, PartitionId: 3, ExtentId: 3},
		{FileOffset: 3, Size: 3, PartitionId: 11, ExtentId: 11},
		{FileOffset: 6, Size: 4, PartitionId: 12, ExtentId: 12},
		{FileOffset: 10, Size: 5, PartitionId: 6, ExtentId: 6},
		{FileOffset: 15, Size: 6, PartitionId: 7, ExtentId: 7},
		{FileOffset: 21, Size: 7, PartitionId: 8, ExtentId: 8},
		{FileOffset: 28, Size: 1, PartitionId: 9, ExtentId: 9},
		{FileOffset: 29, Size: 2, PartitionId: 10, ExtentId: 10},
		{FileOffset: 31, Size: 2, PartitionId: 11, ExtentId: 11},
		{FileOffset: 33, Size: 3, PartitionId: 12, ExtentId: 12},
	}
	for _, ek := range srcEks {
		se.Append(ctx, ek, 1)
	}
	newEk := proto.ExtentKey{FileOffset: 0, Size: 11, PartitionId: 100, ExtentId: 1}
	shouldNotContainEk1 := proto.ExtentKey{FileOffset: 3, Size: 3, PartitionId: 11, ExtentId: 11}
	shouldNotContainEk2 := proto.ExtentKey{FileOffset: 6, Size: 4, PartitionId: 12, ExtentId: 12}
	oldEks := []proto.ExtentKey{
		{FileOffset: 0, Size: 1, PartitionId: 2, ExtentId: 2},
		{FileOffset: 1, Size: 2, PartitionId: 3, ExtentId: 3},
		shouldNotContainEk1,
		shouldNotContainEk2,
	}
	deleteExtents, merged, msg := se.Merge([]proto.ExtentKey{newEk}, oldEks, 1)
	if !merged {
		t.Fatalf("Merge should have no error, current error:%v", msg)
	}
	if len(deleteExtents) != len(oldEks)-2 {
		t.Fatalf("deleteExtents length expect:%v ,actual:%v", len(oldEks)-2, len(deleteExtents))
	}
	for _, extent := range deleteExtents {
		if extent.PartitionId == shouldNotContainEk1.PartitionId && extent.ExtentId == shouldNotContainEk1.ExtentId {
			t.Fatalf("deleteExtents should not contain extentKey:%v", shouldNotContainEk1)
		}
		if extent.PartitionId == shouldNotContainEk2.PartitionId && extent.ExtentId == shouldNotContainEk2.ExtentId {
			t.Fatalf("deleteExtents should not contain extentKey:%v", shouldNotContainEk2)
		}
	}
}

func TestSortedExtents_Merge4(t *testing.T) {
	ctx := context.Background()
	se := NewSortedExtents()
	srcEks := []proto.ExtentKey{
		{FileOffset: 0, Size: 1, PartitionId: 2, ExtentId: 2},
		{FileOffset: 1, Size: 2, PartitionId: 3, ExtentId: 3},
		{FileOffset: 3, Size: 3, PartitionId: 11, ExtentId: 11},
		{FileOffset: 6, Size: 4, PartitionId: 12, ExtentId: 12},
		{FileOffset: 10, Size: 5, PartitionId: 6, ExtentId: 6},
		{FileOffset: 15, Size: 6, PartitionId: 7, ExtentId: 7},
		{FileOffset: 21, Size: 7, PartitionId: 8, ExtentId: 8},
		{FileOffset: 28, Size: 1, PartitionId: 9, ExtentId: 9},
		{FileOffset: 29, Size: 2, PartitionId: 10, ExtentId: 10},
		{FileOffset: 31, Size: 2, PartitionId: 11, ExtentId: 11},
		{FileOffset: 33, Size: 3, PartitionId: 12, ExtentId: 12},
	}
	for _, ek := range srcEks {
		se.Append(ctx, ek, 1)
	}
	newEk := proto.ExtentKey{FileOffset: 0, Size: 18, PartitionId: 100, ExtentId: 1}
	oldEks := []proto.ExtentKey{
		{FileOffset: 4, Size: 3, PartitionId: 11, ExtentId: 11},
		{FileOffset: 6, Size: 4, PartitionId: 12, ExtentId: 12},
		{FileOffset: 10, Size: 5, PartitionId: 6, ExtentId: 6},
		{FileOffset: 15, Size: 6, PartitionId: 7, ExtentId: 7},
	}
	_, merged, msg := se.Merge([]proto.ExtentKey{newEk}, oldEks, 1)
	fmt.Println(msg)
	if merged {
		t.Fatalf("Merge should hava error")
	}
	oldEks = []proto.ExtentKey{
		{FileOffset: 3, Size: 3, PartitionId: 11, ExtentId: 11},
		{FileOffset: 9, Size: 4, PartitionId: 12, ExtentId: 12},
		{FileOffset: 10, Size: 5, PartitionId: 6, ExtentId: 6},
		{FileOffset: 15, Size: 6, PartitionId: 7, ExtentId: 7},
	}
	_, merged, msg = se.Merge([]proto.ExtentKey{newEk}, oldEks, 1)
	fmt.Println(msg)
	if merged {
		t.Fatalf("Merge should hava error")
	}
}

func TestSortedExtents_Merge5(t *testing.T) {
	ctx := context.Background()
	se := NewSortedExtents()
	srcEks := []proto.ExtentKey{
		{FileOffset: 0, Size: 1, PartitionId: 2, ExtentId: 2},
		{FileOffset: 1, Size: 2, PartitionId: 3, ExtentId: 3},
		{FileOffset: 3, Size: 3, PartitionId: 11, ExtentId: 11},
		{FileOffset: 6, Size: 4, PartitionId: 12, ExtentId: 12},
		{FileOffset: 10, Size: 5, PartitionId: 6, ExtentId: 6},
		{FileOffset: 15, Size: 6, PartitionId: 7, ExtentId: 7},
		{FileOffset: 22, Size: 7, PartitionId: 8, ExtentId: 8},
		{FileOffset: 28, Size: 1, PartitionId: 9, ExtentId: 9},
		{FileOffset: 29, Size: 2, PartitionId: 10, ExtentId: 10},
		{FileOffset: 31, Size: 2, PartitionId: 11, ExtentId: 11},
		{FileOffset: 33, Size: 3, PartitionId: 12, ExtentId: 12},
	}
	for _, ek := range srcEks {
		se.Append(ctx, ek, 1)
	}
	newEk := proto.ExtentKey{FileOffset: 4, Size: 18, PartitionId: 100, ExtentId: 1}
	oldEks := []proto.ExtentKey{
		{FileOffset: 3, Size: 3, PartitionId: 11, ExtentId: 11},
		{FileOffset: 6, Size: 4, PartitionId: 12, ExtentId: 12},
		{FileOffset: 10, Size: 5, PartitionId: 6, ExtentId: 6},
		{FileOffset: 15, Size: 6, PartitionId: 7, ExtentId: 7},
	}
	_, merged, msg := se.Merge([]proto.ExtentKey{newEk}, oldEks, 1)
	fmt.Println(msg)
	if merged {
		t.Fatalf("Merge should hava error")
	}
}

func TestSortedExtents_QueryByFileRange(t *testing.T) {

	// 这个方法用于通过全部顺序遍历的方式查询ExtentKey链中符合查询文件数据范围的ExtentKey。用于验证QueryByFileRange方法是否正确
	var querySortedExtentByRangeAll = func(se *SortedExtents, fileOffset uint64, size uint32) (re []proto.ExtentKey) {
		var tmpEK = proto.ExtentKey{
			FileOffset: fileOffset,
			Size:       size,
		}
		se.Range(func(ek proto.ExtentKey) bool {
			if ek.Overlap(&tmpEK) {
				re = append(re, ek)
			}
			return true
		})
		return
	}

	// Sub cases, case name -> case function
	var subCases = map[string]func(t *testing.T){
		"Random_Query": func(t *testing.T) {

			const numOfExtentKeys = 10000
			var se = NewSortedExtents()
			var r = rand.New(rand.NewSource(time.Now().UnixNano()))
			var nextFileOffset uint64
			for i := 0; i < numOfExtentKeys; i++ {
				var randSize = r.Intn(1024)
				// 1/4为空洞跳过，3/4为有效ExtentKey
				if i%4 != 0 {
					se.Insert(context.Background(), proto.ExtentKey{
						FileOffset: nextFileOffset, Size: uint32(randSize), PartitionId: 1, ExtentId: uint64(i),
					}, 1)
				}
				nextFileOffset += uint64(randSize)
			}

			// 随机10000次FileOffset和Size验证查询结果
			const validationCount = 10000
			r = rand.New(rand.NewSource(time.Now().UnixNano()))
			for i := 0; i < validationCount; i++ {
				var randFileOffset = uint64(rand.Int63n(int64(se.Size() + 1024)))
				var randSize = uint32(rand.Int31n(4096))
				var resultEKs = se.QueryByFileRange(randFileOffset, randSize)
				// 检查结果是否与遍历完全一致
				if strictResultEKs := querySortedExtentByRangeAll(se, randFileOffset, randSize); !reflect.DeepEqual(resultEKs, strictResultEKs) {
					t.Fatalf("Query result of file range [%v, %v) validation failed:\n"+
						"  expect: %v\n"+
						"  actual: %v",
						randFileOffset, randFileOffset+uint64(randSize), strictResultEKs, resultEKs)
				}
			}
		},
		"Specific_Scene_1": func(t *testing.T) {
			// Query:                 |-----| [25,35)
			// EKs  :       |-----|     |-----|
			//              10         30
			var se = NewSortedExtents()
			var sampleEKs = []proto.ExtentKey{
				{
					FileOffset: 10,
					Size:       10,
				},
				{
					FileOffset: 30,
					Size:       10,
				},
			}
			for _, sampleEK := range sampleEKs {
				se.Insert(context.Background(), sampleEK, 1)
			}
			var resultEKs = se.QueryByFileRange(25, 10)
			if len(resultEKs) != 1 {
				t.Fatalf("Query result of file range [%v, %v) validation failed: result length mismatch: expeced 1, actual %v", 25, 35, len(resultEKs))
			}
			if !reflect.DeepEqual(resultEKs[0], sampleEKs[1]) {
				t.Fatalf("Query result of file range [%v, %v) validation failed: result[0] mimstach: expeced %v, actual %v", 25, 35, sampleEKs[1], resultEKs[0])
			}
		},
		"Specific_Scene_2": func(t *testing.T) {
			// Query:          |-----| [15,25)
			// EKs  :       |-----|     |-----|
			//              10         30
			var se = NewSortedExtents()
			var sampleEKs = []proto.ExtentKey{
				{
					FileOffset: 10,
					Size:       10,
				},
				{
					FileOffset: 30,
					Size:       10,
				},
			}
			for _, sampleEK := range sampleEKs {
				se.Insert(context.Background(), sampleEK, 1)
			}
			var resultEKs = se.QueryByFileRange(15, 10)
			if len(resultEKs) != 1 {
				t.Fatalf("Query result of file range [%v, %v) validation failed: result length mismatch: expeced 1, actual %v", 15, 25, len(resultEKs))
			}
			if !reflect.DeepEqual(resultEKs[0], sampleEKs[0]) {
				t.Fatalf("Query result of file range [%v, %v) validation failed: result[0] mimstach: expeced %v, actual %v", 15, 25, sampleEKs[0], resultEKs[0])
			}
		},
		"Specific_Scene_3": func(t *testing.T) {
			// Query:               |-| [25,26)
			// EKs  :       |-----|     |-----|
			//              10         30
			var se = NewSortedExtents()
			var sampleEKs = []proto.ExtentKey{
				{
					FileOffset: 10,
					Size:       10,
				},
				{
					FileOffset: 30,
					Size:       10,
				},
			}
			for _, sampleEK := range sampleEKs {
				se.Insert(context.Background(), sampleEK, 1)
			}
			var resultEKs = se.QueryByFileRange(25, 1)
			if len(resultEKs) != 0 {
				t.Fatalf("Query result of file range [%v, %v) validation failed: result length mismatch: expeced 0, actual %v", 25, 26, len(resultEKs))
			}
		},
		"Specific_Scene_4": func(t *testing.T) {
			// Query:                           |-| [40,50)
			// EKs  :       |-----|     |-----|
			//              10         30
			var se = NewSortedExtents()
			var sampleEKs = []proto.ExtentKey{
				{
					FileOffset: 10,
					Size:       10,
				},
				{
					FileOffset: 30,
					Size:       10,
				},
			}
			for _, sampleEK := range sampleEKs {
				se.Insert(context.Background(), sampleEK, 1)
			}
			var resultEKs = se.QueryByFileRange(40, 10)
			if len(resultEKs) != 0 {
				t.Fatalf("Query result of file range [%v, %v) validation failed: result length mismatch: expeced 0, actual %v", 40, 50, len(resultEKs))
			}
		},
	}

	for name, subCase := range subCases {
		t.Run(name, subCase)
	}
}

// BenchmarkSortedExtents_VisitByFileRange 用来验证在长度由400000(40万)个ExtentKey组成的EK链中,
// 使用 SortedExtent.VisitByFileRange 方法查找符合指定文件范围(FileRange)的性能。
func BenchmarkSortedExtents_VisitByFileRange(b *testing.B) {
	se := NewSortedExtents()
	const numOfExtentKeys = 400000
	for i := 0; i < numOfExtentKeys; i++ {
		se.Insert(context.Background(), proto.ExtentKey{FileOffset: uint64(i * 100), Size: 100, PartitionId: 1, ExtentId: uint64(i)}, 1)
	}
	var ran = rand.New(rand.NewSource(time.Now().UnixNano()))
	var offsets = make([]uint64, b.N)
	for i := 0; i < b.N; i++ {
		offsets[i] = uint64(ran.Intn(numOfExtentKeys * 100))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		se.VisitByFileRange(offsets[i], 50, func(ek proto.ExtentKey) bool {
			return true
		})
	}
}

// BenchmarkSortedExtents_Range 用来验证在长度由400000(40万)个ExtentKey组成的EK链中,
// 使用 SortedExtent.Range 方法查找符合指定文件范围(FileRange)的性能。
func BenchmarkSortedExtents_Range(b *testing.B) {
	se := NewSortedExtents()
	const numOfExtentKeys = 400000
	for i := 0; i < numOfExtentKeys; i++ {
		se.Insert(context.Background(), proto.ExtentKey{FileOffset: uint64(i * 100), Size: 100, PartitionId: 1, ExtentId: uint64(i)}, 1)
	}
	var ran = rand.New(rand.NewSource(time.Now().UnixNano()))
	var offsets = make([]uint64, b.N)
	for i := 0; i < b.N; i++ {
		offsets[i] = uint64(ran.Intn(numOfExtentKeys * 100))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var prevOverlapped bool
		se.Range(func(ek proto.ExtentKey) bool {
			overlapped := ek.Overlap(&proto.ExtentKey{FileOffset: offsets[i], Size: 50})
			if !overlapped && prevOverlapped {
				return false
			}
			prevOverlapped = overlapped
			return true
		})
	}
}
