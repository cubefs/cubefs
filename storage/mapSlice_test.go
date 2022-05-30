package storage

import (
	"bytes"
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"path"
	"reflect"
	"runtime"
	"sync"
	"testing"
)

func initExtentInfo(id uint64) ExtentInfoBlock {
	return ExtentInfoBlock{
		FileID: id,
		Size:   id,
	}
}

func TestBaseFunc(t *testing.T) {
	ms := NewMapSlice(1)
	var id uint64 = 1
	ms.Store(id, initExtentInfo(id))
	assertEqual(t, 1, ms.Len())

	ms.Delete(id)
	assertEqual(t, 1, ms.Len())

	var id1 uint64 = 65
	ms.Store(id1, initExtentInfo(id1))
	assertEqual(t, 2, ms.Len())
	var id2 uint64 = 1e8
	ms.Store(id2, initExtentInfo(id2))
	assertEqual(t, 3, ms.Len())
	dObj, ok := ms.Load(id2)
	assertEqual(t, true, ok)
	assertEqual(t, id2, dObj[FileID])
	assertEqual(t, id2, dObj[Size])
}

func TestTinyExtent(t *testing.T) {
	ms := NewMapSlice(1)
	for i := 1; i < TinyExtentCount+1; i++ {
		ms.Store(uint64(i), initExtentInfo(uint64(i)))
	}
	assertEqual(t, TinyExtentCount, ms.Len())
	ms.RangeTinyExtent(func(extentID uint64, ei *ExtentInfoBlock) {
		if extentID == 0 {
			t.Errorf("extentID:%v should not exist.", extentID)
		} else {
			assertEqual(t, extentID, (*ei)[FileID])
		}
	})
}

func TestNormalExtent(t *testing.T) {
	ms := NewMapSlice(1)
	normalCount := 1000
	for i := TinyExtentCount + 1; i < normalCount+TinyExtentCount+1; i++ {
		ms.Store(uint64(i), initExtentInfo(uint64(i)))
	}
	assertEqual(t, normalCount, ms.Len())

	ms.RangeTinyExtent(func(extentID uint64, ei *ExtentInfoBlock) {
		assertEqual(t, EmptyExtentBlock, *ei)
	})
	var deleteExtentId = [5]uint64{66, 68, 69, 100, 225}
	for _, deid := range deleteExtentId {
		ms.Delete(deid)
	}
	assertEqual(t, normalCount-len(deleteExtentId), ms.Len())
	ms.RangeNormalExtent(func(extentID uint64, ei *ExtentInfoBlock) {
		for _, deid := range deleteExtentId {
			if extentID == deid {
				t.Errorf("extentID:%v should not exist.", extentID)
			}
		}
		assertEqual(t, extentID, (*ei)[FileID])
	})
}

func TestDelete(t *testing.T) {
	ms := NewMapSlice(1)
	var extentId uint64 = 999
	ms.Store(extentId, initExtentInfo(extentId))
	ms.Delete(extentId)
	_, ok := ms.Load(extentId)
	assertEqual(t, false, ok)
}

func TestReduce(t *testing.T) {
	var maxSize = 5000
	var minExtentID = 100
	ms := NewMapSlice(1)
	for i := 1; i < maxSize; i++ {
		ms.Store(uint64(i), initExtentInfo(uint64(i)))
	}
	fmt.Println("begin delete")
	var deleteCnt int
	sumCnt := ms.Len()
	for i := maxSize - 1; i > minExtentID; i -= 2 {
		_, ok := ms.Load(uint64(i))
		if ok {
			ms.Delete(uint64(i))
			deleteCnt++
			ms.Range(func(extentID uint64, ei *ExtentInfoBlock) {
				assertEqual(t, extentID, (*ei)[FileID])
			})
		}
	}
	assertEqual(t, sumCnt-deleteCnt, ms.Len())

	ms.Range(func(extentID uint64, ei *ExtentInfoBlock) {
		eiLoad, _ := ms.Load(extentID)
		assertEqual(t, extentID, (*eiLoad)[FileID])
	})
	for i := maxSize - 1; i > minExtentID; i -= 5 {
		_, ok := ms.Load(uint64(i))
		if ok {
			ms.Delete(uint64(i))
			deleteCnt++
			ms.Range(func(extentID uint64, ei *ExtentInfoBlock) {
				assertEqual(t, extentID, (*ei)[FileID])
			})
		}
	}
	assertEqual(t, sumCnt-deleteCnt, ms.Len())
}

func TestRandomReduce(t *testing.T) {
	ms := NewMapSlice(1)
	seq := []uint64{65, 66, 67, 89, 100}
	for _, extentId := range seq {
		ms.Store(extentId, initExtentInfo(extentId))
	}
	ms.Range(func(extentID uint64, ei *ExtentInfoBlock) {
		assertEqual(t, extentID, ei[FileID])
	})
	ms.Delete(89)
	ms.Range(func(extentID uint64, ei *ExtentInfoBlock) {
		assertEqual(t, extentID, ei[FileID])
	})

	maxExtentId := 1000
	for i := 1; i <= maxExtentId; i++ {
		ms.Store(uint64(i), initExtentInfo(uint64(i)))
	}

	deleteCnt := 0
	for i := 1; i <= maxExtentId; i++ {
		if i%5 == 0 {
			ms.Delete(uint64(i))
			if i > TinyExtentCount {
				deleteCnt++
			}
		}
	}
	assertEqual(t, maxExtentId - deleteCnt, ms.Len())
	ms.Range(func(extentID uint64, ei *ExtentInfoBlock) {
		assertEqual(t, extentID, ei[FileID])
	})
}

func TestIter(t *testing.T) {
	ms := NewMapSlice(1)
	seq := []uint64{9, 5, 2, 7, 1970, 1, 3, 654}
	for _, v := range seq {
		ms.Store(v, initExtentInfo(v))
	}
	count := 0
	ms.Range(func(extentID uint64, obj *ExtentInfoBlock) {
		assertEqual(t, extentID, obj[FileID])
		count++
	})
	assertEqual(t, count, len(seq))
	ms.Delete(654)
	count = 0
	ms.Range(func(extentID uint64, obj *ExtentInfoBlock) {
		assertEqual(t, extentID, obj[FileID])
		count++
	})
	assertEqual(t, count, len(seq)-1)

	ms.Delete(1970)
	count = 0
	ms.Range(func(extentID uint64, obj *ExtentInfoBlock) {
		assertEqual(t, extentID, obj[FileID])
		count++
	})
	assertEqual(t, count, len(seq)-2)
	assertEqual(t, ms.Len(), len(seq)-2)
}

func TestMapSlice_RangeDist(t *testing.T) {
	ms := NewMapSlice(1)
	for i := 1; i < 1000; i++ {
		ms.Store(uint64(i), initExtentInfo(uint64(i)))
	}
	ms.RangeDist(proto.AllExtentType, func(extentID uint64, ei *ExtentInfoBlock) {
		if extentID == 0 || extentID >= 1000 {
			t.Fail()
		}
		assertEqual(t, extentID, (*ei)[FileID])
	})
	ms.RangeDist(proto.TinyExtentType, func(extentID uint64, ei *ExtentInfoBlock) {
		if extentID <= 0 || extentID >= TinyExtentCount+1 {
			t.Fail()
		}
		assertEqual(t, extentID, (*ei)[FileID])
	})
	ms.RangeDist(proto.NormalExtentType, func(extentID uint64, ei *ExtentInfoBlock) {
		if extentID < TinyExtentCount {
			t.Fail()
		}
		assertEqual(t, extentID, (*ei)[FileID])
	})
}

func assertEqual(t *testing.T, expected, actual interface{}, msgAndArgs ...interface{}) {
	if !objectsAreEqual(expected, actual) {
		_, file, line, _ := runtime.Caller(1)
		file = path.Base(file)
		t.Errorf(fmt.Sprintf("\n%s:%d: Not equal: \n"+
			"expected: %T(%#v)\n"+
			"actual  : %T(%#v)\n",
			file, line, expected, expected, actual, actual), msgAndArgs...)
	}
}

func noError(t *testing.T, e error) {
	if e != nil {
		_, file, line, _ := runtime.Caller(1)
		file = path.Base(file)
		t.Errorf(fmt.Sprintf("\n%s:%d: Error is not nil: \n"+
			"actual  : %T(%#v)\n", file, line, e, e))
	}
}

func objectsAreEqual(expected, actual interface{}) bool {
	if expected == nil || actual == nil {
		return expected == actual
	}

	exp, ok := expected.([]byte)
	if !ok {
		return reflect.DeepEqual(expected, actual)
	}

	act, ok := actual.([]byte)
	if !ok {
		return false
	}
	if exp == nil || act == nil {
		return exp == nil && act == nil
	}
	return bytes.Equal(exp, act)
}


const (
	MaxExtentID=60000
)

var (
	ems *MapSlice
)


func Test_DeleteExtentData(t *testing.T) {
	InsertExtentData(t)
	var wg sync.WaitGroup
	wg.Add(1)
	go asyncDeleteExtentData(t,3,&wg)
	wg.Wait()
	wg.Add(1)
	go loadExtentInfoBlockArr(t,3,&wg)
	wg.Wait()
}

func InsertExtentData(t *testing.T) {
	var wg sync.WaitGroup
	ems=NewMapSlice(1)
	for i:=1;i<8;i++{
		wg.Add(1)
		go asyncInsertExtentData(t,i,&wg)
	}
	wg.Wait()

}

func asyncInsertExtentData(t *testing.T,modNun int,wg *sync.WaitGroup) {
	defer func() {
		wg.Done()
	}()
	var count int
	for i:=1;i<MaxExtentID;i++{
		if i%modNun==0 {
			eb:=ExtentInfoBlock{
				FileID:uint64(i),
				Size:uint64(i+1),
				Crc:uint64(i+2),
				ModifyTime:uint64(i+3),
			}
			ems.Store(uint64(i),eb)
			count++
		}
	}
	t.Logf("modNum(%v) insert success(%v) emsSumRecords(%v)",modNun,count,ems.Len())
}




func asyncDeleteExtentData(t *testing.T,modNun int,wg *sync.WaitGroup) {
	defer func() {
		wg.Done()
	}()
	var count int
	for i:=1;i<MaxExtentID;i++{
		if IsTinyExtent(uint64(i)){
			continue
		}
		if i%modNun==0 {
			ems.Delete(uint64(i))
			count++
		}
	}
	t.Logf("modNum(%v) delete success(%v) emsSumRecords(%v)",modNun,count,ems.Len())
}



func loadExtentInfoBlockArr(t *testing.T,modNun int,wg *sync.WaitGroup) {
	defer func() {
		wg.Done()
	}()
	var count int
	for i:=1;i<MaxExtentID;i++{
		if i%modNun==0 {
			if IsTinyExtent(uint64(i)) {
				continue
			}
			ei,_,err:=loadExtentInfoBlockAndCheck(t,uint64(i))
			if err==nil {
				t.Fatalf("extent(%v) has been delete,why can load(%v)",uint64(i),ei)
				t.FailNow()
			}
			count++
		}
	}
	t.Logf("modNum(%v) load failed  count(%v) emsSumRecords(%v)",modNun,count,ems.Len())
}


func loadExtentInfoBlockAndCheck(t *testing.T,eid uint64)(ei *ExtentInfoBlock,ok bool,err error) {
	ei,ok=ems.Load(eid)
	if !ok {
		err=fmt.Errorf("cannot load extent(%v),because not exsit",eid)
		return
	}
	if ei[FileID]!=uint64(eid) {
		err=fmt.Errorf("check extentID failed :eid(%v) extentInfoBlock(%v)",eid,ei)
		return
	}
	if ei[Size]!=uint64(eid+1) {
		err=fmt.Errorf("check extent Size failed :eid(%v) extentInfoBlock(%v)",eid,ei)
		return
	}
	if ei[Crc]!=uint64(eid+2) {
		err=fmt.Errorf("check extent Crc failed :eid(%v) extentInfoBlock(%v)",eid,ei)
		return
	}
	if ei[ModifyTime]!=uint64(eid+3) {
		err=fmt.Errorf("check extent ModifyTime failed :eid(%v) extentInfoBlock(%v)",eid,ei)
		return
	}

	return
}