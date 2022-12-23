package sortedextent

import (
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/btree"
	"sort"
	"sync"
	"time"
)

const (
	defaultDelEkDegreed = 4
	maxDelExtentSetSize = 10000
)

type DelEkSet interface {
	Put(ek *proto.ExtentKey, ino, srcType uint64)
	Put2(ek *proto.ExtentKey, ino, srcType uint64)
    GetDelExtentKeys(eks []proto.ExtentKey) []proto.MetaDelExtentKey
}

type ExtentKeySet struct {
	set []proto.MetaDelExtentKey
}

func NewExtentKeySet() *ExtentKeySet {
    return &ExtentKeySet{make([]proto.MetaDelExtentKey, 0)}
}

func (s *ExtentKeySet) sort() {
	sort.SliceStable(s.set, func(i, j int) bool {
		return s.set[i].PartitionId < s.set[j].PartitionId ||
			(s.set[i].PartitionId == s.set[j].PartitionId &&
				s.set[i].ExtentId < s.set[j].ExtentId)
	})
}

func (s *ExtentKeySet) dup() {
	s.sort()
	newSet := make([]proto.MetaDelExtentKey, 0)
	lastEk := proto.MetaDelExtentKey{}
	for _, ek := range s.set {
		if lastEk.PartitionId == ek.PartitionId && lastEk.ExtentId == ek.ExtentId {
			continue
		}
		newSet = append(newSet, ek)
		lastEk = ek
	}
	s.set = newSet
}

func (s *ExtentKeySet) search(ek *proto.ExtentKey) (i int, found bool) {
	i = sort.Search(len(s.set), func(i int) bool {
		return ek.PartitionId < s.set[i].PartitionId || (ek.PartitionId == s.set[i].PartitionId && ek.ExtentId <= s.set[i].ExtentId)
	})
	found = i >= 0 && i < len(s.set) && ek.PartitionId == s.set[i].PartitionId && ek.ExtentId == s.set[i].ExtentId
	return
}

func (s *ExtentKeySet) Has(ek *proto.ExtentKey) (has bool) {
	_, has = s.search(ek)
	return
}

func (s *ExtentKeySet) Remove(ek *proto.ExtentKey) bool{
	if i, found := s.search(ek); found {
		if i == len(s.set)-1 {
			s.set = (s.set)[:i]
			return true
		}
		s.set = append((s.set)[:i], (s.set)[i+1:]...)
		return true
	}
	return false
}

func (s *ExtentKeySet) ToExtentKeys() []proto.MetaDelExtentKey {
	return s.set
}

func (s ExtentKeySet) Length() int {
	return len(s.set)
}

func (s ExtentKeySet) Reset() {
	s.set = (s.set)[:0]
}

func (s *ExtentKeySet) Put(ek *proto.ExtentKey, ino, srcType uint64) {
	s.set = append(s.set, proto.MetaDelExtentKey{
		ExtentKey: proto.ExtentKey{FileOffset: ek.FileOffset, PartitionId: ek.PartitionId,
			ExtentId: ek.ExtentId, ExtentOffset: ek.ExtentOffset, Size: ek.Size, CRC: ek.CRC},
		InodeId:   ino, TimeStamp: time.Now().Unix(), SrcType: srcType})
	//if _, found := s.search(ek); !found {
	//	s.set = append(s.set, proto.MetaDelExtentKey{
	//						ExtentKey: proto.ExtentKey{FileOffset: ek.FileOffset, PartitionId: ek.PartitionId,
	//								ExtentId: ek.ExtentId, ExtentOffset: ek.ExtentOffset, Size: ek.Size, CRC: ek.CRC},
	//						InodeId:   ino, TimeStamp: time.Now().Unix(), SrcType: srcType})
	//	s.sort()
	//}
}

func (s *ExtentKeySet) Put2(ek *proto.ExtentKey, ino, srcType uint64) {
	s.set = append(s.set, proto.MetaDelExtentKey{
		ExtentKey: proto.ExtentKey{FileOffset: ek.FileOffset, PartitionId: ek.PartitionId,
			ExtentId: ek.ExtentId, ExtentOffset: ek.ExtentOffset, Size: ek.Size, CRC: ek.CRC},
		InodeId:   ino, TimeStamp: time.Now().Unix(), SrcType: srcType})

	//if _, found := s.search(ek); !found {
	//	s.set = append(s.set, proto.MetaDelExtentKey{
	//						ExtentKey: proto.ExtentKey{FileOffset: ek.FileOffset, PartitionId: ek.PartitionId,
	//								ExtentId: ek.ExtentId, ExtentOffset: ek.ExtentOffset, Size: ek.Size, CRC: ek.CRC},
	//						InodeId:   ino, TimeStamp: time.Now().Unix(), SrcType: srcType})
	//	s.sort()
	//}
}

func (s *ExtentKeySet) GetDelExtentKeys(eks []proto.ExtentKey) []proto.MetaDelExtentKey {
	if s.Length() == 0 {
		return nil
	}
	s.dup()
	//fmt.Printf("after dup, set len:%d\n", s.Length())

	for i := 0; i < len(eks) && s.Length() != 0; i++ {
		s.Remove(&eks[i])
		if s.Length() == 0{
			break
		}
	}
	return s.ToExtentKeys()
}

type  DelExtentKeyBtree struct {
	sync.RWMutex
	tree *btree.BTree
}
func NewDelExtentKeyBtree() *DelExtentKeyBtree {
	return &DelExtentKeyBtree{
		tree: btree.New(defaultDelEkDegreed),
	}
}

func (s *DelExtentKeyBtree) Put(ek *proto.ExtentKey, ino, srcType uint64) {
	meteDekEk := &proto.MetaDelExtentKey{
					ExtentKey: proto.ExtentKey{FileOffset: ek.FileOffset, PartitionId: ek.PartitionId,
							ExtentId: ek.ExtentId, ExtentOffset: ek.ExtentOffset, Size: ek.Size, CRC: ek.CRC},
					InodeId:   ino, TimeStamp: time.Now().Unix(), SrcType: srcType}
	s.tree.ReplaceOrInsert(meteDekEk)
}

func (s *DelExtentKeyBtree) Put2(ek *proto.ExtentKey, ino, srcType uint64) {
	meteDekEk := &proto.MetaDelExtentKey{
		ExtentKey: proto.ExtentKey{FileOffset: ek.FileOffset, PartitionId: ek.PartitionId,
			ExtentId: ek.ExtentId, ExtentOffset: ek.ExtentOffset, Size: ek.Size, CRC: ek.CRC},
		InodeId:   ino, TimeStamp: time.Now().Unix(), SrcType: srcType}
	s.tree.ReplaceOrInsert(meteDekEk)
}

func (s *DelExtentKeyBtree) GetDelExtentKeys(eks []proto.ExtentKey) []proto.MetaDelExtentKey {
	if s.tree.Len() == 0 {
		return nil
	}

	tmpDelEk := &proto.MetaDelExtentKey{}
	for i := 0; i < len(eks); i++ {
		tmpDelEk.PartitionId = eks[i].PartitionId
		tmpDelEk.ExtentId = eks[i].ExtentId
		s.tree.Delete(tmpDelEk)
		if s.tree.Len() == 0 {
			break
		}
	}

	delEkArr := make([]proto.MetaDelExtentKey, s.tree.Len())
	i := 0
	s.tree.AscendGreaterOrEqual(&proto.MetaDelExtentKey{}, func (item btree.Item) bool {
		delEkArr[i] = * item.(*proto.MetaDelExtentKey)
		i++
		return true
	})
	//s.tree.Clear(false)
	return delEkArr

}

type DelEkKey struct {
	PartitionId uint64
	ExtentId    uint64
}
type  DelExtentKeyMap struct {
	sync.RWMutex
	ekMap map[DelEkKey]*proto.MetaDelExtentKey
}
func NewDelExtentKeyMap(cnt int) *DelExtentKeyMap {
	return &DelExtentKeyMap{
		ekMap: make(map[DelEkKey]*proto.MetaDelExtentKey, cnt),
	}
}

func (s *DelExtentKeyMap) Put(ek *proto.ExtentKey, ino, srcType uint64) {
	meteDekEk := &proto.MetaDelExtentKey{
		ExtentKey: proto.ExtentKey{FileOffset: ek.FileOffset, PartitionId: ek.PartitionId,
			ExtentId: ek.ExtentId, ExtentOffset: ek.ExtentOffset, Size: ek.Size, CRC: ek.CRC},
		InodeId:   ino, TimeStamp: time.Now().Unix(), SrcType: srcType}
	s.ekMap[DelEkKey{PartitionId: ek.PartitionId, ExtentId: ek.ExtentId}] = meteDekEk
}

func (s *DelExtentKeyMap) Put2(ek *proto.ExtentKey, ino, srcType uint64) {
	meteDekEk := &proto.MetaDelExtentKey{
		ExtentKey: proto.ExtentKey{FileOffset: ek.FileOffset, PartitionId: ek.PartitionId,
			ExtentId: ek.ExtentId, ExtentOffset: ek.ExtentOffset, Size: ek.Size, CRC: ek.CRC},
		InodeId:   ino, TimeStamp: time.Now().Unix(), SrcType: srcType}
	s.ekMap[DelEkKey{PartitionId: ek.PartitionId, ExtentId: ek.ExtentId}] = meteDekEk
}

func (s *DelExtentKeyMap) GetDelExtentKeys(eks []proto.ExtentKey) []proto.MetaDelExtentKey {
	if len(s.ekMap) == 0 {
		return nil
	}

	tmpDelEk := DelEkKey{}
	for i := 0; i < len(eks); i++ {
		tmpDelEk.PartitionId = eks[i].PartitionId
		tmpDelEk.ExtentId = eks[i].ExtentId
		delete(s.ekMap, tmpDelEk)
		if len(s.ekMap) == 0 {
			break
		}
	}

	delEkArr := make([]proto.MetaDelExtentKey, len(s.ekMap))
	i := 0
	for _, ek := range s.ekMap {
		delEkArr[i] = *ek
		i++
	}
	//s.tree.Clear(false)
	return delEkArr

}
