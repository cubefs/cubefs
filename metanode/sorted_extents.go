package metanode

import (
	"bytes"
	"encoding/json"
	"sync"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/storage"
	"github.com/cubefs/cubefs/util/log"
)

type SortedExtents struct {
	sync.RWMutex
	eks []proto.ExtentKey
}

func NewSortedExtents() *SortedExtents {
	return &SortedExtents{
		eks: make([]proto.ExtentKey, 0),
	}
}

// attention: only used for deleted eks
func NewSortedExtentsFromEks(eks []proto.ExtentKey) *SortedExtents {
	return &SortedExtents{
		eks: eks,
	}
}

func (se *SortedExtents) String() string {
	se.RLock()
	data, err := json.Marshal(se.eks)
	se.RUnlock()
	if err != nil {
		return ""
	}
	return string(data)
}

func (se *SortedExtents) MarshalBinary(v3 bool) ([]byte, error) {
	var data []byte

	se.RLock()
	defer se.RUnlock()

	data = make([]byte, 0, proto.ExtentLength*len(se.eks))
	for _, ek := range se.eks {
		ekdata, err := ek.MarshalBinary(v3)
		if err != nil {
			return nil, err
		}
		data = append(data, ekdata...)
	}

	return data, nil
}

func (se *SortedExtents) UnmarshalBinary(data []byte, v3 bool) (err error, splitMap *sync.Map) {
	se.Lock()
	defer se.Unlock()

	buf := bytes.NewBuffer(data)
	for {
		var ek proto.ExtentKey
		if buf.Len() == 0 {
			break
		}
		if err = ek.UnmarshalBinary(buf, v3); err != nil {
			return
		}
		// Don't use se.Append here, since we need to retain the raw ek order.
		se.eks = append(se.eks, ek)
		if ek.IsSplit() {
			if splitMap == nil {
				splitMap = new(sync.Map)
			}
			val, ok := splitMap.Load(ek.GenerateId())
			if !ok {
				splitMap.Store(ek.GenerateId(), uint32(1))
				continue
			}
			splitMap.Store(ek.GenerateId(), val.(uint32)+1)
		}
	}
	return
}

func (se *SortedExtents) Append(ek proto.ExtentKey) (deleteExtents []proto.ExtentKey) {
	endOffset := ek.FileOffset + uint64(ek.Size)

	se.Lock()
	defer se.Unlock()

	if len(se.eks) <= 0 {
		se.eks = append(se.eks, ek)
		return
	}
	lastKey := se.eks[len(se.eks)-1]
	if lastKey.FileOffset+uint64(lastKey.Size) <= ek.FileOffset {
		se.eks = append(se.eks, ek)
		return
	}
	firstKey := se.eks[0]
	if firstKey.FileOffset >= endOffset {
		eks := se.doCopyExtents()
		se.eks = se.eks[:0]
		se.eks = append(se.eks, ek)
		se.eks = append(se.eks, eks...)
		return
	}

	var startIndex, endIndex int

	invalidExtents := make([]proto.ExtentKey, 0)
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

	endIndex = startIndex + len(invalidExtents)
	upperExtents := make([]proto.ExtentKey, len(se.eks)-endIndex)
	copy(upperExtents, se.eks[endIndex:])
	se.eks = se.eks[:startIndex]
	se.eks = append(se.eks, ek)
	se.eks = append(se.eks, upperExtents...)
	// check if ek and key are the same extent file with size extented
	deleteExtents = make([]proto.ExtentKey, 0, len(invalidExtents))
	for _, key := range invalidExtents {
		if key.PartitionId != ek.PartitionId || key.ExtentId != ek.ExtentId {
			deleteExtents = append(deleteExtents, key)
		}
	}
	return
}

func storeEkSplit(mpId uint64, inodeID uint64, ekRef *sync.Map, ek *proto.ExtentKey) (id uint64) {
	if ekRef == nil {
		log.LogErrorf("[storeEkSplit] mpId [%v] inodeID %v ekRef nil", mpId, inodeID)
		return
	}
	log.LogDebugf("[storeEkSplit] mpId [%v] inode[%v] dp [%v] extent id[%v] ek [%v]", mpId, inodeID, ek.PartitionId, ek.ExtentId, ek)
	id = ek.PartitionId<<32 | ek.ExtentId
	var v uint32
	if val, ok := ekRef.Load(id); !ok {
		if ek.IsSplit() {
			log.LogErrorf("[storeEkSplit] mpId [%v]inode id[%v] ek [%v] already be set split", mpId, inodeID, ek)
		}
		v = 1
	} else {
		v = val.(uint32) + 1
	}
	ek.SetSplit(true)
	ekRef.Store(id, v)
	log.LogDebugf("[storeEkSplit] mpId [%v] inode[%v] dp [%v] extent id[%v].key %v, cnt %v", mpId, inodeID, ek.PartitionId, ek.ExtentId,
		ek.PartitionId<<32|ek.ExtentId, v)
	return
}

func (se *SortedExtents) SplitWithCheck(mpId uint64, inodeID uint64, ekSplit proto.ExtentKey, ekRef *sync.Map) (delExtents []proto.ExtentKey, status uint8) {
	status = proto.OpOk
	endOffset := ekSplit.FileOffset + uint64(ekSplit.Size)
	log.LogDebugf("[SplitWithCheck] mpId [%v]. inode[%v]  ekSplit ek [%v]", mpId, inodeID, ekSplit)
	se.Lock()
	defer se.Unlock()

	if len(se.eks) <= 0 {
		log.LogErrorf("[SplitWithCheck] mpId [%v]. inode[%v] eks empty cann't find ek [%v]", mpId, inodeID, ekSplit)
		status = proto.OpArgMismatchErr
		return
	}
	lastKey := se.eks[len(se.eks)-1]
	if lastKey.FileOffset+uint64(lastKey.Size) <= ekSplit.FileOffset {
		log.LogErrorf("[SplitWithCheck] mpId [%v]. inode[%v] eks do split not found", mpId, inodeID)
		status = proto.OpArgMismatchErr
		return
	}

	firstKey := se.eks[0]
	if firstKey.FileOffset >= endOffset {
		log.LogErrorf("[SplitWithCheck] mpId [%v]. inode[%v] eks do split not found", mpId, inodeID)
		status = proto.OpArgMismatchErr
		return
	}

	var startIndex int
	for idx, key := range se.eks {
		if ekSplit.FileOffset >= key.FileOffset {
			startIndex = idx + 1
			continue
		}
		if endOffset >= key.FileOffset+uint64(key.Size) {
			continue
		}
		break
	}

	if startIndex == 0 {
		status = proto.OpArgMismatchErr
		log.LogErrorf("[SplitWithCheck] mpId [%v]. inode[%v] should have no valid extent request [%v]", mpId, inodeID, ekSplit)
		return
	}

	key := &se.eks[startIndex-1]
	if !storage.IsTinyExtent(key.ExtentId) && (key.PartitionId != ekSplit.PartitionId || key.ExtentId != ekSplit.ExtentId) {
		status = proto.OpArgMismatchErr
		log.LogErrorf("SplitWithCheck. mpId [%v] inode[%v]  key found with mismatch extent info [%v] request [%v]", mpId, inodeID, key, ekSplit)
		return
	}

	keySize := key.Size
	key.AddModGen()
	if !key.IsSplit() {
		storeEkSplit(mpId, inodeID, ekRef, key)
	}

	if ekSplit.FileOffset+uint64(ekSplit.Size) > key.FileOffset+uint64(key.Size) {
		status = proto.OpArgMismatchErr
		log.LogErrorf("SplitWithCheck. mpId [%v] inode[%v] request [%v] out scope of exist key [%v]", mpId, inodeID, ekSplit, key)
		return
	}
	// Makes the request idempotent, just in case client retries.
	if ekSplit.IsEqual(key) {
		log.LogWarnf("SplitWithCheck. mpId [%v] request key %v is a repeat request", mpId, key)
		return
	}

	delKey := *key
	delKey.ExtentOffset = key.ExtentOffset + (ekSplit.FileOffset - key.FileOffset)
	delKey.Size = ekSplit.Size
	storeEkSplit(mpId, inodeID, ekRef, &delKey)

	if ekSplit.Size == 0 {
		log.LogErrorf("SplitWithCheck. mpId [%v] inode[%v] delKey %v,key %v, eksplit %v", mpId, inodeID, delKey, key, ekSplit)
	}
	delKey.FileOffset = ekSplit.FileOffset

	delExtents = append(delExtents, delKey)

	log.LogDebugf("SplitWithCheck. mpId [%v]  inode[%v]  key offset %v, split FileOffset %v, startIndex %v,key [%v], ekSplit[%v] delkey [%v]", mpId, inodeID,
		key.FileOffset, ekSplit.FileOffset, startIndex, key, ekSplit, delKey)

	if key.FileOffset == ekSplit.FileOffset { // at the begin
		keyDup := *key
		eks := make([]proto.ExtentKey, len(se.eks)-startIndex)
		copy(eks, se.eks[startIndex:])
		se.eks = se.eks[:startIndex-1]

		var keyBefore *proto.ExtentKey
		if len(se.eks) > 0 {
			keyBefore = &se.eks[len(se.eks)-1]
			log.LogDebugf("SplitWithCheck. mpId [%v].keyBefore. ek [%v] and ekSplit %v", mpId, keyBefore, ekSplit)
		}
		if keyBefore != nil && keyBefore.IsSequenceWithSameSeq(&ekSplit) {
			log.LogDebugf("SplitWithCheck. mpId [%v]. inode[%v]  keyBefore [%v], ekSplit [%v]", mpId, inodeID, keyBefore, ekSplit)
			log.LogDebugf("SplitWithCheck. mpId [%v].merge.head. ek [%v] and %v", mpId, keyBefore, ekSplit)
			keyBefore.Size += ekSplit.Size
		} else {
			se.eks = append(se.eks, ekSplit)
			storeEkSplit(mpId, inodeID, ekRef, &ekSplit)
		}

		keyDup.FileOffset = keyDup.FileOffset + uint64(ekSplit.Size)
		keyDup.ExtentOffset = keyDup.ExtentOffset + uint64(ekSplit.Size)
		keyDup.Size = keySize - ekSplit.Size
		if keyDup.Size == 0 {
			log.LogErrorf("SplitWithCheck. mpId [%v] inode[%v] delKey %v,keyDup %v, eksplit %v", mpId, inodeID, delKey, keyDup, ekSplit)
		}
		se.eks = append(se.eks, keyDup)
		se.eks = append(se.eks, eks...)
	} else if key.FileOffset+uint64(key.Size) == ekSplit.FileOffset+uint64(ekSplit.Size) { // in the end
		key.Size = keySize - ekSplit.Size
		if key.Size == 0 {
			log.LogErrorf("SplitWithCheck. mpId [%v].inode[%v] delKey %v,key %v, eksplit %v", mpId, inodeID, delKey, key, ekSplit)
		}
		eks := make([]proto.ExtentKey, len(se.eks[startIndex:]))
		copy(eks, se.eks[startIndex:])
		se.eks = se.eks[:startIndex]

		if len(eks) > 0 && ekSplit.IsSequenceWithSameSeq(&eks[0]) {
			log.LogDebugf("SplitWithCheck.mpId [%v].merge.end. ek [%v] and %v", mpId, ekSplit, eks[0])
			eks[0].FileOffset = ekSplit.FileOffset
			eks[0].ExtentOffset = ekSplit.ExtentOffset
			eks[0].Size += ekSplit.Size
		} else {
			se.eks = append(se.eks, ekSplit)
			storeEkSplit(mpId, inodeID, ekRef, &ekSplit)
		}

		se.eks = append(se.eks, eks...)
	} else { // in the middle
		key.Size = uint32(ekSplit.FileOffset - key.FileOffset)
		if key.Size == 0 {
			log.LogErrorf("SplitWithCheck. mpId [%v].inode[%v] delKey %v,key %v, eksplit %v", mpId, inodeID, delKey, key, ekSplit)
		}
		eks := make([]proto.ExtentKey, len(se.eks[startIndex:]))
		copy(eks, se.eks[startIndex:])

		se.eks = se.eks[:startIndex]
		se.eks = append(se.eks, ekSplit)
		storeEkSplit(mpId, inodeID, ekRef, &ekSplit)
		mKey := &proto.ExtentKey{
			FileOffset:   ekSplit.FileOffset + uint64(ekSplit.Size),
			PartitionId:  key.PartitionId,
			ExtentId:     key.ExtentId,
			ExtentOffset: key.ExtentOffset + uint64(key.Size) + uint64(ekSplit.Size),
			Size:         keySize - key.Size - ekSplit.Size,
			// crc
			SnapInfo: &proto.ExtSnapInfo{
				VerSeq:  key.GetSeq(),
				ModGen:  0,
				IsSplit: true,
			},
		}
		se.eks = append(se.eks, *mKey)
		storeEkSplit(mpId, inodeID, ekRef, mKey)

		if keySize-key.Size-ekSplit.Size == 0 {
			log.LogErrorf("SplitWithCheck. mpId [%v].inode[%v] keySize %v,key %v, eksplit %v", mpId, inodeID, keySize, key, ekSplit)
		}
		se.eks = append(se.eks, eks...)
	}
	return
}

func (se *SortedExtents) CheckAndAddRef(lastKey *proto.ExtentKey, currEk *proto.ExtentKey, addRefFunc func(*proto.ExtentKey)) (ok bool) {
	if !lastKey.IsSameExtent(currEk) {
		return
	}
	log.LogDebugf("action[AppendWithCheck.CheckAndAddRef] ek [%v],lastKey %v", currEk, lastKey)
	if lastKey.FileOffset+uint64(lastKey.Size) <= currEk.FileOffset {
		if !lastKey.IsSplit() {
			addRefFunc(lastKey)
		}
		addRefFunc(currEk)
		ok = true
		return
	}

	if lastKey.FileOffset == currEk.FileOffset &&
		lastKey.PartitionId == currEk.PartitionId &&
		lastKey.ExtentId == currEk.ExtentId &&
		lastKey.ExtentOffset == currEk.ExtentOffset && lastKey.Size < currEk.Size && lastKey.GetSeq() < currEk.GetSeq() {

		log.LogDebugf("action[AppendWithCheck.CheckAndAddRef] split append key %v", currEk)
		currEk.FileOffset = lastKey.FileOffset + uint64(lastKey.Size)
		currEk.ExtentOffset = currEk.ExtentOffset + uint64(lastKey.Size)
		currEk.Size = currEk.Size - lastKey.Size
		log.LogDebugf("action[AppendWithCheck.CheckAndAddRef] after split append key %v", currEk)
		if !lastKey.IsSplit() {
			addRefFunc(lastKey)
		}
		addRefFunc(currEk)
		ok = true
		return
	}
	return
}

func (se *SortedExtents) AppendWithCheck(inodeID uint64, ek proto.ExtentKey, addRefFunc func(*proto.ExtentKey), clientDiscardExts []proto.ExtentKey) (deleteExtents []proto.ExtentKey, status uint8) {
	status = proto.OpOk
	endOffset := ek.FileOffset + uint64(ek.Size)
	se.Lock()
	defer se.Unlock()
	log.LogDebugf("action[AppendWithCheck] ek [%v], clientDiscardExts [%v] se.eks [%v]", ek, clientDiscardExts, se.eks)
	if len(se.eks) <= 0 {
		se.eks = append(se.eks, ek)
		return
	}
	idx := len(se.eks) - 1
	tailKey := &se.eks[idx]

	log.LogDebugf("action[AppendWithCheck] ek [%v],tailKey %v, clientDiscardExts [%v] se.eks [%v]", ek, tailKey, clientDiscardExts, se.eks)
	if ok := se.CheckAndAddRef(tailKey, &ek, addRefFunc); ok {
		se.eks = append(se.eks, ek)
		return
	}

	firstKey := se.eks[0]
	if firstKey.FileOffset >= endOffset {
		se.insert(ek, 0)
		return
	}

	var startIndex, endIndex int
	invalidExtents := make([]proto.ExtentKey, 0)
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

	// Makes the request idempotent, just in case client retries.
	if len(invalidExtents) == 1 && invalidExtents[0].Equals(&ek) {
		log.LogDebugf("action[AppendWithCheck] ek [%v]", ek)
		return
	}

	// check if ek and key are the same extent file with size extented
	deleteExtents = make([]proto.ExtentKey, 0, len(invalidExtents))
	for _, key := range invalidExtents {
		if key.PartitionId != ek.PartitionId || key.ExtentId != ek.ExtentId || key.ExtentOffset != ek.ExtentOffset {
			deleteExtents = append(deleteExtents, key)
		}
	}

	log.LogDebugf("action[AppendWithCheck] invalidExtents(%v) deleteExtents(%v) discardExtents(%v)", invalidExtents, deleteExtents, clientDiscardExts)
	if clientDiscardExts != nil {
		if len(deleteExtents) != len(clientDiscardExts) {
			log.LogErrorf("action[AppendWithCheck] OpConflictExtentsErr error. inode[%v] deleteExtents [%v] clientDiscardExts [%v]", inodeID, deleteExtents, clientDiscardExts)
			return deleteExtents, proto.OpConflictExtentsErr
		}
		for i := 0; i < len(clientDiscardExts); i++ {
			if deleteExtents[i].PartitionId != clientDiscardExts[i].PartitionId || deleteExtents[i].ExtentId != clientDiscardExts[i].ExtentId || deleteExtents[i].ExtentOffset != clientDiscardExts[i].ExtentOffset {
				log.LogDebugf("action[AppendWithCheck] OpConflictExtentsErr error. inode[%v] idx %v deleteExtents[%v]  clientDiscardExts [%v]", inodeID, i, deleteExtents[i], clientDiscardExts[i])
				return deleteExtents, proto.OpConflictExtentsErr
			}
		}
	} else if len(deleteExtents) != 0 {
		log.LogDebugf("action[AppendWithCheck] OpConflictExtentsErr error. inode[%v] deleteExtents [%v]", inodeID, deleteExtents)
		return deleteExtents, proto.OpConflictExtentsErr
	}

	defer func() {
		if startIndex == 0 {
			return
		}
		se.CheckAndAddRef(&se.eks[startIndex-1], &se.eks[startIndex], addRefFunc)
	}()

	if len(invalidExtents) == 0 {
		se.insert(ek, startIndex)
		return
	}

	endIndex = startIndex + len(invalidExtents)
	se.instertWithDiscard(ek, startIndex, endIndex)
	return
}

func (se *SortedExtents) Truncate(offset uint64, doOnLastKey func(*proto.ExtentKey), insertRefMap func(ek *proto.ExtentKey)) (deleteExtents []proto.ExtentKey) {
	var endIndex int

	se.Lock()
	defer se.Unlock()

	endIndex = -1
	for idx, key := range se.eks {
		if key.FileOffset >= offset {
			endIndex = idx
			break
		}
	}

	if endIndex < 0 {
		deleteExtents = make([]proto.ExtentKey, 0)
	} else {
		deleteExtents = make([]proto.ExtentKey, len(se.eks)-endIndex)
		copy(deleteExtents, se.eks[endIndex:])
		se.eks = se.eks[:endIndex]
	}

	numKeys := len(se.eks)
	if numKeys > 0 {
		lastKey := &se.eks[numKeys-1]
		if lastKey.FileOffset+uint64(lastKey.Size) > offset {
			if doOnLastKey != nil {
				doOnLastKey(&proto.ExtentKey{Size: uint32(lastKey.FileOffset + uint64(lastKey.Size) - offset)})
			}
			rsKey := &proto.ExtentKey{}
			*rsKey = *lastKey
			lastKey.Size = uint32(offset - lastKey.FileOffset)
			if insertRefMap != nil {
				insertRefMap(lastKey)
			}

			rsKey.Size -= lastKey.Size
			rsKey.FileOffset += uint64(lastKey.Size)
			rsKey.ExtentOffset += uint64(lastKey.Size)
			if insertRefMap != nil {
				insertRefMap(rsKey)
			}

			deleteExtents = append([]proto.ExtentKey{*rsKey}, deleteExtents...)
			log.LogDebugf("SortedExtents.Truncate rsKey %v, deleteExtents %v", rsKey, deleteExtents)
		}
	}
	return
}

func (se *SortedExtents) insert(ek proto.ExtentKey, startIdx int) {
	se.eks = append(se.eks, ek)
	size := len(se.eks)

	for idx := size - 1; idx > startIdx; idx-- {
		se.eks[idx] = se.eks[idx-1]
	}

	se.eks[startIdx] = ek
}

func (se *SortedExtents) instertWithDiscard(ek proto.ExtentKey, startIdx, endIdx int) {
	upperSize := len(se.eks) - endIdx
	se.eks[startIdx] = ek

	for idx := 0; idx < upperSize; idx++ {
		se.eks[startIdx+1+idx] = se.eks[endIdx+idx]
	}

	se.eks = se.eks[:startIdx+1+upperSize]
}

func (se *SortedExtents) Len() int {
	se.RLock()
	defer se.RUnlock()
	return len(se.eks)
}

// Returns the file size
func (se *SortedExtents) LayerSize() (layerSize uint64) {
	se.RLock()
	defer se.RUnlock()

	last := len(se.eks)
	if last <= 0 {
		return uint64(0)
	}
	for _, ek := range se.eks {
		layerSize += uint64(ek.Size)
	}
	return
}

// Returns the file size
func (se *SortedExtents) Size() uint64 {
	se.RLock()
	defer se.RUnlock()

	last := len(se.eks)
	if last <= 0 {
		return uint64(0)
	}
	return se.eks[last-1].FileOffset + uint64(se.eks[last-1].Size)
}

func (se *SortedExtents) Range(f func(index int, ek proto.ExtentKey) bool) {
	se.RLock()
	defer se.RUnlock()

	for i, ek := range se.eks {
		if !f(i, ek) {
			break
		}
	}
}

func (se *SortedExtents) Clone() *SortedExtents {
	newSe := NewSortedExtents()

	se.RLock()
	defer se.RUnlock()

	newSe.eks = se.doCopyExtents()
	return newSe
}

func (se *SortedExtents) CopyExtents() []proto.ExtentKey {
	se.RLock()
	defer se.RUnlock()
	return se.doCopyExtents()
}

func (se *SortedExtents) CopyTinyExtents() []proto.ExtentKey {
	se.RLock()
	defer se.RUnlock()
	return se.doCopyTinyExtents()
}

func (se *SortedExtents) doCopyExtents() []proto.ExtentKey {
	eks := make([]proto.ExtentKey, len(se.eks))
	copy(eks, se.eks)
	return eks
}

func (se *SortedExtents) doCopyTinyExtents() []proto.ExtentKey {
	eks := make([]proto.ExtentKey, 0)
	for _, ek := range se.eks {
		if storage.IsTinyExtent(ek.ExtentId) {
			eks = append(eks, ek)
		}
	}
	return eks
}

// discard code
func (se *SortedExtents) Delete(delEks []proto.ExtentKey) (curEks []proto.ExtentKey) {
	se.RLock()
	defer se.RUnlock()

	curEks = make([]proto.ExtentKey, len(se.eks)-len(delEks))
	for _, key := range se.eks {
		delFlag := false
		for _, delKey := range delEks {
			if key.FileOffset == delKey.ExtentOffset && key.ExtentId == delKey.ExtentId &&
				key.ExtentOffset == delKey.ExtentOffset && key.PartitionId == delKey.PartitionId &&
				key.Size == delKey.Size {
				delFlag = true
				break
			}
		}
		if !delFlag {
			curEks = append(curEks, key)
		}
	}
	se.eks = curEks
	return
}
