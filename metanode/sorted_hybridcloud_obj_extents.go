package metanode

import (
	"bytes"
	"fmt"

	"github.com/cubefs/cubefs/proto"
)

type SortedHybridCloudExtents struct {
	sortedEks interface{}
}

func (se *SortedHybridCloudExtents) GetSortedEks() interface{} {
	return se.sortedEks
}

func (se *SortedHybridCloudExtents) Empty() bool {
	return se.sortedEks == nil
}

func (se *SortedHybridCloudExtents) HasReplicaExts() bool {
	if se.sortedEks == nil {
		return false
	}

	_, ok := se.sortedEks.(*SortedExtents)
	return ok
}

func NewSortedHybridCloudExtents() *SortedHybridCloudExtents {
	return &SortedHybridCloudExtents{}
}

type SortedHybridCloudExtentsMigration struct {
	sortedEks    interface{}
	storageClass uint32
	expiredTime  int64 // delay delete
}

func (sem *SortedHybridCloudExtentsMigration) GetSortedEks() interface{} {
	return sem.sortedEks
}

func (sem *SortedHybridCloudExtentsMigration) GetStorageClass() uint32 {
	return sem.storageClass
}

func (sem *SortedHybridCloudExtentsMigration) GetExpiredTime() int64 {
	return sem.expiredTime
}

func (sem *SortedHybridCloudExtentsMigration) HasReplicaMigrationExts() bool {
	if !proto.IsStorageClassReplica(sem.storageClass) {
		return false
	}

	if sem.sortedEks == nil {
		return false
	}

	eks := sem.sortedEks.(*SortedExtents)

	return eks.Len() > 0
}

func (sem *SortedHybridCloudExtentsMigration) Empty() bool {
	return sortEksEmpty(sem.sortedEks, sem.storageClass)
}

func NewSortedHybridCloudExtentsMigration() *SortedHybridCloudExtentsMigration {
	return &SortedHybridCloudExtentsMigration{storageClass: proto.StorageClass_Unspecified}
}

func (sem *SortedHybridCloudExtentsMigration) String() string {
	buff := bytes.NewBuffer(nil)
	buff.Grow(128)
	buff.WriteString("{")
	buff.WriteString(fmt.Sprintf("\"StorageClass\":%d,", sem.storageClass))
	buff.WriteString(fmt.Sprintf("\"expiredTime\"%d,", sem.expiredTime))

	if sem.sortedEks == nil {
		buff.WriteString("ExtentsMigration[nil]")
		buff.WriteString("}")
		return buff.String()
	}

	if proto.IsStorageClassReplica(sem.storageClass) {
		buff.WriteString(fmt.Sprintf("\"ExtentsMigration\"[%s]", sem.sortedEks.(*SortedExtents)))
	} else if proto.IsStorageClassBlobStore(sem.storageClass) {
		buff.WriteString(fmt.Sprintf("\"ExtentsMigration\"[%s]", sem.sortedEks.(*SortedObjExtents)))
	} else {
		buff.WriteString("ExtentsMigration[unknown type]")
	}
	buff.WriteString("}")
	return buff.String()
}

func sortEksEmpty(sortEks interface{}, storageClass uint32) bool {
	if sortEks == nil {
		return true
	}

	if proto.IsStorageClassReplica(storageClass) {
		eks := sortEks.(*SortedExtents)
		return eks.IsEmpty()
	}

	if proto.IsStorageClassBlobStore(storageClass) {
		eks := sortEks.(*SortedObjExtents)
		return eks.IsEmpty()
	}

	return true
}
