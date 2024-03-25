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

func NewSortedHybridCloudExtents() *SortedHybridCloudExtents {
	return &SortedHybridCloudExtents{}
}

type SortedHybridCloudExtentsMigration struct {
	sortedEks    interface{}
	storageClass uint32
	expiredTime  int64 //delay delete
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
		buff.WriteString(fmt.Sprint("\"ExtentsMigration\"[nil]"))
		buff.WriteString("}")
		return buff.String()
	}

	if proto.IsStorageClassReplica(sem.storageClass) {
		buff.WriteString(fmt.Sprintf("\"ExtentsMigration\"[%s]", sem.sortedEks.(*SortedExtents)))
	} else if proto.IsStorageClassBlobStore(sem.storageClass) {
		buff.WriteString(fmt.Sprintf("\"ExtentsMigration\"[%s]", sem.sortedEks.(*SortedObjExtents)))
	} else {
		buff.WriteString(fmt.Sprint("\"ExtentsMigration\"[unknown type]"))
	}
	buff.WriteString("}")
	return buff.String()
}
