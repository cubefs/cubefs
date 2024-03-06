package metanode

import "github.com/cubefs/cubefs/proto"

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
