package metanode

import "github.com/cubefs/cubefs/proto"

type SortedHybridCloudExtents struct {
	sortedEks interface{}
}

func NewSortedHybridCloudExtents() *SortedHybridCloudExtents {
	return &SortedHybridCloudExtents{}
}

type SortedHybridCloudExtentsMigration struct {
	sortedEks    interface{}
	storageClass uint32
}

func NewSortedHybridCloudExtentsMigration() *SortedHybridCloudExtentsMigration {
	return &SortedHybridCloudExtentsMigration{storageClass: proto.StorageClass_Unspecified}
}
