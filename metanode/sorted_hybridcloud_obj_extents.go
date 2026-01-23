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

func NewSortedHybridCloudExtentsExt(eks interface{}) *SortedHybridCloudExtents {
	return &SortedHybridCloudExtents{
		sortedEks: eks,
	}
}

type SortedHybridCloudExtentsMigration struct {
	sortedEks    interface{}
	storageClass uint32
	expiredTime  int64 // delay delete
	poolId       uint8
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

	// Safe type assertion to prevent panic
	eks, ok := sem.sortedEks.(*SortedExtents)
	if !ok {
		return false
	}

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
	buff.Grow(256) // Increased buffer size for better performance

	buff.WriteString("{")
	buff.WriteString(fmt.Sprintf("\"StorageClass\":%d,", sem.storageClass))
	buff.WriteString(fmt.Sprintf("\"expiredTime\":%d,", sem.expiredTime)) // Fixed missing colon

	if sem.sortedEks == nil {
		buff.WriteString("\"ExtentsMigration\":null")
		buff.WriteString("}")
		return buff.String()
	}

	// Handle different storage class types with safe type assertions
	switch {
	case proto.IsStorageClassReplica(sem.storageClass):
		if eks, ok := sem.sortedEks.(*SortedExtents); ok {
			buff.WriteString(fmt.Sprintf("\"ExtentsMigration\":[%s]", eks.String()))
		} else {
			buff.WriteString("\"ExtentsMigration\":\"invalid_replica_type\"")
		}
	case proto.IsStorageClassBlobStore(sem.storageClass):
		if eks, ok := sem.sortedEks.(*SortedObjExtents); ok {
			buff.WriteString(fmt.Sprintf("\"ExtentsMigration\":[%s]", eks.String()))
		} else {
			buff.WriteString("\"ExtentsMigration\":\"invalid_blobstore_type\"")
		}
	default:
		buff.WriteString("\"ExtentsMigration\":\"unknown_type\"")
	}

	buff.WriteString("}")
	return buff.String()
}

// sortEksEmpty checks if the sorted extents collection is empty based on storage class
func sortEksEmpty(sortEks interface{}, storageClass uint32) bool {
	if sortEks == nil {
		return true
	}

	// Use switch statement for better performance and readability
	switch {
	case proto.IsStorageClassReplica(storageClass):
		if eks, ok := sortEks.(*SortedExtents); ok {
			return eks.IsEmpty()
		}
		return true
	case proto.IsStorageClassBlobStore(storageClass):
		if eks, ok := sortEks.(*SortedObjExtents); ok {
			return eks.IsEmpty()
		}
		return true
	default:
		return true
	}
}
