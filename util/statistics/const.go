package statistics

const (
	ConfigMonitorAddr = "monitorAddr"
)

const (
	ModelDataNode   = "datanode"
	ModelMetaNode   = "metanode"
	ModelObjectNode = "objectnode"
)

const (
	// Monitor API
	MonitorCollect = "/collect"
	MonitorCluster = "/cluster/set"

	MonitorClusterTopVol = "/cluster/top/vol"
	MonitorClusterTopIP  = "/cluster/top/ip"
	MonitorOpTopVol      = "/op/top/vol"
	MonitorOpTopIP       = "/op/top/ip"
	MonitorTopPartition  = "/top/partition"
	MonitorTopOp         = "/top/op"
	MonitorTopIP         = "/top/ip"
)

const (
	ActionRead = iota
	ActionRepairRead
	ActionAppendWrite
	ActionOverWrite
	ActionRepairWrite
)

var ActionDataMap = map[int]string{
	ActionRead:        "read",
	ActionRepairRead:  "repairRead",
	ActionAppendWrite: "appendWrite",
	ActionOverWrite:   "overWrite",
	ActionRepairWrite: "repairWrite",
}

const (
	ActionMetaCreateInode = iota
	ActionMetaEvictInode
	ActionMetaCreateDentry
	ActionMetaDeleteDentry
	ActionMetaLookup
	ActionMetaReadDir
	ActionMetaInodeGet
	ActionMetaBatchInodeGet
	ActionMetaExtentsAdd
	ActionMetaExtentsList
	ActionMetaTruncate
	ActionMetaExtentsInsert
)

var ActionMetaMap = map[int]string{
	ActionMetaCreateInode:   "createInode",
	ActionMetaEvictInode:    "evictInode",
	ActionMetaCreateDentry:  "createDentry",
	ActionMetaDeleteDentry:  "deleteDentry",
	ActionMetaLookup:        "lookup",
	ActionMetaReadDir:       "readDir",
	ActionMetaInodeGet:      "inodeGet",
	ActionMetaBatchInodeGet: "batchInodeGet",
	ActionMetaExtentsAdd:    "addExtents",
	ActionMetaExtentsList:   "listExtents",
	ActionMetaTruncate:      "truncate",
	ActionMetaExtentsInsert: "insertExtent",
}

const (
	ActionS3HeadObject = iota
	ActionS3GetObject
	ActionS3PutObject
	ActionS3ListObjects
	ActionS3DeleteObject
	ActionS3CopyObject
	ActionS3CreateMultipartUpload
	ActionS3UploadPart
	ActionS3CompleteMultipartUpload
	ActionS3AbortMultipartUpload
	ActionS3ListMultipartUploads
	ActionS3ListParts
)

var ActionObjectMap = map[int]string{
	ActionS3HeadObject:              "HeadObject",
	ActionS3GetObject:               "GetObject",
	ActionS3PutObject:               "PutObject",
	ActionS3ListObjects:             "ListObjects",
	ActionS3DeleteObject:            "DeleteObject",
	ActionS3CopyObject:              "CopyObject",
	ActionS3CreateMultipartUpload:   "CreateMultipartUpload",
	ActionS3UploadPart:              "UploadPart",
	ActionS3CompleteMultipartUpload: "CompleteMultipartUpload",
	ActionS3AbortMultipartUpload:    "AbortMultipartUpload",
	ActionS3ListMultipartUploads:    "ListMultipartUploads",
	ActionS3ListParts:               "ListParts",
}
