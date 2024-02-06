package statistics

const (
	ConfigMonitorAddr = "monitorAddr"
)

const (
	ModelDataNode   = "datanode"
	ModelMetaNode   = "metanode"
	ModelObjectNode = "objectnode"
	ModelFlashNode  = "flashnode"
)

const (
	// Monitor API
	MonitorCollect = "/collect"

	// Monitor Admin API
	MonitorClusterSet 	= "/cluster/set"
	MonitorClusterGet 	= "/cluster/get"
	MonitorClusterAdd 	= "/cluster/add"
	MonitorClusterDel	= "/cluster/del"
	MonitorTopicSet 	= "/topic/set"
)

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
