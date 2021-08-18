package statistics

const (
	ConfigMonitorAddr = "monitorAddr"
)

const (
	ModelDataNode = "datanode"
	ModelMetaNode = "metanode"
)

const (
	// Monitor API
	MonitorCollect = "/collect"
	MonitorCluster = "/cluster/set"

	MonitorClusterTopVol	= "/cluster/top/vol"
	MonitorClusterTopIP  	= "/cluster/top/ip"
	MonitorOpTopVol      	= "/op/top/vol"
	MonitorOpTopIP       	= "/op/top/ip"
	MonitorTopPartition  	= "/top/partition"
	MonitorTopOp  			= "/top/op"
	MonitorTopIP  			= "/top/ip"
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
