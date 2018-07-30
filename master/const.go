package master

const (
	ParaNodeAddr          = "addr"
	ParaName              = "name"
	ParaId                = "id"
	ParaCount             = "count"
	ParaReplicas          = "replicas"
	ParaDataPartitionType = "type"
	ParaStart             = "start"
	ParaEnable            = "enable"
	ParaThreshold         = "threshold"
)

const (
	DeleteExcessReplicationErr     = "DeleteExcessReplicationErr "
	AddLackReplicationErr          = "AddLackReplicationErr "
	CheckDataPartitionDiskErrorErr = "CheckDataPartitionDiskErrorErr  "
	GetAvailDataNodeHostsErr       = "GetAvailDataNodeHostsErr "
	GetAvailMetaNodeHostsErr       = "GetAvailMetaNodeHostsErr "
	GetDataReplicaFileCountInfo    = "GetDataReplicaFileCountInfo "
	DataNodeOfflineInfo            = "dataNodeOfflineInfo"
	HandleDataPartitionOfflineErr  = "HandleDataPartitionOffLineErr "
)

const (
	UnderlineSeparator = "_"
)

const (
	DefaultMaxMetaPartitionInodeID      uint64  = 1<<63 - 1
	DefaultMetaPartitionInodeIDStep     uint64  = 1 << 24
	DefaultMetaNodeReservedMem          uint64  = 1 << 32
	RuntimeStackBufSize                         = 4096
	NodesAliveRate                      float32 = 0.5
	MinReadWriteDataPartitions                  = 200
	MinReadWriteDataPartitionsForClient         = 10
	SpaceAvailRate                              = 0.95
)

const (
	OK = iota
	Failed
)
