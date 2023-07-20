package vol

type ClientVol struct {
	ID                uint64
	Name              string
	Owner             string
	OSSAccessKey      string
	OSSSecretKey      string
	dpReplicaNum      uint8
	mpReplicaNum      uint8
	Status            uint8
	threshold         float32
	dataPartitionSize uint64 // byte
	Capacity          uint64 // GB
	VolType           int

	EbsBlkSize       int
	CacheCapacity    uint64
	CacheAction      int
	CacheThreshold   int
	CacheTTL         int
	CacheHighWater   int
	CacheLowWater    int
	CacheLRUInterval int
	CacheRule        string

	PreloadCacheOn        bool
	NeedToLowerReplica    bool
	FollowerRead          bool
}
